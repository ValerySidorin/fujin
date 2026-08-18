package wasm

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"gopkg.in/yaml.v3"
)

const (
	defaultMaxMemoryMB   = 16
	defaultMaxOutputSize = 4 << 20
	defaultTimeout       = 5 * time.Millisecond
	wasmPageSize         = 64 << 10
)

var moduleSequence atomic.Uint64

func init() {
	if err := cmw.RegisterCompiled("transform_wasm", compile); err != nil {
		panic(fmt.Sprintf("register transform_wasm connector middleware: %v", err))
	}
}

type settings struct {
	Path            string        `yaml:"path"`
	SHA256          string        `yaml:"sha256"`
	ProduceFunction string        `yaml:"produce_function"`
	ConsumeFunction string        `yaml:"consume_function"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxMemoryMB     uint32        `yaml:"max_memory_mb"`
	MaxOutputBytes  uint32        `yaml:"max_output_bytes"`
}

func compile(raw any, l *slog.Logger) (cmw.Compiled, error) {
	config, err := decodeSettings(raw)
	if err != nil {
		return nil, err
	}
	binary, err := os.ReadFile(config.Path)
	if err != nil {
		return nil, fmt.Errorf("read WebAssembly module: %w", err)
	}
	if err := verifyDigest(binary, config.SHA256); err != nil {
		return nil, err
	}
	pages := uint32(((uint64(config.MaxMemoryMB) << 20) + wasmPageSize - 1) / wasmPageSize)
	runtimeConfig := wazero.NewRuntimeConfigCompiler().
		WithMemoryLimitPages(pages).
		WithCloseOnContextDone(true)
	ctx := context.Background()
	runtime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	compiled, err := runtime.CompileModule(ctx, binary)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("compile WebAssembly module: %w", err)
	}
	if err := validateABI(compiled, config); err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}
	name := fmt.Sprintf("transform-wasm-%d", moduleSequence.Add(1))
	module, err := runtime.InstantiateModule(
		ctx,
		compiled,
		wazero.NewModuleConfig().WithName(name).WithStartFunctions(),
	)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("instantiate WebAssembly module: %w", err)
	}
	if l == nil {
		l = slog.Default()
	}
	middleware := &middleware{module: module, config: config, l: l}
	return &compiledMiddleware{
		runtime: runtime, compiled: compiled, config: config, l: l, middleware: middleware,
	}, nil
}

func decodeSettings(raw any) (settings, error) {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return settings{}, fmt.Errorf("encode transform_wasm settings: %w", err)
	}
	var config settings
	if err := yaml.Unmarshal(data, &config); err != nil {
		return settings{}, fmt.Errorf("decode transform_wasm settings: %w", err)
	}
	if config.Path == "" {
		return settings{}, errors.New("transform_wasm path is required")
	}
	if config.SHA256 == "" {
		return settings{}, errors.New("transform_wasm sha256 is required")
	}
	if config.ProduceFunction == "" && config.ConsumeFunction == "" {
		return settings{}, errors.New("transform_wasm requires produce_function or consume_function")
	}
	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}
	if config.MaxMemoryMB == 0 {
		config.MaxMemoryMB = defaultMaxMemoryMB
	}
	if config.MaxMemoryMB > 4096 {
		return settings{}, errors.New("transform_wasm max_memory_mb must not exceed 4096")
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputSize
	}
	return config, nil
}

func verifyDigest(binary []byte, expected string) error {
	decoded, err := hex.DecodeString(expected)
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("transform_wasm sha256 must be a 64-character hexadecimal digest")
	}
	actual := sha256.Sum256(binary)
	if subtle.ConstantTimeCompare(actual[:], decoded) != 1 {
		return errors.New("transform_wasm sha256 mismatch")
	}
	return nil
}

func validateABI(module wazero.CompiledModule, config settings) error {
	if len(module.ImportedFunctions()) != 0 || len(module.ImportedMemories()) != 0 {
		return errors.New("transform_wasm modules may not import host capabilities")
	}
	if _, ok := module.ExportedMemories()["memory"]; !ok {
		return errors.New("transform_wasm module must export memory")
	}
	functions := module.ExportedFunctions()
	if err := validateFunction(functions, "alloc", []api.ValueType{api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32}); err != nil {
		return err
	}
	if err := validateFunction(functions, "dealloc", []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, nil); err != nil {
		return err
	}
	for _, name := range []string{config.ProduceFunction, config.ConsumeFunction} {
		if name == "" {
			continue
		}
		if err := validateFunction(functions, name, []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI64}); err != nil {
			return err
		}
	}
	return nil
}

func validateFunction(
	functions map[string]api.FunctionDefinition,
	name string,
	params []api.ValueType,
	results []api.ValueType,
) error {
	definition, ok := functions[name]
	if !ok {
		return fmt.Errorf("transform_wasm module must export function %q", name)
	}
	if !equalTypes(definition.ParamTypes(), params) || !equalTypes(definition.ResultTypes(), results) {
		return fmt.Errorf("transform_wasm function %q has an invalid signature", name)
	}
	return nil
}

func equalTypes(actual, expected []api.ValueType) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

type compiledMiddleware struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	config   settings
	l        *slog.Logger

	mu         sync.Mutex
	closed     bool
	middleware *middleware
}

func (c *compiledMiddleware) Open(*slog.Logger) (cmw.Middleware, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("transform_wasm compiled middleware is closed")
	}
	return c.middleware, nil
}

func (c *compiledMiddleware) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	runtime := c.runtime
	c.runtime = nil
	c.compiled = nil
	c.middleware = nil
	c.mu.Unlock()
	if runtime == nil {
		return nil
	}
	return runtime.Close(ctx)
}

type middleware struct {
	module api.Module
	config settings
	l      *slog.Logger
	callMu sync.Mutex
}

func (m *middleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	if m.config.ProduceFunction == "" {
		return w
	}
	return newWriter(w, newTransformer(m, m.config.ProduceFunction), m.l.With("connector", connectorName))
}

func (m *middleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	if m.config.ConsumeFunction == "" {
		return r
	}
	return newReader(r, newTransformer(m, m.config.ConsumeFunction), m.l.With("connector", connectorName))
}
