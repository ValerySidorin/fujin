// Package connector provides a plugin system for connector middlewares.
// Connector middlewares wrap readers and writers to add cross-cutting functionality
// like observability, rate limiting, retries, etc.
package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

// Middleware wraps readers and writers with additional functionality.
type Middleware interface {
	// WrapWriter wraps a writer with additional functionality.
	WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser
	// WrapReader wraps a reader with additional functionality.
	WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser
}

// Factory creates a connector middleware from configuration.
// config is the connector middleware-specific configuration (can be nil).
type Factory func(config any, l *slog.Logger) (Middleware, error)

// Compiled is generation-scoped validated middleware configuration.
type Compiled interface {
	Open(l *slog.Logger) (Middleware, error)
	Close(context.Context) error
}

// CompileFunc validates middleware configuration before generation publication.
type CompileFunc func(config any, l *slog.Logger) (Compiled, error)

var (
	factories = make(map[string]Factory)
	compilers = make(map[string]CompileFunc)
	mu        sync.RWMutex
)

// Register registers a connector middleware factory with the given name.
// This is typically called from init() in connector middleware implementations.
func Register(name string, factory Factory) error {
	if factory == nil {
		return fmt.Errorf("connector middleware %q factory is nil", name)
	}
	return register(name, factory, func(config any, l *slog.Logger) (Compiled, error) {
		middleware, err := factory(config, l)
		if err != nil {
			return nil, err
		}
		return &factoryCompiled{factory: factory, config: config, initial: middleware}, nil
	})
}

// RegisterCompiled registers middleware with generation-scoped compilation and cleanup.
func RegisterCompiled(name string, compile CompileFunc) error {
	if compile == nil {
		return fmt.Errorf("connector middleware %q compiler is nil", name)
	}
	return register(name, nil, compile)
}

func register(name string, factory Factory, compile CompileFunc) error {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := compilers[name]; exists {
		return fmt.Errorf("connector middleware %q already registered", name)
	}
	if factory != nil {
		factories[name] = factory
	}
	compilers[name] = compile
	return nil
}

// Get returns a connector middleware factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered connector middleware names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(compilers))
	for name := range compilers {
		names = append(names, name)
	}
	return names
}

// Compile validates and prepares one immutable generation middleware chain.
func Compile(configs []config.Config, l *slog.Logger) (connector.MiddlewareChain, error) {
	chain := &compiledChain{}
	for _, cfg := range configs {
		if cfg.Enabled != nil && !*cfg.Enabled {
			continue
		}
		mu.RLock()
		compile, ok := compilers[cfg.Name]
		mu.RUnlock()
		if !ok {
			_ = chain.Close(context.Background())
			return nil, fmt.Errorf("connector middleware %q not found (is it compiled in?)", cfg.Name)
		}
		compiled, err := compile(cfg.Config, l)
		if err != nil {
			_ = chain.Close(context.Background())
			return nil, fmt.Errorf("compile connector middleware %q: %w", cfg.Name, err)
		}
		if compiled == nil {
			_ = chain.Close(context.Background())
			return nil, fmt.Errorf("compile connector middleware %q: compiler returned nil", cfg.Name)
		}
		chain.entries = append(chain.entries, compiledEntry{name: cfg.Name, compiled: compiled})
	}
	if len(chain.entries) == 0 {
		return nil, nil
	}
	return chain, nil
}

type factoryCompiled struct {
	factory Factory
	config  any

	mu      sync.Mutex
	initial Middleware
}

func (c *factoryCompiled) Open(l *slog.Logger) (Middleware, error) {
	c.mu.Lock()
	if c.initial != nil {
		middleware := c.initial
		c.initial = nil
		c.mu.Unlock()
		return middleware, nil
	}
	c.mu.Unlock()
	return c.factory(c.config, l)
}

func (*factoryCompiled) Close(context.Context) error { return nil }

type compiledEntry struct {
	name     string
	compiled Compiled
}

type compiledChain struct {
	entries []compiledEntry
	mu      sync.Mutex
	closed  bool
}

func (c *compiledChain) WrapWriter(
	w connector.WriteCloser,
	connectorName string,
	l *slog.Logger,
) (connector.WriteCloser, error) {
	for _, entry := range c.entries {
		middleware, err := entry.compiled.Open(l)
		if err != nil {
			return nil, fmt.Errorf("open connector middleware %q: %w", entry.name, err)
		}
		w = middleware.WrapWriter(w, connectorName)
	}
	return w, nil
}

func (c *compiledChain) WrapReader(
	r connector.ReadCloser,
	connectorName string,
	l *slog.Logger,
) (connector.ReadCloser, error) {
	for _, entry := range c.entries {
		middleware, err := entry.compiled.Open(l)
		if err != nil {
			return nil, fmt.Errorf("open connector middleware %q: %w", entry.name, err)
		}
		r = middleware.WrapReader(r, connectorName)
	}
	return r, nil
}

func (c *compiledChain) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	entries := c.entries
	c.entries = nil
	c.mu.Unlock()

	errs := make([]error, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		if err := entries[i].compiled.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close connector middleware %q: %w", entries[i].name, err))
		}
	}
	return errors.Join(errs...)
}

// Chain applies a chain of connector middlewares to a writer and reader.
func Chain(
	w connector.WriteCloser,
	r connector.ReadCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.WriteCloser, connector.ReadCloser, error) {
	for _, cfg := range configs {
		// Skip if disabled (nil = true = enabled by default)
		if cfg.Enabled != nil && !*cfg.Enabled {
			continue
		}

		factory, ok := Get(cfg.Name)
		if !ok {
			return nil, nil, fmt.Errorf("connector middleware %q not found (is it compiled in?)", cfg.Name)
		}

		dec, err := factory(cfg.Config, l)
		if err != nil {
			return nil, nil, fmt.Errorf("create connector middleware %q: %w", cfg.Name, err)
		}

		if w != nil {
			w = dec.WrapWriter(w, connectorName)
		}
		if r != nil {
			r = dec.WrapReader(r, connectorName)
		}
	}

	return w, r, nil
}

// ChainWriter applies a chain of connector middlewares to a writer only.
func ChainWriter(
	w connector.WriteCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.WriteCloser, error) {
	result, _, err := Chain(w, nil, connectorName, configs, l)
	return result, err
}

// ChainReader applies a chain of connector middlewares to a reader only.
func ChainReader(
	r connector.ReadCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.ReadCloser, error) {
	_, result, err := Chain(nil, r, connectorName, configs, l)
	return result, err
}
