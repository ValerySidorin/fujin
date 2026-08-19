package wasm

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"context"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
	"github.com/stretchr/testify/require"
)

func TestCompileRejectsInvalidModuleBeforePublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.wasm")
	content := []byte("not wasm")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	digest := sha256.Sum256(content)

	_, err := cmw.Compile([]cmwconfig.Config{{
		Name: "transform_wasm",
		Config: map[string]any{
			"path":             path,
			"sha256":           fmt.Sprintf("%x", digest),
			"produce_function": "transform",
		},
	}}, slog.Default())
	require.ErrorContains(t, err, "compile WebAssembly module")
}

type captureWriter struct {
	message []byte
}

func (w *captureWriter) Produce(_ context.Context, message []byte, callback func(error)) {
	w.message = append([]byte(nil), message...)
	if callback != nil {
		callback(nil)
	}
}

func (w *captureWriter) HProduce(ctx context.Context, message []byte, _ [][]byte, callback func(error)) {
	w.Produce(ctx, message, callback)
}

func (*captureWriter) Flush(context.Context) error      { return nil }
func (*captureWriter) BeginTx(context.Context) error    { return nil }
func (*captureWriter) CommitTx(context.Context) error   { return nil }
func (*captureWriter) RollbackTx(context.Context) error { return nil }
func (*captureWriter) Close() error                     { return nil }

func TestWriterTransformsMessageWithCompiledRustModule(t *testing.T) {
	path := filepath.Join("testdata", "uppercase.wasm")
	binary, err := os.ReadFile(path)
	require.NoError(t, err)
	digest := sha256.Sum256(binary)
	chain, err := cmw.Compile([]cmwconfig.Config{{
		Name: "transform_wasm",
		Config: map[string]any{
			"path":             path,
			"sha256":           fmt.Sprintf("%x", digest),
			"produce_function": "transform",
		},
	}}, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chain.Close(context.Background())) })

	inner := &captureWriter{}
	wrapped, err := chain.WrapWriter(inner, "test", slog.Default())
	require.NoError(t, err)
	var callbackErr error
	wrapped.Produce(context.Background(), []byte("hello, fujin"), func(err error) { callbackErr = err })
	require.NoError(t, callbackErr)
	require.Equal(t, []byte("HELLO, FUJIN"), inner.message)
}
