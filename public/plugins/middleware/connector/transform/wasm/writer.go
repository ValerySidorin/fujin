package wasm

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type writer struct {
	inner       connector.WriteCloser
	transformer *transformer
	l           *slog.Logger
}

func newWriter(inner connector.WriteCloser, transformer *transformer, l *slog.Logger) connector.WriteCloser {
	return &writer{inner: inner, transformer: transformer, l: l}
}

func (w *writer) Produce(ctx context.Context, message []byte, callback func(error)) {
	output, err := w.transformer.transform(ctx, message)
	if err != nil {
		w.l.Warn("produce rejected: WebAssembly transform failed", "err", err)
		if callback != nil {
			callback(&TransformError{Err: err})
		}
		return
	}
	w.inner.Produce(ctx, output, callback)
}

func (w *writer) HProduce(ctx context.Context, message []byte, headers [][]byte, callback func(error)) {
	output, err := w.transformer.transform(ctx, message)
	if err != nil {
		w.l.Warn("hproduce rejected: WebAssembly transform failed", "err", err)
		if callback != nil {
			callback(&TransformError{Err: err})
		}
		return
	}
	w.inner.HProduce(ctx, output, headers, callback)
}

func (w *writer) Flush(ctx context.Context) error      { return w.inner.Flush(ctx) }
func (w *writer) BeginTx(ctx context.Context) error    { return w.inner.BeginTx(ctx) }
func (w *writer) CommitTx(ctx context.Context) error   { return w.inner.CommitTx(ctx) }
func (w *writer) RollbackTx(ctx context.Context) error { return w.inner.RollbackTx(ctx) }
func (w *writer) Close() error                         { return w.inner.Close() }

// TransformError reports a guest transform failure without exposing the module runtime.
type TransformError struct{ Err error }

func (e *TransformError) Error() string {
	return fmt.Sprintf("WebAssembly transform failed: %s", e.Err)
}
func (e *TransformError) Unwrap() error { return e.Err }
