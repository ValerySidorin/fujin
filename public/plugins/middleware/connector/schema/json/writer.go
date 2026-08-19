package json

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type schemaWriterWrapper struct {
	w  connector.WriteCloser
	mw *schemaMiddleware
	l  *slog.Logger
}

func (w *schemaWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	if err := w.mw.validate(msg); err != nil {
		w.l.Warn("produce rejected: schema validation failed", "error", formatValidationError(err))
		if callback != nil {
			callback(err)
		}
		return
	}
	w.w.Produce(ctx, msg, callback)
}

func (w *schemaWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	if err := w.mw.validate(msg); err != nil {
		w.l.Warn("hproduce rejected: schema validation failed", "error", formatValidationError(err))
		if callback != nil {
			callback(err)
		}
		return
	}
	w.w.HProduce(ctx, msg, headers, callback)
}

func (w *schemaWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *schemaWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *schemaWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *schemaWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *schemaWriterWrapper) Close() error {
	return w.w.Close()
}
