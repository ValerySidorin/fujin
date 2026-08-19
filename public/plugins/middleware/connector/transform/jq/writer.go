package jq

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type jqWriterWrapper struct {
	w  connector.WriteCloser
	mw *transformJQMiddleware
	l  *slog.Logger
}

func (w *jqWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	out, err := transform(w.mw.produceCode, msg)
	if err != nil {
		w.l.Warn("produce rejected: jq transform failed", "error", err.Error())
		if callback != nil {
			callback(&TransformError{Err: err})
		}
		return
	}
	w.w.Produce(ctx, out, callback)
}

func (w *jqWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	out, err := transform(w.mw.produceCode, msg)
	if err != nil {
		w.l.Warn("hproduce rejected: jq transform failed", "error", err.Error())
		if callback != nil {
			callback(&TransformError{Err: err})
		}
		return
	}
	w.w.HProduce(ctx, out, headers, callback)
}

func (w *jqWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *jqWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *jqWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *jqWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *jqWriterWrapper) Close() error {
	return w.w.Close()
}
