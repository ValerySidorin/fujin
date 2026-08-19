package jq

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type filterWriterWrapper struct {
	w  connector.WriteCloser
	mw *filterJQMiddleware
	l  *slog.Logger
}

func (w *filterWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	pass, err := evaluate(w.mw.produceCode, msg)
	if err != nil {
		w.l.Warn("produce rejected: filter evaluation failed", "error", err.Error())
		if callback != nil {
			callback(err)
		}
		return
	}
	if !pass {
		w.l.Debug("produce filtered out")
		if callback != nil {
			callback(&FilteredError{})
		}
		return
	}
	w.w.Produce(ctx, msg, callback)
}

func (w *filterWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	pass, err := evaluate(w.mw.produceCode, msg)
	if err != nil {
		w.l.Warn("hproduce rejected: filter evaluation failed", "error", err.Error())
		if callback != nil {
			callback(err)
		}
		return
	}
	if !pass {
		w.l.Debug("hproduce filtered out")
		if callback != nil {
			callback(&FilteredError{})
		}
		return
	}
	w.w.HProduce(ctx, msg, headers, callback)
}

func (w *filterWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *filterWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *filterWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *filterWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *filterWriterWrapper) Close() error {
	return w.w.Close()
}
