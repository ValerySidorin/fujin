package dedup

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type dedupWriterWrapper struct {
	w  connector.WriteCloser
	mw *dedupMiddleware
	l  *slog.Logger
}

func (w *dedupWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	key, err := w.mw.computeKey(msg, nil)
	if err != nil {
		w.l.Warn("produce rejected: dedup key computation failed", "error", err.Error())
		if callback != nil {
			callback(err)
		}
		return
	}
	if !w.mw.produceStore.Add(key) {
		w.l.Debug("produce rejected: duplicate message")
		if callback != nil {
			callback(&DuplicateError{})
		}
		return
	}
	w.w.Produce(ctx, msg, callback)
}

func (w *dedupWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	key, err := w.mw.computeKey(msg, headers)
	if err != nil {
		w.l.Warn("hproduce rejected: dedup key computation failed", "error", err.Error())
		if callback != nil {
			callback(err)
		}
		return
	}
	if !w.mw.produceStore.Add(key) {
		w.l.Debug("hproduce rejected: duplicate message")
		if callback != nil {
			callback(&DuplicateError{})
		}
		return
	}
	w.w.HProduce(ctx, msg, headers, callback)
}

func (w *dedupWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *dedupWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *dedupWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *dedupWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *dedupWriterWrapper) Close() error {
	return w.w.Close()
}
