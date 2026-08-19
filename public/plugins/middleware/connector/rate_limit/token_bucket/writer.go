package token_bucket

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type rateLimitWriterWrapper struct {
	w  connector.WriteCloser
	mw *rateLimitMiddleware
	l  *slog.Logger
}

func (w *rateLimitWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	if !w.mw.limiter.Allow() {
		w.l.Debug("produce rejected: rate limit exceeded")
		if callback != nil {
			callback(&RateLimitError{})
		}
		return
	}
	w.w.Produce(ctx, msg, callback)
}

func (w *rateLimitWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	if !w.mw.limiter.Allow() {
		w.l.Debug("hproduce rejected: rate limit exceeded")
		if callback != nil {
			callback(&RateLimitError{})
		}
		return
	}
	w.w.HProduce(ctx, msg, headers, callback)
}

func (w *rateLimitWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *rateLimitWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *rateLimitWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *rateLimitWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *rateLimitWriterWrapper) Close() error {
	return w.w.Close()
}
