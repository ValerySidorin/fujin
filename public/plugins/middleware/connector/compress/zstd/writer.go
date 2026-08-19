package zstd

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type zstdWriterWrapper struct {
	w  connector.WriteCloser
	mw *compressZstdMiddleware
	l  *slog.Logger
}

func (w *zstdWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	w.w.Produce(ctx, w.mw.compress(msg), callback)
}

func (w *zstdWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	w.w.HProduce(ctx, w.mw.compress(msg), headers, callback)
}

func (w *zstdWriterWrapper) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *zstdWriterWrapper) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *zstdWriterWrapper) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *zstdWriterWrapper) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *zstdWriterWrapper) Close() error {
	return w.w.Close()
}
