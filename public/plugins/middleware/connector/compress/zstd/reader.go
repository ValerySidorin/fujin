package zstd

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type zstdReaderWrapper struct {
	r  connector.ReadCloser
	mw *compressZstdMiddleware
	l  *slog.Logger
}

func (r *zstdReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	return r.r.Subscribe(ctx, ready, func(message []byte, topic string, args ...any) {
		out, err := r.mw.decompress(message)
		if err != nil {
			r.l.Warn("subscribe: decompress failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		h(out, topic, args...)
	})
}

func (r *zstdReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return r.r.SubscribeWithHeaders(ctx, ready, func(message []byte, topic string, hs [][]byte, args ...any) {
		out, err := r.mw.decompress(message)
		if err != nil {
			r.l.Warn("subscribe: decompress failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		h(out, topic, hs, args...)
	})
}

func (r *zstdReaderWrapper) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	r.r.Fetch(ctx, n, fetchHandler, func(message []byte, topic string, args ...any) {
		out, err := r.mw.decompress(message)
		if err != nil {
			r.l.Warn("fetch: decompress failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		msgHandler(out, topic, args...)
	})
}

func (r *zstdReaderWrapper) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	r.r.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, topic string, hs [][]byte, args ...any) {
		out, err := r.mw.decompress(message)
		if err != nil {
			r.l.Warn("fetch: decompress failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		msgHandler(out, topic, hs, args...)
	})
}

func (r *zstdReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	r.r.Ack(ctx, msgIDs, ackHandler, ackMsgHandler)
}

func (r *zstdReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	r.r.Nack(ctx, msgIDs, nackHandler, nackMsgHandler)
}

func (r *zstdReaderWrapper) MsgIDArgsLen() int {
	return r.r.MsgIDArgsLen()
}

func (r *zstdReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.r.EncodeMsgID(buf, topic, args...)
}

func (r *zstdReaderWrapper) AutoCommit() bool {
	return r.r.AutoCommit()
}

func (r *zstdReaderWrapper) Close() error {
	return r.r.Close()
}
