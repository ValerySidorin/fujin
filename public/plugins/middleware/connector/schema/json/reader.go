package json

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type schemaReaderWrapper struct {
	r  connector.ReadCloser
	mw *schemaMiddleware
	l  *slog.Logger
}

func (r *schemaReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	return r.r.Subscribe(ctx, ready, func(message []byte, topic string, args ...any) {
		if err := r.mw.validate(message); err != nil {
			r.l.Warn("subscribe: invalid message skipped", "topic", topic, "error", formatValidationError(err))
			return
		}
		h(message, topic, args...)
	})
}

func (r *schemaReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return r.r.SubscribeWithHeaders(ctx, ready, func(message []byte, topic string, hs [][]byte, args ...any) {
		if err := r.mw.validate(message); err != nil {
			r.l.Warn("subscribe: invalid message skipped", "topic", topic, "error", formatValidationError(err))
			return
		}
		h(message, topic, hs, args...)
	})
}

func (r *schemaReaderWrapper) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	r.r.Fetch(ctx, n, fetchHandler, func(message []byte, topic string, args ...any) {
		if err := r.mw.validate(message); err != nil {
			r.l.Warn("fetch: invalid message skipped", "topic", topic, "error", formatValidationError(err))
			return
		}
		msgHandler(message, topic, args...)
	})
}

func (r *schemaReaderWrapper) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	r.r.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, topic string, hs [][]byte, args ...any) {
		if err := r.mw.validate(message); err != nil {
			r.l.Warn("fetch: invalid message skipped", "topic", topic, "error", formatValidationError(err))
			return
		}
		msgHandler(message, topic, hs, args...)
	})
}

func (r *schemaReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	r.r.Ack(ctx, msgIDs, ackHandler, ackMsgHandler)
}

func (r *schemaReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	r.r.Nack(ctx, msgIDs, nackHandler, nackMsgHandler)
}

func (r *schemaReaderWrapper) MsgIDArgsLen() int {
	return r.r.MsgIDArgsLen()
}

func (r *schemaReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.r.EncodeMsgID(buf, topic, args...)
}

func (r *schemaReaderWrapper) AutoCommit() bool {
	return r.r.AutoCommit()
}

func (r *schemaReaderWrapper) Close() error {
	return r.r.Close()
}
