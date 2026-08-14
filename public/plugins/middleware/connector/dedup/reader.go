package dedup

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type dedupReaderWrapper struct {
	r  connector.ReadCloser
	mw *dedupMiddleware
	l  *slog.Logger
}

func (r *dedupReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	return r.r.Subscribe(ctx, ready, func(message []byte, topic string, args ...any) {
		key, err := r.mw.computeKey(message, nil)
		if err != nil {
			r.l.Warn("subscribe: dedup key computation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !r.mw.consumeStore.Add(key) {
			r.l.Debug("subscribe: duplicate message skipped", "topic", topic)
			return
		}
		h(message, topic, args...)
	})
}

func (r *dedupReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return r.r.SubscribeWithHeaders(ctx, ready, func(message []byte, topic string, hs [][]byte, args ...any) {
		key, err := r.mw.computeKey(message, hs)
		if err != nil {
			r.l.Warn("subscribe: dedup key computation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !r.mw.consumeStore.Add(key) {
			r.l.Debug("subscribe: duplicate message skipped", "topic", topic)
			return
		}
		h(message, topic, hs, args...)
	})
}

func (r *dedupReaderWrapper) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	r.r.Fetch(ctx, n, fetchHandler, func(message []byte, topic string, args ...any) {
		key, err := r.mw.computeKey(message, nil)
		if err != nil {
			r.l.Warn("fetch: dedup key computation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !r.mw.consumeStore.Add(key) {
			r.l.Debug("fetch: duplicate message skipped", "topic", topic)
			return
		}
		msgHandler(message, topic, args...)
	})
}

func (r *dedupReaderWrapper) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	r.r.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, topic string, hs [][]byte, args ...any) {
		key, err := r.mw.computeKey(message, hs)
		if err != nil {
			r.l.Warn("fetch: dedup key computation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !r.mw.consumeStore.Add(key) {
			r.l.Debug("fetch: duplicate message skipped", "topic", topic)
			return
		}
		msgHandler(message, topic, hs, args...)
	})
}

func (r *dedupReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	r.r.Ack(ctx, msgIDs, ackHandler, ackMsgHandler)
}

func (r *dedupReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	r.r.Nack(ctx, msgIDs, nackHandler, nackMsgHandler)
}

func (r *dedupReaderWrapper) MsgIDArgsLen() int {
	return r.r.MsgIDArgsLen()
}

func (r *dedupReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.r.EncodeMsgID(buf, topic, args...)
}

func (r *dedupReaderWrapper) AutoCommit() bool {
	return r.r.AutoCommit()
}

func (r *dedupReaderWrapper) Close() error {
	return r.r.Close()
}
