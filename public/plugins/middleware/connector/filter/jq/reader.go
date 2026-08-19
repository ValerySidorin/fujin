package jq

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type filterReaderWrapper struct {
	r  connector.ReadCloser
	mw *filterJQMiddleware
	l  *slog.Logger
}

func (r *filterReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	return r.r.Subscribe(ctx, ready, func(message []byte, topic string, args ...any) {
		pass, err := evaluate(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("subscribe: filter evaluation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !pass {
			r.l.Debug("subscribe: message filtered out", "topic", topic)
			return
		}
		h(message, topic, args...)
	})
}

func (r *filterReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return r.r.SubscribeWithHeaders(ctx, ready, func(message []byte, topic string, hs [][]byte, args ...any) {
		pass, err := evaluate(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("subscribe: filter evaluation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !pass {
			r.l.Debug("subscribe: message filtered out", "topic", topic)
			return
		}
		h(message, topic, hs, args...)
	})
}

func (r *filterReaderWrapper) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	r.r.Fetch(ctx, n, fetchHandler, func(message []byte, topic string, args ...any) {
		pass, err := evaluate(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("fetch: filter evaluation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !pass {
			r.l.Debug("fetch: message filtered out", "topic", topic)
			return
		}
		msgHandler(message, topic, args...)
	})
}

func (r *filterReaderWrapper) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	r.r.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, topic string, hs [][]byte, args ...any) {
		pass, err := evaluate(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("fetch: filter evaluation failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		if !pass {
			r.l.Debug("fetch: message filtered out", "topic", topic)
			return
		}
		msgHandler(message, topic, hs, args...)
	})
}

func (r *filterReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	r.r.Ack(ctx, msgIDs, ackHandler, ackMsgHandler)
}

func (r *filterReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	r.r.Nack(ctx, msgIDs, nackHandler, nackMsgHandler)
}

func (r *filterReaderWrapper) MsgIDArgsLen() int {
	return r.r.MsgIDArgsLen()
}

func (r *filterReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.r.EncodeMsgID(buf, topic, args...)
}

func (r *filterReaderWrapper) AutoCommit() bool {
	return r.r.AutoCommit()
}

func (r *filterReaderWrapper) Close() error {
	return r.r.Close()
}
