package jq

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type jqReaderWrapper struct {
	r  connector.ReadCloser
	mw *transformJQMiddleware
	l  *slog.Logger
}

func (r *jqReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	return r.r.Subscribe(ctx, ready, func(message []byte, topic string, args ...any) {
		out, err := transform(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("subscribe: transform failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		h(out, topic, args...)
	})
}

func (r *jqReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return r.r.SubscribeWithHeaders(ctx, ready, func(message []byte, topic string, hs [][]byte, args ...any) {
		out, err := transform(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("subscribe: transform failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		h(out, topic, hs, args...)
	})
}

func (r *jqReaderWrapper) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	r.r.Fetch(ctx, n, fetchHandler, func(message []byte, topic string, args ...any) {
		out, err := transform(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("fetch: transform failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		msgHandler(out, topic, args...)
	})
}

func (r *jqReaderWrapper) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	r.r.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, topic string, hs [][]byte, args ...any) {
		out, err := transform(r.mw.consumeCode, message)
		if err != nil {
			r.l.Warn("fetch: transform failed, message skipped", "topic", topic, "error", err.Error())
			return
		}
		msgHandler(out, topic, hs, args...)
	})
}

func (r *jqReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	r.r.Ack(ctx, msgIDs, ackHandler, ackMsgHandler)
}

func (r *jqReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	r.r.Nack(ctx, msgIDs, nackHandler, nackMsgHandler)
}

func (r *jqReaderWrapper) MsgIDArgsLen() int {
	return r.r.MsgIDArgsLen()
}

func (r *jqReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.r.EncodeMsgID(buf, topic, args...)
}

func (r *jqReaderWrapper) AutoCommit() bool {
	return r.r.AutoCommit()
}

func (r *jqReaderWrapper) Close() error {
	return r.r.Close()
}
