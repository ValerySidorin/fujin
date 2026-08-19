package wasm

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type reader struct {
	inner       connector.ReadCloser
	transformer *transformer
	l           *slog.Logger
}

func newReader(inner connector.ReadCloser, transformer *transformer, l *slog.Logger) connector.ReadCloser {
	return &reader{inner: inner, transformer: transformer, l: l}
}

func (r *reader) Subscribe(ctx context.Context, ready func() error, handler func([]byte, string, ...any)) error {
	return r.inner.Subscribe(ctx, ready, func(message []byte, source string, args ...any) {
		output, err := r.transformer.transform(ctx, message)
		if err != nil {
			r.l.Warn("subscribe: WebAssembly transform failed, message skipped", "source", source, "err", err)
			return
		}
		handler(output, source, args...)
	})
}

func (r *reader) SubscribeWithHeaders(
	ctx context.Context,
	ready func() error,
	handler func([]byte, string, [][]byte, ...any),
) error {
	return r.inner.SubscribeWithHeaders(ctx, ready, func(message []byte, source string, headers [][]byte, args ...any) {
		output, err := r.transformer.transform(ctx, message)
		if err != nil {
			r.l.Warn("subscribe: WebAssembly transform failed, message skipped", "source", source, "err", err)
			return
		}
		handler(output, source, headers, args...)
	})
}

func (r *reader) Fetch(
	ctx context.Context,
	n uint32,
	fetchHandler func(uint32, error),
	messageHandler func([]byte, string, ...any),
) {
	r.inner.Fetch(ctx, n, fetchHandler, func(message []byte, source string, args ...any) {
		output, err := r.transformer.transform(ctx, message)
		if err != nil {
			r.l.Warn("fetch: WebAssembly transform failed, message skipped", "source", source, "err", err)
			return
		}
		messageHandler(output, source, args...)
	})
}

func (r *reader) FetchWithHeaders(
	ctx context.Context,
	n uint32,
	fetchHandler func(uint32, error),
	messageHandler func([]byte, string, [][]byte, ...any),
) {
	r.inner.FetchWithHeaders(ctx, n, fetchHandler, func(message []byte, source string, headers [][]byte, args ...any) {
		output, err := r.transformer.transform(ctx, message)
		if err != nil {
			r.l.Warn("fetch: WebAssembly transform failed, message skipped", "source", source, "err", err)
			return
		}
		messageHandler(output, source, headers, args...)
	})
}

func (r *reader) Ack(ctx context.Context, ids [][]byte, ack func(error), each func([]byte, error)) {
	r.inner.Ack(ctx, ids, ack, each)
}

func (r *reader) Nack(ctx context.Context, ids [][]byte, nack func(error), each func([]byte, error)) {
	r.inner.Nack(ctx, ids, nack, each)
}

func (r *reader) MsgIDArgsLen() int { return r.inner.MsgIDArgsLen() }
func (r *reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return r.inner.EncodeMsgID(buf, source, args...)
}
func (r *reader) AutoCommit() bool { return r.inner.AutoCommit() }
func (r *reader) Close() error     { return r.inner.Close() }
