package core

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/nats-io/nats.go"
)

type Reader struct {
	conf       ReaderConfig
	autoCommit bool
	nc         *nats.Conn
	l          *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	return &Reader{
		conf:       conf,
		autoCommit: autoCommit,
		nc:         nc,
		l:          l.With("reader_type", "nats_core"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		h(msg.Data, msg.Subject)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		var hs [][]byte
		for k, headers := range msg.Header {
			hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(k)), len(k)))
			var vals [][]byte
			for _, h := range headers {
				vals = append(vals, unsafe.Slice((*byte)(unsafe.StringData(h)), len(h)))
			}
			hs = append(hs, joinBytes(vals, ','))
		}
		h(msg.Data, msg.Subject, hs)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) FetchH(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(cerr.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(cerr.ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return buf
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 0
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {
	r.nc.Close()
}

func joinBytes(elems [][]byte, sep byte) []byte {
	// Обработка простых случаев
	switch len(elems) {
	case 0:
		return nil
	case 1:
		out := make([]byte, len(elems[0]))
		copy(out, elems[0])
		return out
	}

	totalLen := len(elems) - 1
	for _, e := range elems {
		totalLen += len(e)
	}

	out := make([]byte, totalLen)

	pos := copy(out, elems[0])
	for _, e := range elems[1:] {
		pos += copy(out[pos:], []byte{sep})
		pos += copy(out[pos:], e)
	}

	return out
}
