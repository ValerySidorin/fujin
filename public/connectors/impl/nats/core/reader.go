//go:build nats_core

package core

import (
	"context"
	"fmt"
	"log/slog"

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

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
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
