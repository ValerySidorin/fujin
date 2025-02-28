package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

type Reader struct {
	conf ReaderConfig
	nc   *nats.Conn
	l    *slog.Logger
}

func NewReader(conf ReaderConfig, l *slog.Logger) (*Reader, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	return &Reader{
		conf: conf,
		nc:   nc,
		l:    l.With("reader_type", "nats"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any) error) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		_ = h(msg.Data) // not checking err here for performance gains
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

func (r *Reader) Consume(ctx context.Context, trigger <-chan struct{}, n uint32,
	h func(message []byte, args ...any) error) error {
	return errors.New("nats: consume pattern not implemented")
}

func (r *Reader) Ack(ctx context.Context, meta []byte) error {
	return nil
}

func (r *Reader) NAck(ctx context.Context, meta []byte) error {
	return nil
}

func (r *Reader) EncodeMeta(buf []byte, args ...any) []byte {
	return nil
}

func (r *Reader) MessageMetaLen() byte {
	return 0
}

func (r *Reader) IsAutoCommit() bool {
	return true
}

func (r *Reader) Close() {
	r.nc.Close()
}
