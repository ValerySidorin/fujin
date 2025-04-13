package streaming

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/cerr"
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

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		h(msg.Data)
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
	fetchResponseHandler func(n uint32),
	msgHandler func(message []byte, args ...any),
) error {
	return cerr.ErrNotSupported
}

func (r *Reader) Ack(ctx context.Context, meta []byte) error {
	return nil
}

func (r *Reader) Nack(ctx context.Context, meta []byte) error {
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
