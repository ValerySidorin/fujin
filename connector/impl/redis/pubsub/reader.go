package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/ValerySidorin/fujin/connector/cerr"
	"github.com/redis/rueidis"
)

type Reader struct {
	conf      ReaderConfig
	client    rueidis.Client
	subscribe rueidis.Completed
	l         *slog.Logger
}

func NewReader(conf ReaderConfig, l *slog.Logger) (*Reader, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: conf.InitAddress,
		Username:    conf.Username,
		Password:    conf.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("redis: new client: %w", err)
	}

	return &Reader{
		conf:      conf,
		client:    client,
		subscribe: client.B().Subscribe().Channel(conf.Channel).Build(),
		l:         l.With("reader_type", "redis"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := r.client.Receive(ctx, r.subscribe, func(msg rueidis.PubSubMessage) {
				h(unsafe.Slice((*byte)(unsafe.StringData(msg.Message)), len(msg.Message)))
			}); err != nil {
				return fmt.Errorf("redis: receive: %w", err)
			}
		}
	}
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
	r.client.Close()
}
