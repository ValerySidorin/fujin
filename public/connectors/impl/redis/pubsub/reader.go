//go:build redis_pubsub

package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/redis/rueidis"
)

type Reader struct {
	conf       ReaderConfig
	client     rueidis.Client
	subscribe  rueidis.Completed
	autoCommit bool
	l          *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	tlsConf, err := conf.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("redis: get tls config: %w", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		TLSConfig:    tlsConf,
		InitAddress:  conf.InitAddress,
		Username:     conf.Username,
		Password:     conf.Password,
		DisableCache: conf.DisableCache,
	})
	if err != nil {
		return nil, fmt.Errorf("redis: new client: %w", err)
	}

	return &Reader{
		conf:       conf,
		client:     client,
		subscribe:  client.B().Subscribe().Channel(conf.Channels...).Build(),
		autoCommit: autoCommit,
		l:          l.With("reader_type", "redis"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := r.client.Receive(ctx, r.subscribe, func(msg rueidis.PubSubMessage) {
				h(unsafe.Slice((*byte)(unsafe.StringData(msg.Message)), len(msg.Message)), msg.Channel)
			}); err != nil {
				return fmt.Errorf("redis: receive: %w", err)
			}
		}
	}
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
	r.client.Close()
}
