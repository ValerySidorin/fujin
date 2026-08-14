package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/redis/rueidis"
)

// Reader implements connector.ReadCloser for Redis Rueidis PubSub
type Reader struct {
	conf       ConnectorConfig
	client     rueidis.Client
	subscribe  rueidis.Completed
	autoCommit bool
	l          *slog.Logger
}

// NewReader creates a new Redis PubSub reader
func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	tlsConf, err := conf.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("redis_rueidis_pubsub: get tls config: %w", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		TLSConfig:    tlsConf,
		InitAddress:  conf.InitAddress,
		Username:     conf.Username,
		Password:     conf.Password,
		DisableCache: conf.DisableCache,
	})
	if err != nil {
		return nil, fmt.Errorf("redis_rueidis_pubsub: new client: %w", err)
	}

	return &Reader{
		conf:       conf,
		client:     client,
		subscribe:  client.B().Subscribe().Channel(conf.Channels...).Build(),
		autoCommit: autoCommit,
		l:          l.With("reader_type", "redis_rueidis_pubsub"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	return r.receive(ctx, ready, func(msg rueidis.PubSubMessage) {
		h(unsafe.Slice((*byte)(unsafe.StringData(msg.Message)), len(msg.Message)), msg.Channel)
	})
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, headers [][]byte, args ...any)) error {
	return r.receive(ctx, ready, func(msg rueidis.PubSubMessage) {
		h(unsafe.Slice((*byte)(unsafe.StringData(msg.Message)), len(msg.Message)), msg.Channel, nil)
	})
}

func (r *Reader) receive(ctx context.Context, ready func() error, h func(rueidis.PubSubMessage)) error {
	receiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wanted := make(map[string]struct{}, len(r.conf.Channels))
	for _, channel := range r.conf.Channels {
		wanted[channel] = struct{}{}
	}
	confirmed := make(map[string]struct{}, len(wanted))
	var mu sync.Mutex
	var readyOnce sync.Once
	var readyErr error
	receiveCtx = rueidis.WithOnSubscriptionHook(receiveCtx, func(subscription rueidis.PubSubSubscription) {
		if subscription.Kind != "subscribe" {
			return
		}
		if _, ok := wanted[subscription.Channel]; !ok {
			return
		}
		mu.Lock()
		confirmed[subscription.Channel] = struct{}{}
		if len(confirmed) == len(wanted) {
			readyOnce.Do(func() {
				readyErr = ready()
				if readyErr != nil {
					cancel()
				}
			})
		}
		mu.Unlock()
	})
	err := r.client.Receive(receiveCtx, r.subscribe, h)
	mu.Lock()
	callbackErr := readyErr
	mu.Unlock()
	if callbackErr != nil {
		return callbackErr
	}
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("redis_rueidis_pubsub: receive: %w", err)
	}
	return nil
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	// Redis PubSub doesn't support acknowledgments (at-most-once delivery)
	ackHandler(util.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// Redis PubSub doesn't support acknowledgments (at-most-once delivery)
	nackHandler(util.ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return buf
}

func (r *Reader) MsgIDArgsLen() int {
	return 0
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	r.client.Close()
	return nil
}
