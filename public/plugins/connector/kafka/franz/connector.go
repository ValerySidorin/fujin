package franz

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Connector struct {
	conf ConnectorConfig
	cl   *kgo.Client

	handler         func(r *kgo.Record, h func(message []byte, source string, args ...any))
	headeredHandler func(r *kgo.Record, h func(message []byte, source string, hs [][]byte, args ...any))
	fetching        atomic.Bool
	autoCommit      bool

	wg sync.WaitGroup
	l  *slog.Logger
}

func NewConnector(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (*Connector, error) {
	err := conf.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("kafka_franz: parse tls: %w", err)
	}

	opts := kgoOptsFromConf(conf, autoCommit, conf.TLS.Config)

	if conf.PingTimeout <= 0 {
		conf.PingTimeout = 5 * time.Second
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka_franz: new client: %w", err)
	}

	c := &Connector{
		conf:       conf,
		autoCommit: autoCommit,
		cl:         client,
		l:          l.With("connector_type", "kafka"),
	}

	if autoCommit {
		c.handler = func(r *kgo.Record, h func(message []byte, source string, args ...any)) {
			h(r.Value, r.Topic)
		}
		c.headeredHandler = func(r *kgo.Record, h func(message []byte, source string, hs [][]byte, args ...any)) {
			var hs [][]byte
			for _, kh := range r.Headers {
				hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(kh.Key)), len(kh.Key)), kh.Value)
			}
			h(r.Value, r.Topic, hs)
		}
	} else {
		c.handler = func(r *kgo.Record, h func(message []byte, source string, args ...any)) {
			h(r.Value, r.Topic, r.Partition, r.LeaderEpoch, r.Offset)
		}
		c.headeredHandler = func(r *kgo.Record, h func(message []byte, source string, hs [][]byte, args ...any)) {
			var hs [][]byte
			for _, kh := range r.Headers {
				hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(kh.Key)), len(kh.Key)), kh.Value)
			}
			h(r.Value, r.Topic, hs, r.Partition, r.LeaderEpoch, r.Offset)
		}
	}

	return c, nil
}

func (c *Connector) Close() error {
	c.wg.Wait()
	c.cl.Close()
	return nil
}
