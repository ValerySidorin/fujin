//go:build nsq

package nsq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/nsqio/go-nsq"
	"github.com/panjf2000/ants/v2"
)

type Writer struct {
	conf     WriterConfig
	producer *nsq.Producer
	pool     *ants.Pool
	l        *slog.Logger
	wg       sync.WaitGroup
	chPool   sync.Pool
}

func NewWriter(conf WriterConfig, l *slog.Logger) (*Writer, error) {
	cfg := nsq.NewConfig()
	prod, err := nsq.NewProducer(conf.Address, cfg)
	if err != nil {
		return nil, fmt.Errorf("new producer: %w", err)
	}

	pool, err := ants.NewPool(conf.Pool.Size, ants.WithPreAlloc(conf.Pool.PreAlloc))
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return &Writer{
		conf:     conf,
		producer: prod,
		pool:     pool,
		l:        l.With("writer_type", "nsq"),
		chPool: sync.Pool{
			New: func() any {
				return make(chan *nsq.ProducerTransaction, 1)
			},
		},
	}, nil
}

func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	ch := w.chPool.Get().(chan *nsq.ProducerTransaction)

	err := w.producer.PublishAsync(w.conf.Topic, msg, ch)
	if err != nil {
		callback(err)
		w.chPool.Put(ch)
		return
	}

	w.wg.Add(1)
	err = w.pool.Submit(func() {
		defer w.wg.Done()
		defer w.chPool.Put(ch)

		select {
		case tx := <-ch:
			callback(tx.Error)
		case <-ctx.Done():
			callback(ctx.Err())
		}
	})
	if err != nil {
		callback(err)
		w.chPool.Put(ch)
		w.wg.Done()
	}
}

func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	w.Produce(ctx, msg, callback)
}

func (w *Writer) Flush(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) CommitTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) Endpoint() string {
	return w.conf.Address
}

func (w *Writer) Close() error {
	w.wg.Wait()
	w.producer.Stop()
	if w.pool != nil {
		if w.conf.Pool.ReleaseTimeout != 0 {
			if err := w.pool.ReleaseTimeout(w.conf.Pool.ReleaseTimeout); err != nil {
				return fmt.Errorf("release pool: %w", err)
			}
		} else {
			w.pool.Release()
		}
	}
	return nil
}
