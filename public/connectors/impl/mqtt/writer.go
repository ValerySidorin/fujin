//go:build mqtt

package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/panjf2000/ants/v2"
)

type Writer struct {
	conf   WriterConfig
	client mqtt.Client
	pool   *ants.Pool
	l      *slog.Logger
	wg     sync.WaitGroup
}

func NewWriter(conf WriterConfig, l *slog.Logger) (*Writer, error) {
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(conf.BrokerURL).
		SetClientID(conf.ClientID).
		SetCleanSession(conf.CleanSession).
		SetKeepAlive(conf.KeepAlive)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt: connect: %w", token.Error())
	}

	pool, err := ants.NewPool(conf.Pool.Size, ants.WithPreAlloc(conf.Pool.PreAlloc))
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return &Writer{
		conf:   conf,
		client: client,
		pool:   pool,
		l:      l,
	}, nil
}

func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	token := w.client.Publish(w.conf.Topic, w.conf.QoS, w.conf.Retain, msg)
	w.wg.Add(1)
	err := w.pool.Submit(func() {
		token.Wait()
		callback(token.Error())
		w.wg.Done()
	})

	if err != nil {
		callback(err)
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
	return w.conf.BrokerURL
}

func (w *Writer) Close() error {
	w.wg.Wait()
	w.client.Disconnect(uint(w.conf.DisconnectTimeout.Milliseconds()))
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
