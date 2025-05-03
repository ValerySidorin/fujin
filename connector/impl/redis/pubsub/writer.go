package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	"github.com/ValerySidorin/fujin/connector/cerr"
	"github.com/redis/rueidis"
)

type Writer struct {
	conf   WriterConfig
	client rueidis.Client
	l      *slog.Logger

	mu       sync.Mutex
	buffer   []rueidis.Completed
	callback []func(err error)

	batchSize int
	linger    time.Duration
	flushCh   chan struct{}
	closeCh   chan struct{}

	wg sync.WaitGroup

	bufPool sync.Pool
}

func NewWriter(conf WriterConfig, l *slog.Logger) (*Writer, error) {
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

	w := &Writer{
		conf:      conf,
		client:    client,
		l:         l,
		batchSize: conf.BatchSize,
		linger:    conf.Linger,
		flushCh:   make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				return make([]rueidis.Completed, 0, conf.BatchSize)
			},
		},
	}

	go w.flusher()
	return w, nil
}

func (w *Writer) Write(ctx context.Context, msg []byte, callback func(err error)) {
	w.wg.Add(1)

	cmd := w.client.B().
		Publish().
		Channel(w.conf.Channel).
		Message(unsafe.String(&msg[0], len(msg))).
		Build()

	w.mu.Lock()
	w.buffer = append(w.buffer, cmd)
	w.callback = append(w.callback, func(err error) {
		defer w.wg.Done()
		callback(err)
	})
	w.mu.Unlock()

	if len(w.buffer) >= w.batchSize {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}
}

func (w *Writer) flusher() {
	ticker := time.NewTicker(w.linger)
	defer ticker.Stop()

	for {
		select {
		case <-w.flushCh:
			w.flush()
		case <-ticker.C:
			w.flush()
		case <-w.closeCh:
			w.flush()
			return
		}
	}
}

func (w *Writer) flush() {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return
	}

	cmds := w.buffer
	cbs := w.callback

	w.buffer = w.bufPool.Get().([]rueidis.Completed)[:0]
	w.callback = nil
	w.mu.Unlock()

	results := w.client.DoMulti(context.Background(), cmds...)
	for i, r := range results {
		cbs[i](r.Error())
	}

	w.bufPool.Put(cmds)
}

func (w *Writer) Flush(_ context.Context) error {
	w.wg.Wait()
	return nil
}

func (w *Writer) BeginTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) CommitTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) RollbackTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) Endpoint() string {
	return w.conf.Endpoint
}

func (w *Writer) Close() error {
	close(w.closeCh)
	w.wg.Wait()
	w.client.Close()
	return nil
}
