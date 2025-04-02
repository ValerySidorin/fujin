package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Writer struct {
	conf     WriterConfig
	c        *kgo.Client
	endpoint string
	l        *slog.Logger
	wg       sync.WaitGroup
}

func NewWriter(conf WriterConfig, writerID string, l *slog.Logger) (*Writer, error) {
	c, err := kgo.NewClient(kgoOptsFromWriterConf(conf, writerID)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	return &Writer{
		conf:     conf,
		c:        c,
		endpoint: conf.Endpoint,
		l:        l,
	}, nil
}

func (w *Writer) Write(ctx context.Context, msg []byte, callback func(err error)) {
	w.wg.Add(1)
	w.c.Produce(ctx, &kgo.Record{
		Topic: w.conf.Topic,
		Value: msg,
	}, func(r *kgo.Record, err error) {
		callback(err)
		w.wg.Done()
	})
}

func (w *Writer) Flush(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return w.c.BeginTransaction()
}

func (w *Writer) CommitTx(ctx context.Context) error {
	if err := w.c.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}

	switch err := w.c.EndTransaction(ctx, kgo.TryCommit); err {
	case nil:
	case kerr.OperationNotAttempted:
		return w.RollbackTx(ctx)
	default:
		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	return nil
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	if err := w.c.AbortBufferedRecords(ctx); err != nil {
		return fmt.Errorf("kafka: abort buffered records: %w", err)
	}
	if err := w.c.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return fmt.Errorf("kafka: rollback transaction: %w", err)
	}

	return nil
}

func (w *Writer) Endpoint() string {
	return w.endpoint
}

func (w *Writer) Close() error {
	w.wg.Wait()
	w.c.Close()
	return nil
}
