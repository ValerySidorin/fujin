package connector

import (
	"context"
	"errors"
	"sync"
)

var ErrWriterClosed = errors.New("connector writer closed")

// WriterContractCompliant marks writers that natively provide exactly-once callbacks,
// snapshot Flush semantics, and deterministic pending resolution on Close.
type WriterContractCompliant interface {
	WriterContractCompliant()
}

// EnforceWriterContract wraps a writer with exactly-once callbacks, snapshot Flush,
// and deterministic pending-callback resolution on Close.
func EnforceWriterContract(writer WriteCloser) WriteCloser {
	if writer == nil {
		return nil
	}
	if _, ok := writer.(WriterContractCompliant); ok {
		return writer
	}
	return newContractWriter(writer)
}

type contractWriter struct {
	writer WriteCloser

	flushMu sync.Mutex
	mu      sync.Mutex
	notify  chan struct{}

	next             uint64
	completedThrough uint64
	flushedThrough   uint64
	completed        map[uint64]error
	pending          map[uint64]*contractCallback
	closed           bool
	closeOnce        sync.Once
	closeErr         error
}

type contractCallback struct {
	once     sync.Once
	writer   *contractWriter
	sequence uint64
	callback func(error)
}

func newContractWriter(writer WriteCloser) *contractWriter {
	return &contractWriter{
		writer:    writer,
		notify:    make(chan struct{}),
		completed: make(map[uint64]error),
		pending:   make(map[uint64]*contractCallback),
	}
}

func (w *contractWriter) Produce(ctx context.Context, message []byte, callback func(error)) {
	state := w.accept(callback)
	if state != nil {
		w.writer.Produce(ctx, message, state.complete)
	}
}

func (w *contractWriter) HProduce(ctx context.Context, message []byte, headers [][]byte, callback func(error)) {
	state := w.accept(callback)
	if state != nil {
		w.writer.HProduce(ctx, message, headers, state.complete)
	}
}

func (w *contractWriter) accept(callback func(error)) *contractCallback {
	if callback == nil {
		callback = func(error) {}
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		callback(ErrWriterClosed)
		return nil
	}
	w.next++
	state := &contractCallback{writer: w, sequence: w.next, callback: callback}
	w.pending[state.sequence] = state
	w.mu.Unlock()
	return state
}

func (c *contractCallback) complete(err error) {
	c.once.Do(func() {
		c.callback(err)
		w := c.writer
		w.mu.Lock()
		delete(w.pending, c.sequence)
		w.completed[c.sequence] = err
		for {
			if _, ok := w.completed[w.completedThrough+1]; !ok {
				break
			}
			w.completedThrough++
		}
		close(w.notify)
		w.notify = make(chan struct{})
		w.mu.Unlock()
	})
}

func (w *contractWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	snapshot := w.next
	w.mu.Unlock()

	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if err := w.writer.Flush(ctx); err != nil {
		return err
	}
	for {
		w.mu.Lock()
		if w.completedThrough >= snapshot {
			var errs []error
			for sequence := w.flushedThrough + 1; sequence <= snapshot; sequence++ {
				if err := w.completed[sequence]; err != nil {
					errs = append(errs, err)
				}
				delete(w.completed, sequence)
			}
			if snapshot > w.flushedThrough {
				w.flushedThrough = snapshot
			}
			w.mu.Unlock()
			return errors.Join(errs...)
		}
		notify := w.notify
		w.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (w *contractWriter) BeginTx(ctx context.Context) error    { return w.writer.BeginTx(ctx) }
func (w *contractWriter) CommitTx(ctx context.Context) error   { return w.writer.CommitTx(ctx) }
func (w *contractWriter) RollbackTx(ctx context.Context) error { return w.writer.RollbackTx(ctx) }

func (w *contractWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		pending := make([]*contractCallback, 0, len(w.pending))
		for _, callback := range w.pending {
			pending = append(pending, callback)
		}
		w.mu.Unlock()
		for _, callback := range pending {
			callback.complete(ErrWriterClosed)
		}
		w.closeErr = w.writer.Close()
	})
	return w.closeErr
}
