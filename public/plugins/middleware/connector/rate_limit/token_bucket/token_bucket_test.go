package token_bucket

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

// --- mock writer ---

type mockWriter struct {
	produceCalled  bool
	hproduceCalled bool
	lastMsg        []byte
}

func (w *mockWriter) Produce(_ context.Context, msg []byte, cb func(err error)) {
	w.produceCalled = true
	w.lastMsg = msg
	if cb != nil {
		cb(nil)
	}
}

func (w *mockWriter) HProduce(_ context.Context, msg []byte, _ [][]byte, cb func(err error)) {
	w.hproduceCalled = true
	w.lastMsg = msg
	if cb != nil {
		cb(nil)
	}
}

func (w *mockWriter) Flush(_ context.Context) error      { return nil }
func (w *mockWriter) BeginTx(_ context.Context) error    { return nil }
func (w *mockWriter) CommitTx(_ context.Context) error   { return nil }
func (w *mockWriter) RollbackTx(_ context.Context) error { return nil }
func (w *mockWriter) Close() error                       { return nil }

// --- mock reader ---

type mockReader struct{}

func (r *mockReader) Subscribe(_ context.Context, ready func() error, _ func([]byte, string, ...any)) error {
	return ready()
}
func (r *mockReader) SubscribeWithHeaders(_ context.Context, ready func() error, _ func([]byte, string, [][]byte, ...any)) error {
	return ready()
}
func (r *mockReader) Fetch(_ context.Context, _ uint32, fh func(uint32, error), _ func([]byte, string, ...any)) {
	fh(0, nil)
}
func (r *mockReader) FetchWithHeaders(_ context.Context, _ uint32, fh func(uint32, error), _ func([]byte, string, [][]byte, ...any)) {
	fh(0, nil)
}
func (r *mockReader) Ack(_ context.Context, _ [][]byte, ah func(error), _ func([]byte, error)) {
	ah(nil)
}
func (r *mockReader) Nack(_ context.Context, _ [][]byte, nh func(error), _ func([]byte, error)) {
	nh(nil)
}
func (r *mockReader) MsgIDArgsLen() int                                 { return 0 }
func (r *mockReader) EncodeMsgID(buf []byte, _ string, _ ...any) []byte { return buf }
func (r *mockReader) AutoCommit() bool                                  { return true }
func (r *mockReader) Close() error                                      { return nil }

// --- helpers ---

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newTestMiddleware(t *testing.T, rateVal float64, burst int) *rateLimitMiddleware {
	t.Helper()
	mw, err := newRateLimitMiddleware(map[string]any{
		"rate":  rateVal,
		"burst": burst,
	}, testLogger())
	if err != nil {
		t.Fatalf("newRateLimitMiddleware: %v", err)
	}
	return mw.(*rateLimitMiddleware)
}

// --- writer tests ---

func TestWriter_Produce_WithinLimit(t *testing.T) {
	mw := newTestMiddleware(t, 1000, 10)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"id":1}`), func(err error) { cbErr = err })

	if !mock.produceCalled {
		t.Fatal("expected Produce to be forwarded")
	}
	if cbErr != nil {
		t.Fatalf("expected nil callback error, got: %v", cbErr)
	}
}

func TestWriter_Produce_ExceedsLimit(t *testing.T) {
	// burst=2 means only 2 messages can pass immediately
	mw := newTestMiddleware(t, 1, 2)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	// First 2 should pass (burst)
	wrapped.Produce(context.Background(), []byte(`{"id":1}`), nil)
	wrapped.Produce(context.Background(), []byte(`{"id":2}`), nil)

	// Third should be rejected
	mock.produceCalled = false
	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"id":3}`), func(err error) { cbErr = err })

	if mock.produceCalled {
		t.Fatal("expected Produce to be rejected after burst exceeded")
	}
	var rle *RateLimitError
	if !errors.As(cbErr, &rle) {
		t.Fatalf("expected RateLimitError, got: %T (%v)", cbErr, cbErr)
	}
}

func TestWriter_HProduce_WithinLimit(t *testing.T) {
	mw := newTestMiddleware(t, 1000, 10)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	wrapped.HProduce(context.Background(), []byte(`{"id":1}`), [][]byte{[]byte("k"), []byte("v")}, nil)

	if !mock.hproduceCalled {
		t.Fatal("expected HProduce to be forwarded")
	}
}

func TestWriter_HProduce_ExceedsLimit(t *testing.T) {
	mw := newTestMiddleware(t, 1, 1)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	wrapped.HProduce(context.Background(), []byte(`{"id":1}`), nil, nil)

	mock.hproduceCalled = false
	var cbErr error
	wrapped.HProduce(context.Background(), []byte(`{"id":2}`), nil, func(err error) { cbErr = err })

	if mock.hproduceCalled {
		t.Fatal("expected HProduce to be rejected")
	}
	if cbErr == nil {
		t.Fatal("expected RateLimitError")
	}
}

// --- reader passthrough ---

func TestReader_Passthrough(t *testing.T) {
	mw := newTestMiddleware(t, 100, 10)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	if wrapped != mock {
		t.Fatal("expected WrapReader to return original reader (no consume rate limiting)")
	}
}

// --- config tests ---

func TestConfig_MissingRate(t *testing.T) {
	_, err := newRateLimitMiddleware(map[string]any{"burst": 10}, testLogger())
	if err == nil {
		t.Fatal("expected error when rate is missing")
	}
}

func TestConfig_MissingBurst(t *testing.T) {
	_, err := newRateLimitMiddleware(map[string]any{"rate": float64(100)}, testLogger())
	if err == nil {
		t.Fatal("expected error when burst is missing")
	}
}

func TestConfig_ZeroRate(t *testing.T) {
	_, err := newRateLimitMiddleware(map[string]any{"rate": float64(0), "burst": 10}, testLogger())
	if err == nil {
		t.Fatal("expected error when rate is zero")
	}
}

func TestConfig_NegativeBurst(t *testing.T) {
	_, err := newRateLimitMiddleware(map[string]any{"rate": float64(100), "burst": float64(-1)}, testLogger())
	if err == nil {
		t.Fatal("expected error when burst is negative")
	}
}

// --- concurrency test ---

type noopWriter struct{}

func (w *noopWriter) Produce(_ context.Context, _ []byte, cb func(err error)) {
	if cb != nil {
		cb(nil)
	}
}
func (w *noopWriter) HProduce(_ context.Context, _ []byte, _ [][]byte, cb func(err error)) {
	if cb != nil {
		cb(nil)
	}
}
func (w *noopWriter) Flush(_ context.Context) error      { return nil }
func (w *noopWriter) BeginTx(_ context.Context) error    { return nil }
func (w *noopWriter) CommitTx(_ context.Context) error   { return nil }
func (w *noopWriter) RollbackTx(_ context.Context) error { return nil }
func (w *noopWriter) Close() error                       { return nil }

func TestWriter_Produce_Concurrent(t *testing.T) {
	// burst=10 means at most 10 can pass immediately
	mw := newTestMiddleware(t, 1, 10)
	wrapped := mw.WrapWriter(&noopWriter{}, "test")

	var passedCount atomic.Int64
	var rejectedCount atomic.Int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Produce(context.Background(), []byte(`{"data":"test"}`), func(err error) {
				if err == nil {
					passedCount.Add(1)
				} else {
					rejectedCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	passed := passedCount.Load()
	rejected := rejectedCount.Load()

	if passed > 11 {
		// burst=10 + possibly 1 from rate refill
		t.Fatalf("expected at most ~11 passed, got %d", passed)
	}
	if passed+rejected != 100 {
		t.Fatalf("expected 100 total, got passed=%d rejected=%d", passed, rejected)
	}
}
