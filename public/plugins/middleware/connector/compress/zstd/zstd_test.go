package zstd

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// --- mock writer ---

type mockWriter struct {
	produceCalled  bool
	hproduceCalled bool
	lastMsg        []byte
	lastHeaders    [][]byte
}

func (w *mockWriter) Produce(_ context.Context, msg []byte, cb func(err error)) {
	w.produceCalled = true
	w.lastMsg = msg
	if cb != nil {
		cb(nil)
	}
}

func (w *mockWriter) HProduce(_ context.Context, msg []byte, headers [][]byte, cb func(err error)) {
	w.hproduceCalled = true
	w.lastMsg = msg
	w.lastHeaders = headers
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

type mockReader struct {
	handler       func([]byte, string, ...any)
	headerHandler func([]byte, string, [][]byte, ...any)
}

func (r *mockReader) Subscribe(_ context.Context, ready func() error, h func([]byte, string, ...any)) error {
	r.handler = h
	return nil
}

func (r *mockReader) SubscribeWithHeaders(_ context.Context, ready func() error, h func([]byte, string, [][]byte, ...any)) error {
	r.headerHandler = h
	return nil
}

func (r *mockReader) Fetch(_ context.Context, _ uint32, fh func(uint32, error), mh func([]byte, string, ...any)) {
	r.handler = mh
	fh(0, nil)
}

func (r *mockReader) FetchWithHeaders(_ context.Context, _ uint32, fh func(uint32, error), mh func([]byte, string, [][]byte, ...any)) {
	r.headerHandler = mh
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

func newTestMiddleware(t *testing.T, cfg map[string]any) *compressZstdMiddleware {
	t.Helper()
	if cfg == nil {
		cfg = map[string]any{}
	}
	mw, err := newCompressZstdMiddleware(cfg, testLogger())
	if err != nil {
		t.Fatalf("newCompressZstdMiddleware: %v", err)
	}
	return mw.(*compressZstdMiddleware)
}

// --- roundtrip tests ---

func TestRoundtrip_Produce_Subscribe(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	original := []byte(`{"temperature":22.5,"humidity":65,"device":"sensor-001"}`)

	// Produce: compress
	mockW := &mockWriter{}
	wrappedW := mw.WrapWriter(mockW, "test")
	wrappedW.Produce(context.Background(), original, nil)

	if !mockW.produceCalled {
		t.Fatal("expected Produce to be called")
	}
	compressed := mockW.lastMsg
	if bytes.Equal(compressed, original) {
		t.Fatal("expected compressed data to differ from original")
	}

	// Subscribe: decompress
	mockR := &mockReader{}
	wrappedR := mw.WrapReader(mockR, "test")

	var received []byte
	wrappedR.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = msg
	})
	mockR.handler(compressed, "topic1")

	if !bytes.Equal(received, original) {
		t.Fatalf("roundtrip failed: got %q, want %q", received, original)
	}
}

func TestRoundtrip_HProduce_SubscribeWithHeaders(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	original := []byte(`{"data":"test"}`)
	headers := [][]byte{[]byte("key"), []byte("value")}

	mockW := &mockWriter{}
	wrappedW := mw.WrapWriter(mockW, "test")
	wrappedW.HProduce(context.Background(), original, headers, nil)

	compressed := mockW.lastMsg

	mockR := &mockReader{}
	wrappedR := mw.WrapReader(mockR, "test")

	var received []byte
	var receivedHeaders [][]byte
	wrappedR.SubscribeWithHeaders(context.Background(), func() error { return nil }, func(msg []byte, topic string, hs [][]byte, args ...any) {
		received = msg
		receivedHeaders = hs
	})
	mockR.headerHandler(compressed, "topic1", headers)

	if !bytes.Equal(received, original) {
		t.Fatalf("roundtrip failed: got %q, want %q", received, original)
	}
	if len(receivedHeaders) != 2 {
		t.Fatalf("expected 2 header entries, got %d", len(receivedHeaders))
	}
}

func TestRoundtrip_Fetch(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	original := []byte(`{"event":"click"}`)

	compressed := mw.compress(original)

	mockR := &mockReader{}
	wrappedR := mw.WrapReader(mockR, "test")

	var received []byte
	wrappedR.Fetch(context.Background(), 1, func(n uint32, err error) {}, func(msg []byte, topic string, args ...any) {
		received = msg
	})
	mockR.handler(compressed, "topic1")

	if !bytes.Equal(received, original) {
		t.Fatalf("fetch roundtrip failed: got %q, want %q", received, original)
	}
}

// --- compression effectiveness ---

func TestCompression_Reduces_Size(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	// Repetitive data compresses well.
	original := []byte(strings.Repeat(`{"sensor":"temp","value":22.5},`, 100))

	compressed := mw.compress(original)

	if len(compressed) >= len(original) {
		t.Fatalf("expected compression to reduce size: original=%d, compressed=%d", len(original), len(compressed))
	}
}

// --- decompress invalid data ---

func TestReader_Subscribe_InvalidData(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mockR := &mockReader{}
	wrappedR := mw.WrapReader(mockR, "test")

	var received bool
	wrappedR.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mockR.handler([]byte("not zstd compressed data"), "topic1")

	if received {
		t.Fatal("expected invalid compressed data to be skipped")
	}
}

// --- config tests ---

func TestConfig_Default(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}
}

func TestConfig_Levels(t *testing.T) {
	for _, level := range []string{"fastest", "default", "better", "best"} {
		t.Run(level, func(t *testing.T) {
			mw := newTestMiddleware(t, map[string]any{"level": level})
			if mw == nil {
				t.Fatalf("expected non-nil middleware for level %q", level)
			}
		})
	}
}

func TestConfig_InvalidLevel(t *testing.T) {
	_, err := newCompressZstdMiddleware(map[string]any{"level": "turbo"}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid level")
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
	mw := newTestMiddleware(t, nil)
	wrapped := mw.WrapWriter(&noopWriter{}, "test")

	var successCount atomic.Int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Produce(context.Background(), []byte(`{"data":"test payload for compression"}`), func(err error) {
				if err == nil {
					successCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	if successCount.Load() != 100 {
		t.Fatalf("expected 100 successes, got %d", successCount.Load())
	}
}
