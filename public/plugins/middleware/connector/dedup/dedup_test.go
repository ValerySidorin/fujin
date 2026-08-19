package dedup

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	subscribeCalled bool
	handler         func([]byte, string, ...any)
	headerHandler   func([]byte, string, [][]byte, ...any)
}

func (r *mockReader) Subscribe(_ context.Context, ready func() error, h func([]byte, string, ...any)) error {
	r.subscribeCalled = true
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

func newTestMiddleware(t *testing.T, cfg map[string]any) *dedupMiddleware {
	t.Helper()
	if cfg == nil {
		cfg = map[string]any{}
	}
	// Use short TTL and cleanup for tests.
	if _, ok := cfg["ttl"]; !ok {
		cfg["ttl"] = "1m"
	}
	if _, ok := cfg["cleanup_interval"]; !ok {
		cfg["cleanup_interval"] = "1m"
	}
	mw, err := newDedupMiddleware(cfg, testLogger())
	if err != nil {
		t.Fatalf("newDedupMiddleware: %v", err)
	}
	dm := mw.(*dedupMiddleware)
	t.Cleanup(func() {
		dm.produceStore.Stop()
		dm.consumeStore.Stop()
	})
	return dm
}

// --- writer tests: content hash ---

func TestWriter_Produce_FirstMessage(t *testing.T) {
	mw := newTestMiddleware(t, nil)
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

func TestWriter_Produce_Duplicate(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	msg := []byte(`{"id":1}`)
	wrapped.Produce(context.Background(), msg, nil)

	mock.produceCalled = false
	var cbErr error
	wrapped.Produce(context.Background(), msg, func(err error) { cbErr = err })

	if mock.produceCalled {
		t.Fatal("expected duplicate Produce NOT to be forwarded")
	}
	var de *DuplicateError
	if !errors.As(cbErr, &de) {
		t.Fatalf("expected DuplicateError, got: %T (%v)", cbErr, cbErr)
	}
}

func TestWriter_Produce_DifferentMessages(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	wrapped.Produce(context.Background(), []byte(`{"id":1}`), nil)
	mock.produceCalled = false

	wrapped.Produce(context.Background(), []byte(`{"id":2}`), nil)

	if !mock.produceCalled {
		t.Fatal("expected different message to be forwarded")
	}
}

func TestWriter_HProduce_Duplicate(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	msg := []byte(`{"id":1}`)
	headers := [][]byte{[]byte("key"), []byte("val")}
	wrapped.HProduce(context.Background(), msg, headers, nil)

	mock.hproduceCalled = false
	var cbErr error
	wrapped.HProduce(context.Background(), msg, headers, func(err error) { cbErr = err })

	if mock.hproduceCalled {
		t.Fatal("expected duplicate HProduce NOT to be forwarded")
	}
	if cbErr == nil {
		t.Fatal("expected DuplicateError")
	}
}

// --- reader tests: content hash ---

func TestReader_Subscribe_FirstMessage(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mock.handler([]byte(`{"id":1}`), "topic1")

	if !received {
		t.Fatal("expected first message to be delivered")
	}
}

func TestReader_Subscribe_Duplicate(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var count int
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		count++
	})

	msg := []byte(`{"id":1}`)
	mock.handler(msg, "topic1")
	mock.handler(msg, "topic1")
	mock.handler(msg, "topic1")

	if count != 1 {
		t.Fatalf("expected 1 delivery, got %d", count)
	}
}

func TestReader_SubscribeWithHeaders_Duplicate(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var count int
	wrapped.SubscribeWithHeaders(context.Background(), func() error { return nil }, func(msg []byte, topic string, hs [][]byte, args ...any) {
		count++
	})

	msg := []byte(`{"id":1}`)
	mock.headerHandler(msg, "topic1", nil)
	mock.headerHandler(msg, "topic1", nil)

	if count != 1 {
		t.Fatalf("expected 1 delivery, got %d", count)
	}
}

// --- header key strategy ---

func TestWriter_HProduce_HeaderKey(t *testing.T) {
	mw := newTestMiddleware(t, map[string]any{"key": "header:X-Msg-ID"})
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	// Same payload but different header IDs → both should pass.
	msg := []byte(`{"data":"same"}`)
	wrapped.HProduce(context.Background(), msg, [][]byte{[]byte("X-Msg-ID"), []byte("id-1")}, nil)
	mock.hproduceCalled = false

	wrapped.HProduce(context.Background(), msg, [][]byte{[]byte("X-Msg-ID"), []byte("id-2")}, nil)
	if !mock.hproduceCalled {
		t.Fatal("expected message with different header ID to be forwarded")
	}

	// Same header ID → duplicate.
	mock.hproduceCalled = false
	var cbErr error
	wrapped.HProduce(context.Background(), msg, [][]byte{[]byte("X-Msg-ID"), []byte("id-1")}, func(err error) { cbErr = err })
	if mock.hproduceCalled {
		t.Fatal("expected duplicate header ID to be rejected")
	}
	if cbErr == nil {
		t.Fatal("expected DuplicateError")
	}
}

func TestWriter_Produce_HeaderKey_FallbackToContentHash(t *testing.T) {
	mw := newTestMiddleware(t, map[string]any{"key": "header:X-Msg-ID"})
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	// Produce (no headers) should fallback to content hash.
	msg := []byte(`{"data":"test"}`)
	wrapped.Produce(context.Background(), msg, nil)
	mock.produceCalled = false

	wrapped.Produce(context.Background(), msg, nil)
	if mock.produceCalled {
		t.Fatal("expected duplicate content hash to be rejected (header fallback)")
	}
}

// --- jq key strategy ---

func TestWriter_Produce_JQKey(t *testing.T) {
	mw := newTestMiddleware(t, map[string]any{"key": "jq:.event_id"})
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	// Same event_id, different payload → duplicate.
	wrapped.Produce(context.Background(), []byte(`{"event_id":"abc","data":1}`), nil)
	mock.produceCalled = false

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"event_id":"abc","data":2}`), func(err error) { cbErr = err })
	if mock.produceCalled {
		t.Fatal("expected duplicate event_id to be rejected")
	}
	if cbErr == nil {
		t.Fatal("expected DuplicateError")
	}

	// Different event_id → passes.
	mock.produceCalled = false
	wrapped.Produce(context.Background(), []byte(`{"event_id":"def","data":3}`), nil)
	if !mock.produceCalled {
		t.Fatal("expected different event_id to be forwarded")
	}
}

func TestWriter_Produce_JQKey_InvalidJSON(t *testing.T) {
	mw := newTestMiddleware(t, map[string]any{"key": "jq:.event_id"})
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`not json`), func(err error) { cbErr = err })

	if mock.produceCalled {
		t.Fatal("expected non-JSON to be rejected")
	}
	if cbErr == nil {
		t.Fatal("expected error for non-JSON message")
	}
}

// --- TTL expiry ---

func TestStore_TTLExpiry(t *testing.T) {
	s := newDedupStore(50*time.Millisecond, 10*time.Millisecond)
	defer s.Stop()

	key := keyFromContent([]byte("test"))

	if !s.Add(key) {
		t.Fatal("expected first add to succeed")
	}
	if s.Add(key) {
		t.Fatal("expected immediate re-add to fail (duplicate)")
	}

	time.Sleep(100 * time.Millisecond)

	if !s.Add(key) {
		t.Fatal("expected add after TTL to succeed")
	}
}

// --- produce/consume store isolation ---

func TestStoreIsolation(t *testing.T) {
	mw := newTestMiddleware(t, nil)
	mock := &mockWriter{}
	wrappedW := mw.WrapWriter(mock, "test")

	mockR := &mockReader{}
	wrappedR := mw.WrapReader(mockR, "test")

	msg := []byte(`{"id":1}`)

	// Produce the message.
	wrappedW.Produce(context.Background(), msg, nil)
	if !mock.produceCalled {
		t.Fatal("expected first produce to pass")
	}

	// Same message on consume side should still be delivered (separate store).
	var received bool
	wrappedR.Subscribe(context.Background(), func() error { return nil }, func(m []byte, topic string, args ...any) {
		received = true
	})
	mockR.handler(msg, "topic1")

	if !received {
		t.Fatal("expected consume side to deliver message (separate store from produce)")
	}
}

// --- config tests ---

func TestConfig_InvalidTTL(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"ttl": "nope"}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid ttl")
	}
}

func TestConfig_InvalidCleanupInterval(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"cleanup_interval": "nope"}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid cleanup_interval")
	}
}

func TestConfig_UnknownKeyStrategy(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"key": "unknown_strategy"}, testLogger())
	if err == nil {
		t.Fatal("expected error for unknown key strategy")
	}
}

func TestConfig_EmptyHeaderName(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"key": "header:"}, testLogger())
	if err == nil {
		t.Fatal("expected error for empty header name")
	}
}

func TestConfig_InvalidJQExpression(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"key": "jq:{invalid @@!"}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid jq expression")
	}
}

func TestConfig_EmptyJQExpression(t *testing.T) {
	_, err := newDedupMiddleware(map[string]any{"key": "jq:"}, testLogger())
	if err == nil {
		t.Fatal("expected error for empty jq expression")
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

	var passedCount atomic.Int64
	var dupCount atomic.Int64
	var wg sync.WaitGroup

	// 100 goroutines all producing the same message — only first should pass.
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Produce(context.Background(), []byte(`{"same":"msg"}`), func(err error) {
				if err == nil {
					passedCount.Add(1)
				} else {
					dupCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	if passedCount.Load() != 1 {
		t.Fatalf("expected exactly 1 message to pass, got %d", passedCount.Load())
	}
	if dupCount.Load() != 99 {
		t.Fatalf("expected 99 duplicates, got %d", dupCount.Load())
	}
}
