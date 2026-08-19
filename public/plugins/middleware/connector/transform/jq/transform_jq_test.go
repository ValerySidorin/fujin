package jq

import (
	"context"
	"encoding/json"
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

func newTestMiddleware(t *testing.T, produceExpr, consumeExpr string) *transformJQMiddleware {
	t.Helper()
	cfg := map[string]any{}
	if produceExpr != "" {
		cfg["produce"] = produceExpr
	}
	if consumeExpr != "" {
		cfg["consume"] = consumeExpr
	}
	mw, err := newTransformJQMiddleware(cfg, testLogger())
	if err != nil {
		t.Fatalf("newTransformJQMiddleware: %v", err)
	}
	return mw.(*transformJQMiddleware)
}

// --- writer tests ---

func TestWriter_Produce_Transform(t *testing.T) {
	mw := newTestMiddleware(t, `{name: .n, age: .a}`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"n":"alice","a":30}`), func(err error) {
		cbErr = err
	})

	if !mock.produceCalled {
		t.Fatal("expected Produce to be forwarded")
	}
	if cbErr != nil {
		t.Fatalf("expected nil callback error, got: %v", cbErr)
	}

	var result map[string]any
	if err := json.Unmarshal(mock.lastMsg, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", result["name"])
	}
	if result["age"] != float64(30) {
		t.Fatalf("expected age=30, got %v", result["age"])
	}
}

func TestWriter_Produce_InvalidJSON(t *testing.T) {
	mw := newTestMiddleware(t, `.`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`not json`), func(err error) {
		cbErr = err
	})

	if mock.produceCalled {
		t.Fatal("expected Produce NOT to be forwarded for non-JSON")
	}
	if cbErr == nil {
		t.Fatal("expected error for non-JSON message")
	}
	var te *TransformError
	if !errors.As(cbErr, &te) {
		t.Fatalf("expected TransformError, got: %T", cbErr)
	}
}

func TestWriter_HProduce_Transform(t *testing.T) {
	mw := newTestMiddleware(t, `{x: .a}`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	headers := [][]byte{[]byte("key"), []byte("value")}
	wrapped.HProduce(context.Background(), []byte(`{"a":1}`), headers, func(err error) {})

	if !mock.hproduceCalled {
		t.Fatal("expected HProduce to be forwarded")
	}

	var result map[string]any
	json.Unmarshal(mock.lastMsg, &result)
	if result["x"] != float64(1) {
		t.Fatalf("expected x=1, got %v", result["x"])
	}
}

// --- reader tests ---

func TestReader_Subscribe_Transform(t *testing.T) {
	mw := newTestMiddleware(t, "", `{full_name: .n}`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var receivedMsg []byte
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		receivedMsg = msg
	})

	mock.handler([]byte(`{"n":"bob"}`), "topic1")

	if receivedMsg == nil {
		t.Fatal("expected message to be delivered")
	}

	var result map[string]any
	json.Unmarshal(receivedMsg, &result)
	if result["full_name"] != "bob" {
		t.Fatalf("expected full_name=bob, got %v", result["full_name"])
	}
}

func TestReader_Subscribe_InvalidJSON(t *testing.T) {
	mw := newTestMiddleware(t, "", `.`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})

	mock.handler([]byte(`not json`), "topic1")

	if received {
		t.Fatal("expected invalid message to be skipped")
	}
}

func TestReader_SubscribeWithHeaders_Transform(t *testing.T) {
	mw := newTestMiddleware(t, "", `{v: .val}`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var receivedMsg []byte
	wrapped.SubscribeWithHeaders(context.Background(), func() error { return nil }, func(msg []byte, topic string, hs [][]byte, args ...any) {
		receivedMsg = msg
	})

	mock.headerHandler([]byte(`{"val":42}`), "topic1", nil)

	var result map[string]any
	json.Unmarshal(receivedMsg, &result)
	if result["v"] != float64(42) {
		t.Fatalf("expected v=42, got %v", result["v"])
	}
}

// --- passthrough tests ---

func TestWriter_ProduceOnly_ReaderPassthrough(t *testing.T) {
	mw := newTestMiddleware(t, `{x: .a}`, "")
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	// With no consume expression, WrapReader should return original reader
	if wrapped != mock {
		t.Fatal("expected WrapReader to return original reader when no consume expression")
	}
}

func TestReader_ConsumeOnly_WriterPassthrough(t *testing.T) {
	mw := newTestMiddleware(t, "", `{x: .a}`)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	// With no produce expression, WrapWriter should return original writer
	if wrapped != mock {
		t.Fatal("expected WrapWriter to return original writer when no produce expression")
	}
}

// --- config tests ---

func TestConfig_NoExpressions(t *testing.T) {
	_, err := newTransformJQMiddleware(map[string]any{}, testLogger())
	if err == nil {
		t.Fatal("expected error when no expressions provided")
	}
}

func TestConfig_InvalidProduceExpression(t *testing.T) {
	_, err := newTransformJQMiddleware(map[string]any{
		"produce": "{invalid jq @@!",
	}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid jq expression")
	}
}

func TestConfig_InvalidConsumeExpression(t *testing.T) {
	_, err := newTransformJQMiddleware(map[string]any{
		"consume": "{invalid jq @@!",
	}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid jq expression")
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
	mw := newTestMiddleware(t, `{v: .x}`, "")
	wrapped := mw.WrapWriter(&noopWriter{}, "test")

	var successCount atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Produce(context.Background(), []byte(`{"x":1}`), func(err error) {
				if err == nil {
					successCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	if successCount.Load() != 100 {
		t.Fatalf("expected 100 successful transforms, got %d", successCount.Load())
	}
}
