package jq

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

func newTestMiddleware(t *testing.T, produceExpr, consumeExpr string) *filterJQMiddleware {
	t.Helper()
	cfg := map[string]any{}
	if produceExpr != "" {
		cfg["produce"] = produceExpr
	}
	if consumeExpr != "" {
		cfg["consume"] = consumeExpr
	}
	mw, err := newFilterJQMiddleware(cfg, testLogger())
	if err != nil {
		t.Fatalf("newFilterJQMiddleware: %v", err)
	}
	return mw.(*filterJQMiddleware)
}

// --- writer tests ---

func TestWriter_Produce_Pass(t *testing.T) {
	mw := newTestMiddleware(t, `.priority == "high"`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"priority":"high","data":1}`), func(err error) { cbErr = err })

	if !mock.produceCalled {
		t.Fatal("expected Produce to be forwarded")
	}
	if cbErr != nil {
		t.Fatalf("expected nil callback error, got: %v", cbErr)
	}
}

func TestWriter_Produce_Filtered(t *testing.T) {
	mw := newTestMiddleware(t, `.priority == "high"`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"priority":"low","data":1}`), func(err error) { cbErr = err })

	if mock.produceCalled {
		t.Fatal("expected Produce NOT to be forwarded")
	}
	var fe *FilteredError
	if !errors.As(cbErr, &fe) {
		t.Fatalf("expected FilteredError, got: %T (%v)", cbErr, cbErr)
	}
}

func TestWriter_Produce_InvalidJSON(t *testing.T) {
	mw := newTestMiddleware(t, `.x`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`not json`), func(err error) { cbErr = err })

	if mock.produceCalled {
		t.Fatal("expected Produce NOT to be forwarded for non-JSON")
	}
	if cbErr == nil {
		t.Fatal("expected error for non-JSON message")
	}
}

func TestWriter_HProduce_Pass(t *testing.T) {
	mw := newTestMiddleware(t, `.active`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	wrapped.HProduce(context.Background(), []byte(`{"active":true}`), nil, func(err error) {})

	if !mock.hproduceCalled {
		t.Fatal("expected HProduce to be forwarded")
	}
}

func TestWriter_HProduce_Filtered(t *testing.T) {
	mw := newTestMiddleware(t, `.active`, "")
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.HProduce(context.Background(), []byte(`{"active":false}`), nil, func(err error) { cbErr = err })

	if mock.hproduceCalled {
		t.Fatal("expected HProduce NOT to be forwarded")
	}
	if cbErr == nil {
		t.Fatal("expected FilteredError")
	}
}

// --- reader tests ---

func TestReader_Subscribe_Pass(t *testing.T) {
	mw := newTestMiddleware(t, "", `.country == "US"`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mock.handler([]byte(`{"country":"US","name":"alice"}`), "topic1")

	if !received {
		t.Fatal("expected message to be delivered")
	}
}

func TestReader_Subscribe_Filtered(t *testing.T) {
	mw := newTestMiddleware(t, "", `.country == "US"`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mock.handler([]byte(`{"country":"DE","name":"bob"}`), "topic1")

	if received {
		t.Fatal("expected message to be filtered out")
	}
}

func TestReader_Subscribe_InvalidJSON(t *testing.T) {
	mw := newTestMiddleware(t, "", `.x`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mock.handler([]byte(`not json`), "topic1")

	if received {
		t.Fatal("expected invalid JSON message to be skipped")
	}
}

func TestReader_SubscribeWithHeaders_Filtered(t *testing.T) {
	mw := newTestMiddleware(t, "", `.active`)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.SubscribeWithHeaders(context.Background(), func() error { return nil }, func(msg []byte, topic string, hs [][]byte, args ...any) {
		received = true
	})
	mock.headerHandler([]byte(`{"active":null}`), "topic1", nil)

	if received {
		t.Fatal("expected null to be filtered out")
	}
}

// --- passthrough tests ---

func TestWriter_ProduceOnly_ReaderPassthrough(t *testing.T) {
	mw := newTestMiddleware(t, `.x`, "")
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	if wrapped != mock {
		t.Fatal("expected WrapReader to return original reader when no consume expression")
	}
}

func TestReader_ConsumeOnly_WriterPassthrough(t *testing.T) {
	mw := newTestMiddleware(t, "", `.x`)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	if wrapped != mock {
		t.Fatal("expected WrapWriter to return original writer when no produce expression")
	}
}

// --- truthy tests ---

func TestIsTruthy(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want bool
	}{
		{"true", true, true},
		{"false", false, false},
		{"nil", nil, false},
		{"string", "hello", true},
		{"number", float64(42), true},
		{"zero", float64(0), true}, // jq: 0 is truthy (only false/null are falsy)
		{"empty string", "", true}, // jq: "" is truthy
		{"array", []any{1}, true},
		{"object", map[string]any{"a": 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTruthy(tt.val); got != tt.want {
				t.Fatalf("isTruthy(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

// --- config tests ---

func TestConfig_NoExpressions(t *testing.T) {
	_, err := newFilterJQMiddleware(map[string]any{}, testLogger())
	if err == nil {
		t.Fatal("expected error when no expressions provided")
	}
}

func TestConfig_InvalidProduceExpression(t *testing.T) {
	_, err := newFilterJQMiddleware(map[string]any{
		"produce": "{invalid jq @@!",
	}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid jq expression")
	}
}

func TestConfig_InvalidConsumeExpression(t *testing.T) {
	_, err := newFilterJQMiddleware(map[string]any{
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
	mw := newTestMiddleware(t, `.pass`, "")
	wrapped := mw.WrapWriter(&noopWriter{}, "test")

	var passCount atomic.Int64
	var filteredCount atomic.Int64
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var msg []byte
			if i%2 == 0 {
				msg = []byte(`{"pass":true}`)
			} else {
				msg = []byte(`{"pass":false}`)
			}
			wrapped.Produce(context.Background(), msg, func(err error) {
				if err == nil {
					passCount.Add(1)
				} else {
					filteredCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	if passCount.Load() != 50 {
		t.Fatalf("expected 50 passed, got %d", passCount.Load())
	}
	if filteredCount.Load() != 50 {
		t.Fatalf("expected 50 filtered, got %d", filteredCount.Load())
	}
}
