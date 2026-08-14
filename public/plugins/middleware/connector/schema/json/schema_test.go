package json

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
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

const testSchema = `{
	"type": "object",
	"required": ["name", "age"],
	"properties": {
		"name": {"type": "string"},
		"age": {"type": "integer", "minimum": 0}
	},
	"additionalProperties": false
}`

func newTestMiddleware(t *testing.T) *schemaMiddleware {
	t.Helper()
	mw, err := newSchemaMiddleware(map[string]any{"schema": testSchema}, testLogger())
	if err != nil {
		t.Fatalf("newSchemaMiddleware: %v", err)
	}
	return mw.(*schemaMiddleware)
}

// --- writer tests ---

func TestWriter_Produce_ValidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"name":"alice","age":30}`), func(err error) {
		cbErr = err
	})

	if !mock.produceCalled {
		t.Fatal("expected Produce to be forwarded")
	}
	if cbErr != nil {
		t.Fatalf("expected nil callback error, got: %v", cbErr)
	}
}

func TestWriter_Produce_InvalidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`{"name":"alice","age":"not_a_number"}`), func(err error) {
		cbErr = err
	})

	if mock.produceCalled {
		t.Fatal("expected Produce NOT to be forwarded for invalid message")
	}
	if cbErr == nil {
		t.Fatal("expected validation error in callback")
	}
	var ve *ValidationError
	if !errors.As(cbErr, &ve) {
		t.Fatalf("expected ValidationError, got: %T", cbErr)
	}
}

func TestWriter_Produce_NotJSON(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.Produce(context.Background(), []byte(`not json at all`), func(err error) {
		cbErr = err
	})

	if mock.produceCalled {
		t.Fatal("expected Produce NOT to be forwarded for non-JSON")
	}
	if cbErr == nil {
		t.Fatal("expected error for non-JSON message")
	}
}

func TestWriter_HProduce_ValidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	headers := [][]byte{[]byte("key"), []byte("value")}
	wrapped.HProduce(context.Background(), []byte(`{"name":"bob","age":25}`), headers, func(err error) {})

	if !mock.hproduceCalled {
		t.Fatal("expected HProduce to be forwarded")
	}
}

func TestWriter_HProduce_InvalidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockWriter{}
	wrapped := mw.WrapWriter(mock, "test")

	var cbErr error
	wrapped.HProduce(context.Background(), []byte(`{}`), nil, func(err error) {
		cbErr = err
	})

	if mock.hproduceCalled {
		t.Fatal("expected HProduce NOT to be forwarded")
	}
	if cbErr == nil {
		t.Fatal("expected validation error")
	}
}

// --- reader tests ---

func TestReader_Subscribe_ValidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {})

	if !mock.subscribeCalled {
		t.Fatal("expected Subscribe to be called")
	}

	// Simulate broker delivering a valid message
	var received bool
	// Re-subscribe to capture the wrapped handler
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})
	mock.handler([]byte(`{"name":"alice","age":30}`), "topic1")

	if !received {
		t.Fatal("expected valid message to be passed to handler")
	}
}

func TestReader_Subscribe_InvalidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.Subscribe(context.Background(), func() error { return nil }, func(msg []byte, topic string, args ...any) {
		received = true
	})

	// Simulate broker delivering an invalid message
	mock.handler([]byte(`{"bad":"data"}`), "topic1")

	if received {
		t.Fatal("expected invalid message to be skipped")
	}
}

func TestReader_SubscribeWithHeaders_InvalidMessage(t *testing.T) {
	mw := newTestMiddleware(t)
	mock := &mockReader{}
	wrapped := mw.WrapReader(mock, "test")

	var received bool
	wrapped.SubscribeWithHeaders(context.Background(), func() error { return nil }, func(msg []byte, topic string, hs [][]byte, args ...any) {
		received = true
	})

	mock.headerHandler([]byte(`not json`), "topic1", nil)

	if received {
		t.Fatal("expected invalid message to be skipped")
	}
}

// --- config tests ---

func TestConfig_InlineSchema(t *testing.T) {
	mw, err := newSchemaMiddleware(map[string]any{
		"schema": `{"type":"string"}`,
	}, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}
}

func TestConfig_SchemaFile(t *testing.T) {
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "test.json")
	os.WriteFile(schemaFile, []byte(`{"type":"object"}`), 0644)

	mw, err := newSchemaMiddleware(map[string]any{
		"schema_path": schemaFile,
	}, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}
}

func TestConfig_NoSchema(t *testing.T) {
	_, err := newSchemaMiddleware(map[string]any{}, testLogger())
	if err == nil {
		t.Fatal("expected error when no schema provided")
	}
}

func TestConfig_InvalidInlineSchema(t *testing.T) {
	_, err := newSchemaMiddleware(map[string]any{
		"schema": `{not valid json`,
	}, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid schema JSON")
	}
}

// --- concurrency test ---

// noopWriter is a thread-safe writer that does nothing.
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
	mw := newTestMiddleware(t)
	wrapped := mw.WrapWriter(&noopWriter{}, "test")

	var validCount atomic.Int64
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := []byte(`{"name":"alice","age":30}`)
			if i%2 == 0 {
				msg = []byte(`{"bad":"data"}`)
			}
			wrapped.Produce(context.Background(), msg, func(err error) {
				if err == nil {
					validCount.Add(1)
				}
			})
		}()
	}
	wg.Wait()

	if validCount.Load() != 50 {
		t.Fatalf("expected 50 valid messages, got %d", validCount.Load())
	}
}
