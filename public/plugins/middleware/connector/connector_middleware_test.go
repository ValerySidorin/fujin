package connector

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

// mockMiddleware is a test connector middleware
type mockMiddleware struct {
	writerCalls int
	readerCalls int
}

func (m *mockMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	m.writerCalls++
	return w
}

func (m *mockMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	m.readerCalls++
	return r
}

type mockCompiledMiddleware struct {
	middleware Middleware
	closed     int
}

func (m *mockCompiledMiddleware) Open(*slog.Logger) (Middleware, error) {
	return m.middleware, nil
}

func (m *mockCompiledMiddleware) Close(context.Context) error {
	m.closed++
	return nil
}

func TestCompilePreparesAndClosesGenerationMiddleware(t *testing.T) {
	compiled := &mockCompiledMiddleware{middleware: &mockMiddleware{}}
	name := "compiled_connector_middleware"
	if err := RegisterCompiled(name, func(any, *slog.Logger) (Compiled, error) {
		return compiled, nil
	}); err != nil {
		t.Fatal(err)
	}
	chain, err := Compile([]cmwconfig.Config{{Name: name}}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := chain.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if compiled.closed != 1 {
		t.Fatalf("compiled middleware closed %d times, want 1", compiled.closed)
	}
}

func TestRegister(t *testing.T) {
	// Register a test connector middleware
	err := Register("test_connector_middleware", func(config any, l *slog.Logger) (Middleware, error) {
		return &mockMiddleware{}, nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Try to register again - should fail
	err = Register("test_connector_middleware", func(config any, l *slog.Logger) (Middleware, error) {
		return &mockMiddleware{}, nil
	})
	if err == nil {
		t.Fatal("Register() should have failed for duplicate name")
	}
}

func TestGet(t *testing.T) {
	// Register a connector middleware for this test
	_ = Register("get_test", func(config any, l *slog.Logger) (Middleware, error) {
		return &mockMiddleware{}, nil
	})

	// Get existing connector middleware
	factory, ok := Get("get_test")
	if !ok {
		t.Fatal("Get() should find registered connector middleware")
	}
	if factory == nil {
		t.Fatal("Get() returned nil factory")
	}

	// Get non-existent connector middleware
	_, ok = Get("nonexistent")
	if ok {
		t.Fatal("Get() should not find non-existent connector middleware")
	}
}

func TestList(t *testing.T) {
	// Register a connector middleware to ensure List() has something to return
	_ = Register("list_test", func(config any, l *slog.Logger) (Middleware, error) {
		return &mockMiddleware{}, nil
	})

	names := List()
	if len(names) == 0 {
		t.Fatal("List() should return registered connector middlewares")
	}

	// Check that our registered connector middleware is in the list
	found := false
	for _, name := range names {
		if name == "list_test" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("List() should contain 'list_test' connector middleware")
	}
}
