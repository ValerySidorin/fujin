package tcp

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"

	"log/slog"
)

func TestListenAndServeInherited(t *testing.T) {
	// Create a TCP listener, extract its FD, close the original
	origLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := origLn.Addr().String()

	file, err := origLn.(*net.TCPListener).File()
	if err != nil {
		origLn.Close()
		t.Fatal(err)
	}
	origLn.Close() // close original; FD keeps socket alive

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := NewServer(serverconfig.TCPServerConfig{Addr: addr}, testCatalog(t), logger)

	ctx, cancel := context.WithCancel(context.Background())

	// Start serving on inherited FD
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServeInherited(ctx, file)
	}()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}

	// Connect to verify it's accepting
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal("dial inherited listener:", err)
	}
	conn.Close()

	// Shutdown
	cancel()
	if err := <-errCh; err != nil {
		t.Fatal("serve error:", err)
	}
}

func TestFDKey(t *testing.T) {
	srv := NewServer(
		serverconfig.TCPServerConfig{Addr: ":4850"},
		testCatalog(t),
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
	)
	if got := srv.FDKey(); got != "tcp::4850" {
		t.Fatalf("FDKey() = %q, want %q", got, "tcp::4850")
	}
}

func TestListenerFDs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := NewServer(serverconfig.TCPServerConfig{Addr: "127.0.0.1:0"}, testCatalog(t), logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}

	fds, err := srv.ListenerFDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(fds) != 1 {
		t.Fatalf("expected 1 FD, got %d", len(fds))
	}
	if fds[0].Type != "tcp" {
		t.Fatalf("expected type tcp, got %s", fds[0].Type)
	}

	// Verify dup'd FD is a valid listener
	ln, err := net.FileListener(fds[0].FD)
	if err != nil {
		t.Fatal("FD not a valid listener:", err)
	}
	ln.Close()
	fds[0].FD.Close()

	cancel()
	<-errCh
}

func TestCatalogReloadPublishesGenerationToServer(t *testing.T) {
	srv := NewServer(
		serverconfig.TCPServerConfig{Addr: "127.0.0.1:0"},
		testCatalog(t),
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	)
	previous := srv.catalog.Current()
	if err := srv.catalog.Reload(connectorconfig.ConnectorsConfig{}); err != nil {
		t.Fatal(err)
	}
	if srv.catalog.Current() == previous {
		t.Fatal("catalog reload did not publish a new generation")
	}
}

func testCatalog(t *testing.T) *connector.Catalog {
	t.Helper()
	catalog, err := connector.CompileCatalog(connectorconfig.ConnectorsConfig{}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
