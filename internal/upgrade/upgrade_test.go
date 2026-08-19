//go:build unix

package upgrade

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/transport"
)

// mockServer implements UpgradableServer for testing.
type mockServer struct {
	listeners []net.Listener
	fdMetas   []struct {
		typ, addr string
		meta      map[string]string
	}
}

func newMockServer(listeners ...net.Listener) *mockServer {
	m := &mockServer{listeners: listeners}
	for _, ln := range listeners {
		m.fdMetas = append(m.fdMetas, struct {
			typ, addr string
			meta      map[string]string
		}{typ: "tcp", addr: ln.Addr().String(), meta: map[string]string{}})
	}
	return m
}

func (m *mockServer) ListenerFDs() ([]transport.ListenerFD, error) {
	var fds []transport.ListenerFD
	for i, ln := range m.listeners {
		file, err := ln.(*net.TCPListener).File()
		if err != nil {
			return nil, err
		}
		fds = append(fds, transport.ListenerFD{
			FD:   file,
			Type: m.fdMetas[i].typ,
			Addr: m.fdMetas[i].addr,
			Meta: m.fdMetas[i].meta,
		})
	}
	return fds, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testSockPath creates a short temp socket path (macOS has 104-char limit for Unix sockets).
func testSockPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "fujin-test-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestUpgradeSequence(t *testing.T) {
	sockPath := testSockPath(t)

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpLn.Close()

	drainCalled := make(chan struct{})
	srv := newMockServer(tcpLn)
	upgrader := NewUpgrader(sockPath, srv, func() { close(drainCalled) }, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upgradeDone := make(chan error, 1)
	go func() {
		upgradeDone <- upgrader.ListenForUpgrade(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	fds, conn, err := RequestUpgrade(sockPath)
	if err != nil {
		t.Fatal("request upgrade:", err)
	}

	if len(fds) != 1 {
		t.Fatalf("expected 1 FD, got %d", len(fds))
	}

	key := "tcp:" + tcpLn.Addr().String()
	fd, ok := fds[key]
	if !ok {
		t.Fatalf("expected key %q in fds, got keys: %v", key, keysOf(fds))
	}

	inheritedLn, err := net.FileListener(fd)
	if err != nil {
		t.Fatal("inherited FD not a valid listener:", err)
	}
	inheritedLn.Close()
	fd.Close()

	if err := SignalReady(sockPath, conn); err != nil {
		t.Fatal("signal ready:", err)
	}
	conn.Close()

	select {
	case <-drainCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("drain was not called within timeout")
	}

	select {
	case err := <-upgradeDone:
		if err != nil {
			t.Fatal("upgrade handler error:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upgrade handler did not return within timeout")
	}
}

func TestUpgradeSequence_MultipleFDs(t *testing.T) {
	sockPath := testSockPath(t)

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()

	srv := newMockServer(ln1, ln2)
	// Mark second listener as gRPC
	srv.fdMetas[1].meta = map[string]string{"grpc": "true"}

	drainCalled := make(chan struct{})
	upgrader := NewUpgrader(sockPath, srv, func() { close(drainCalled) }, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		upgrader.ListenForUpgrade(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	fds, conn, err := RequestUpgrade(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if len(fds) != 2 {
		t.Fatalf("expected 2 FDs, got %d", len(fds))
	}

	// Verify keys: one plain TCP, one gRPC
	key1 := "tcp:" + ln1.Addr().String()
	key2 := "tcp:" + ln2.Addr().String() + ":grpc"

	if _, ok := fds[key1]; !ok {
		t.Fatalf("missing key %q, got %v", key1, keysOf(fds))
	}
	if _, ok := fds[key2]; !ok {
		t.Fatalf("missing key %q, got %v", key2, keysOf(fds))
	}

	// Both FDs should be valid listeners
	for k, fd := range fds {
		ln, err := net.FileListener(fd)
		if err != nil {
			t.Fatalf("FD %q not a valid listener: %v", k, err)
		}
		ln.Close()
		fd.Close()
	}

	SignalReady(sockPath, conn)

	select {
	case <-drainCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("drain not called")
	}
}

func TestUpgradeListener_ContextCancel(t *testing.T) {
	sockPath := testSockPath(t)

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpLn.Close()

	srv := newMockServer(tcpLn)
	upgrader := NewUpgrader(sockPath, srv, func() {
		t.Error("drain should not be called on cancel")
	}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- upgrader.ListenForUpgrade(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Cancel without any upgrade request
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on cancel, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenForUpgrade did not return after cancel")
	}

	// Socket should be cleaned up
	if _, err := os.Stat(sockPath); !os.IsNotExist(err) {
		t.Fatal("socket file should be removed after cancel")
	}
}

func TestUpgradeSequence_InheritedListenerAcceptsConnections(t *testing.T) {
	sockPath := testSockPath(t)

	// Old process listener
	origLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := origLn.Addr().String()

	srv := newMockServer(origLn)
	drainCalled := make(chan struct{})
	upgrader := NewUpgrader(sockPath, srv, func() { close(drainCalled) }, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go upgrader.ListenForUpgrade(ctx)
	time.Sleep(50 * time.Millisecond)

	// New process requests FDs
	fds, conn, err := RequestUpgrade(sockPath)
	if err != nil {
		t.Fatal(err)
	}

	key := "tcp:" + addr
	fd := fds[key]

	// Create a listener from the inherited FD
	inheritedLn, err := net.FileListener(fd)
	if err != nil {
		t.Fatal(err)
	}
	fd.Close()

	// Start accepting on inherited listener
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := inheritedLn.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	// Signal ready
	SignalReady(sockPath, conn)
	conn.Close()
	<-drainCalled

	// Close old listener — inherited should still work
	origLn.Close()

	// Connect to the inherited listener
	client, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal("dial inherited listener:", err)
	}
	defer client.Close()

	select {
	case srvConn := <-accepted:
		srvConn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("inherited listener did not accept connection")
	}

	inheritedLn.Close()
}

func keysOf(m map[string]*os.File) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
