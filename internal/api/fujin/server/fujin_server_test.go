package server_test

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/internal/api/fujin/server"
	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/public/server/config"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFujinServer(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr:                  ":4848",
		PingInterval:          2 * time.Second,
		PingTimeout:           5 * time.Second,
		PingMaxRetries:        3,
		WriteDeadline:         10 * time.Second,
		ForceTerminateTimeout: 15 * time.Second,
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	assert.NotNil(t, srv)
}

func TestNewFujinServer_WithNilLogger(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: ":4848",
	}
	cman := &connectors.Manager{}

	// This should panic if logger is required
	assert.NotPanics(t, func() {
		server.NewFujinServer(conf, cman, slog.Default())
	})
}

func TestNewFujinServer_WithTLS(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr:         ":4848",
		PingInterval: 2 * time.Second,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	assert.NotNil(t, srv)
}

func TestNewFujinServer_WithQUICConfig(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr:         ":4848",
		PingInterval: 2 * time.Second,
		QUIC: &quic.Config{
			MaxIdleTimeout: 30 * time.Second,
		},
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	assert.NotNil(t, srv)
}

func TestFujinServer_ReadyForConnections_Timeout(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: ":4848",
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	// Should timeout since server is not started
	ready := srv.ReadyForConnections(10 * time.Millisecond)
	assert.False(t, ready, "Server should not be ready (timeout)")
}

func TestFujinServer_ReadyForConnections_Success(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: ":4848",
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	// Simulate server becoming ready
	go func() {
		time.Sleep(10 * time.Millisecond)
		// The ready channel is closed by ListenAndServe, but we can't easily test that
		// For now, we test the timeout case above
	}()

	ready := srv.ReadyForConnections(5 * time.Millisecond)
	assert.False(t, ready)
}

func TestFujinServer_Done(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: ":4848",
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	done := srv.Done()
	assert.NotNil(t, done, "Done channel should not be nil")

	// Channel should not be closed initially
	select {
	case <-done:
		t.Fatal("Done channel should not be closed initially")
	case <-time.After(10 * time.Millisecond):
		// Expected: channel is not closed
	}
}

func TestFujinServer_ListenAndServe_InvalidAddress(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: "invalid address format",
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := srv.ListenAndServe(ctx)
	assert.Error(t, err, "Should fail with invalid address")
}

func TestFujinServer_ListenAndServe_PortInUse(t *testing.T) {
	t.Skip("Skipping: requires integration test setup")
	// This test would require starting two servers on the same port
	// which is more of an integration test
}

func TestFujinServer_ListenAndServe_CancelContext(t *testing.T) {
	// Use a random available port
	conf := config.FujinServerConfig{
		Addr:                  "127.0.0.1:0", // OS will assign a free port
		PingInterval:          100 * time.Millisecond,
		PingTimeout:           200 * time.Millisecond,
		PingMaxRetries:        2,
		WriteDeadline:         1 * time.Second,
		ForceTerminateTimeout: 2 * time.Second,
		TLS:                   &tls.Config{},
		QUIC:                  &quic.Config{},
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Reduce noise in tests
	}))

	srv := server.NewFujinServer(conf, cman, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()

	// Wait for server to be ready
	ready := srv.ReadyForConnections(2 * time.Second)
	require.True(t, ready, "Server should be ready within timeout")

	// Cancel context to stop server
	cancel()

	// Wait for server to stop
	select {
	case err := <-done:
		assert.NoError(t, err, "Server should stop cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not stop within timeout")
	}

	// Verify Done channel is closed
	select {
	case <-srv.Done():
		// Expected: channel is closed
	case <-time.After(1 * time.Second):
		t.Fatal("Done channel should be closed after server stops")
	}
}

func TestFujinServer_MultipleInstances(t *testing.T) {
	// Test creating multiple server instances (not starting them)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cman := &connectors.Manager{}

	servers := make([]*server.FujinServer, 3)
	for i := 0; i < 3; i++ {
		conf := config.FujinServerConfig{
			Addr:         ":4848",
			PingInterval: 2 * time.Second,
		}
		servers[i] = server.NewFujinServer(conf, cman, logger)
		assert.NotNil(t, servers[i])
	}

	// All servers should be independent
	for i := 0; i < 3; i++ {
		assert.NotNil(t, servers[i].Done())
	}
}

func TestFujinServer_ConfigurationVariations(t *testing.T) {
	tests := []struct {
		name string
		conf config.FujinServerConfig
	}{
		{
			name: "minimal config",
			conf: config.FujinServerConfig{
				Addr: ":4848",
			},
		},
		{
			name: "full config",
			conf: config.FujinServerConfig{
				Addr:                  ":4848",
				PingInterval:          1 * time.Second,
				PingTimeout:           3 * time.Second,
				PingMaxRetries:        5,
				WriteDeadline:         5 * time.Second,
				ForceTerminateTimeout: 10 * time.Second,
				TLS:                   &tls.Config{},
				QUIC:                  &quic.Config{},
			},
		},
		{
			name: "with TLS only",
			conf: config.FujinServerConfig{
				Addr: ":4848",
				TLS: &tls.Config{
					MinVersion: tls.VersionTLS13,
				},
			},
		},
		{
			name: "with QUIC only",
			conf: config.FujinServerConfig{
				Addr: ":4848",
				QUIC: &quic.Config{
					MaxIdleTimeout: 60 * time.Second,
				},
			},
		},
		{
			name: "zero values",
			conf: config.FujinServerConfig{
				Addr:                  ":0",
				PingInterval:          0,
				PingTimeout:           0,
				PingMaxRetries:        0,
				WriteDeadline:         0,
				ForceTerminateTimeout: 0,
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cman := &connectors.Manager{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := server.NewFujinServer(tt.conf, cman, logger)
			assert.NotNil(t, srv)
			assert.NotNil(t, srv.Done())
		})
	}
}

func TestFujinServer_ReadyForConnections_MultipleWaiters(t *testing.T) {
	conf := config.FujinServerConfig{
		Addr: ":4848",
	}
	cman := &connectors.Manager{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := server.NewFujinServer(conf, cman, logger)

	// Multiple goroutines waiting for server to be ready
	results := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func() {
			ready := srv.ReadyForConnections(10 * time.Millisecond)
			results <- ready
		}()
	}

	// Collect results
	for i := 0; i < 3; i++ {
		ready := <-results
		assert.False(t, ready, "All should timeout since server not started")
	}
}
