package health

import (
	"context"
	"net/http"
	"testing"
	"time"

	"log/slog"
	"os"

	"github.com/fujin-io/fujin/public/server/config"
)

func TestHealthEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := NewServer(config.HealthConfig{Enabled: true, Addr: "127.0.0.1:0"}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	// Wait for server to start
	var addr string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := srv.Addr(); a != "" {
			addr = a
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("health server did not start")
	}

	// healthz should always return 200
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal("healthz request:", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// readyz should return 503 before SetReady
	resp, err = http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal("readyz request:", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready: expected 503, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Set ready
	srv.SetReady(true)

	// readyz should return 200 after SetReady
	resp, err = http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal("readyz request:", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz after ready: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	cancel()
	if err := <-errCh; err != nil {
		t.Fatal("serve error:", err)
	}
}
