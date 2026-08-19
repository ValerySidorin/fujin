package health

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin/public/server/config"
)

// Server provides HTTP health check endpoints for liveness and readiness probes.
type Server struct {
	conf       config.HealthConfig
	server     *http.Server
	ready      atomic.Bool
	listenAddr atomic.Value // stores string
	l          *slog.Logger
}

// NewServer creates a new health check HTTP server.
func NewServer(conf config.HealthConfig, l *slog.Logger) *Server {
	s := &Server{
		conf: conf,
		l:    l.With("server", "health"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	s.server = &http.Server{
		Addr:              conf.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

// ListenAndServe starts the health check HTTP server.
// It blocks until the context is cancelled, then gracefully shuts down.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("health server listen: %w", err)
	}

	addr := ln.Addr().String()
	s.listenAddr.Store(addr)
	s.l.Info("health server listening", "addr", addr)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("health server: %w", err)
	}
	return nil
}

// Addr returns the actual listening address. Empty string if not yet listening.
func (s *Server) Addr() string {
	v := s.listenAddr.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// Stop gracefully shuts down the health check server.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.server.Shutdown(ctx)
}

// SetReady sets the readiness state of the server.
func (s *Server) SetReady(v bool) {
	s.ready.Store(v)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte("not ready"))
}
