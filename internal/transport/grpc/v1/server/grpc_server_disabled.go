//go:build !grpc

package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

var ErrGRPCNotCompiledIn = fmt.Errorf("grpc is not compiled in")

// GRPCServer stub implementation when gRPC is disabled
type GRPCServer struct {
	conf    serverconfig.GRPCServerConfig
	enabled bool
	ready   chan struct{}
	done    chan struct{}
	l       *slog.Logger
}

// NewGRPCServer creates a stub gRPC server instance.
func NewGRPCServer(conf serverconfig.GRPCServerConfig, _ *connector.Catalog, l *slog.Logger) *GRPCServer {
	return &GRPCServer{
		conf:    conf,
		enabled: conf.Enabled,
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
		l:       l.With("server", "grpc"),
	}
}

// ListenAndServe returns an error indicating gRPC is not compiled in
func (s *GRPCServer) ListenAndServe(ctx context.Context) error {
	defer close(s.done)
	if s.enabled {
		s.l.Error("gRPC server is enabled but not compiled in - rebuild with 'grpc' build tag")
		return ErrGRPCNotCompiledIn
	}
	// If not enabled, just wait for context cancellation
	<-ctx.Done()
	return nil
}

// Stop does nothing in stub implementation
func (s *GRPCServer) Stop() {
	// no-op
}

func (s *GRPCServer) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	case <-s.done:
		return false
	}
}

func (s *GRPCServer) Done() <-chan struct{} {
	return s.done
}
