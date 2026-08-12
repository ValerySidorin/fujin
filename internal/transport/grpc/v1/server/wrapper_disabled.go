//go:build !grpc

package server

import (
	"context"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"log/slog"
	"time"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
// This is a stub implementation when gRPC is disabled
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper (stub version)
func NewGRPCServerWrapper(conf serverconfig.GRPCServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{
		server: NewGRPCServer(conf, baseConfig, l),
	}
}

// ListenAndServe starts the gRPC server (stub version)
func (w *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	return w.server.ListenAndServe(ctx)
}

// Stop gracefully stops the gRPC server (stub version)
func (w *GRPCServerWrapper) Stop() {
	w.server.Stop()
}

// ReadyForConnections reports that the unavailable gRPC server is not ready.
func (w *GRPCServerWrapper) ReadyForConnections(timeout time.Duration) bool {
	return w.server.ReadyForConnections(timeout)
}

// Done is closed after the disabled server exits.
func (w *GRPCServerWrapper) Done() <-chan struct{} {
	return w.server.Done()
}
