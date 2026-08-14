//go:build !grpc

package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
// This is a stub implementation when gRPC is disabled
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper (stub version).
func NewGRPCServerWrapper(conf serverconfig.GRPCServerConfig, catalog *connector.Catalog, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{server: NewGRPCServer(conf, catalog, l)}
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
