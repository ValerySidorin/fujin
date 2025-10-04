package server

import (
	"context"
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/connectors"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper
func NewGRPCServerWrapper(conf GRPCServerConfig, cman *connectors.Manager, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{
		server: NewGRPCServer(conf, cman, l),
	}
}

// ListenAndServe starts the gRPC server
func (w *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	return w.server.ListenAndServe(ctx)
}

// Stop gracefully stops the gRPC server
func (w *GRPCServerWrapper) Stop() {
	w.server.Stop()
}
