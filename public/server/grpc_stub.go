//go:build !grpc

package server

import (
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/connectors"
)

// newGRPCServerImpl creates a new gRPC server implementation
// This is a stub that returns nil when gRPC is not enabled
func newGRPCServerImpl(addr string, cman *connectors.Manager, l *slog.Logger) GRPCServer {
	return nil
}
