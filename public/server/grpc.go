//go:build grpc

package server

import (
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/api/grpc/server"
	"github.com/ValerySidorin/fujin/internal/connectors"
)

// newGRPCServerImpl creates a new gRPC server implementation
func newGRPCServerImpl(addr string, cman *connectors.Manager, l *slog.Logger) GRPCServer {
	return server.NewGRPCServerWrapper(addr, cman, l)
}
