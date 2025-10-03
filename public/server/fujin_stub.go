//go:build !fujin

package server

import (
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/api/fujin/server"
	"github.com/ValerySidorin/fujin/internal/connectors"
)

// newFujinServerImpl creates a new Fujin server implementation
// This is a stub that returns nil when Fujin server is not enabled
func newFujinServerImpl(_ server.ServerConfig, _ *connectors.Manager, _ *slog.Logger) FujinServer {
	return nil
}
