//go:build fujin

package server

import (
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/api/fujin/server"
	"github.com/ValerySidorin/fujin/internal/connectors"
)

// newFujinServerImpl creates a new Fujin server implementation
func newFujinServerImpl(conf server.ServerConfig, cman *connectors.Manager, l *slog.Logger) FujinServer {
	return server.NewServer(conf, cman, l)
}
