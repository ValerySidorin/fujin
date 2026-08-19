//go:build !unix

package service

import (
	"context"
	"log/slog"
	"os"
	"syscall"

	"github.com/fujin-io/fujin/public/plugins/configurator"
)

// shutdownSignals are the OS signals that trigger graceful shutdown.
// On Windows: SIGINT, SIGTERM only (no SIGQUIT).
var shutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}

// startReloadLoop is a no-op on non-Unix platforms (no SIGHUP).
func startReloadLoop(
	_ context.Context,
	_ configurator.Configurator,
	_ *connectorRuntimeController,
	_ *slog.LevelVar,
	_ *slog.Logger,
) <-chan struct{} {
	settled := make(chan struct{})
	close(settled)
	return settled
}
