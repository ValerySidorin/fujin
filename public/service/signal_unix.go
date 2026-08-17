//go:build unix

package service

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fujin-io/fujin/public/plugins/configurator"
)

// shutdownSignals are the OS signals that trigger graceful shutdown.
// On Unix: SIGINT, SIGTERM, SIGQUIT.
var shutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// startReloadLoop listens for SIGHUP and reloads process-local settings. A live
// ConnectorWatcher exclusively owns connector state; startup-only configurators
// reload connectors through the same serialized runtime controller.
func startReloadLoop(
	ctx context.Context,
	loader configurator.Configurator,
	controller *connectorRuntimeController,
	logLevelVar *slog.LevelVar,
	logger *slog.Logger,
) {
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	localRevision := controller.ActiveRevision()
	go func() {
		defer signal.Stop(sighup)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sighup:
				logger.Info("received SIGHUP, reloading configuration")
				logLevelVar.Set(parseLogLevel(os.Getenv("FUJIN_LOG_LEVEL")))
				if err := reloadConnectorsFromConfigurator(ctx, loader, controller, &localRevision); err != nil {
					logger.Error("reload connectors failed", "revision", localRevision, "err", err)
				}
			}
		}
	}()
}
