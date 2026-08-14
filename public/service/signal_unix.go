//go:build unix

package service

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fujin-io/fujin/public/server"
)

// shutdownSignals are the OS signals that trigger graceful shutdown.
// On Unix: SIGINT, SIGTERM, SIGQUIT.
var shutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// startReloadLoop listens for SIGHUP and reloads config + log level.
func startReloadLoop(s *server.Server, logLevelVar *slog.LevelVar, logger *slog.Logger) {
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		for range sighup {
			logger.Info("received SIGHUP, reloading config")
			var newConf Config
			if err := loadConfig(&newConf); err != nil {
				logger.Error("reload config failed", "err", err)
				continue
			}
			if err := s.ReloadConnectors(newConf.Connectors); err != nil {
				logger.Error("reload connectors failed", "err", err)
				continue
			}
			logLevelVar.Set(parseLogLevel(os.Getenv("FUJIN_LOG_LEVEL")))
		}
	}()
}
