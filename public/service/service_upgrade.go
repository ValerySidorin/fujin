//go:build unix

package service

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/fujin-io/fujin/internal/upgrade"
	"github.com/fujin-io/fujin/public/server"
)

const defaultUpgradeSock = "/run/fujin/upgrade.sock"

// upgradeState holds state for the upgrade handshake.
type upgradeState struct {
	inheritedFDs map[string]*os.File
	controlConn  *net.UnixConn
}

func (us *upgradeState) applyTo(s *server.Server) {
	if us != nil {
		s.SetInheritedFDs(us.inheritedFDs)
	}
}

// requestUpgradeFromOld connects to the old process and requests listener FDs.
// Returns nil state if not in upgrade mode (FUJIN_UPGRADE != "1").
func requestUpgradeFromOld(l *slog.Logger) (*upgradeState, error) {
	if os.Getenv("FUJIN_UPGRADE") != "1" {
		return nil, nil
	}

	sockPath := upgradeSockPath()
	l.Info("upgrade mode: requesting FDs from old process", "sock", sockPath)

	fds, conn, err := upgrade.RequestUpgrade(sockPath)
	if err != nil {
		return nil, err
	}

	l.Info("upgrade mode: received inherited FDs", "count", len(fds))
	return &upgradeState{inheritedFDs: fds, controlConn: conn}, nil
}

// signalOldProcessReady tells the old process we're ready to accept connections.
func signalOldProcessReady(us *upgradeState, l *slog.Logger) {
	if us == nil {
		return
	}
	defer us.controlConn.Close()

	sockPath := upgradeSockPath()
	if err := upgrade.SignalReady(sockPath, us.controlConn); err != nil {
		l.Error("upgrade mode: failed to signal ready", "err", err)
		return
	}
	l.Info("upgrade mode: signaled ready to old process")
}

// startUpgradeListener starts the control socket for future upgrades.
func startUpgradeListener(ctx context.Context, s *server.Server, drainFn func(), l *slog.Logger) <-chan struct{} {
	settled := make(chan struct{})
	sockPath := upgradeSockPath()
	upgrader := upgrade.NewUpgrader(sockPath, s, drainFn, l)
	go func() {
		defer close(settled)
		if err := upgrader.ListenForUpgrade(ctx); err != nil {
			l.Error("upgrade listener", "err", err)
		}
	}()
	return settled
}

func upgradeSockPath() string {
	if p := os.Getenv("FUJIN_UPGRADE_SOCK"); p != "" {
		return p
	}
	return defaultUpgradeSock
}
