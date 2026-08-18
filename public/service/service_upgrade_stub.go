//go:build !unix

package service

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/server"
)

type upgradeState struct{}

func (us *upgradeState) applyTo(_ *server.Server) {}

func requestUpgradeFromOld(_ *slog.Logger) (*upgradeState, error) { return nil, nil }
func signalOldProcessReady(_ *upgradeState, _ *slog.Logger)       {}
func startUpgradeListener(_ context.Context, _ *server.Server, _ func(), _ *slog.Logger) <-chan struct{} {
	settled := make(chan struct{})
	close(settled)
	return settled
}
