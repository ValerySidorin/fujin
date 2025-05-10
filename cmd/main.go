package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ValerySidorin/fujin/public/service"
	_ "go.uber.org/automaxprocs"

	_ "github.com/ValerySidorin/fujin/public/connectors/all"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
