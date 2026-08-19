package main

import (
	"context"
	"os/signal"

	_ "github.com/fujin-io/fujin/public/plugins/configurator/all"
	_ "github.com/fujin-io/fujin/public/plugins/connector/all"
	_ "github.com/fujin-io/fujin/public/plugins/middleware/bind/all"
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/all"
	_ "github.com/fujin-io/fujin/public/plugins/transport/all"
	"github.com/fujin-io/fujin/public/service"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), service.ShutdownSignals()...)
	defer cancel()

	service.RunCLI(ctx)
}
