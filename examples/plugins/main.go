package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ValerySidorin/fujin/public/service"

	// import all connector plugins with:
	// _ "github.com/ValerySidorin/fujin/public/connectors/all"

	// or you can import select connector plugin with:
	// _ "github.com/ValerySidorin/fujin/public/connectors/impl/kafka"
	// for example.

	// import your custom connector plugin:
	_ "github.com/ValerySidorin/fujin/examples/plugins/faker"
)

// This example binary accepts optional argument: path to config yaml file
// By default it performs search in: ["./config.yaml", "conf/config.yaml", "config/config.yaml"]
// You can run it from repo root like this: go run ./examples/plugins/main.go ./examples/plugins/config.yaml
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
