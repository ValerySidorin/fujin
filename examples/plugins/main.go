package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ValerySidorin/fujin/public/service"

	// Import all connector plugins at once:
	// _ "github.com/ValerySidorin/fujin/public/connectors/all"

	// Or import specific connector plugins individually:
	// _ "github.com/ValerySidorin/fujin/public/connectors/impl/kafka"

	// Note: For standard plugins, you need to build with the appropriate tags.
	// Available tags: [kafka nats_core amqp091 amqp10 resp_pubsub redis_streams mqtt nsq]
	// Example: go build -tags kafka ...

	// Import your custom connector plugin:
	_ "github.com/ValerySidorin/fujin/examples/plugins/faker"
)

// This example demonstrates how to run a Fujin server with custom plugins.
// It accepts an optional config file path as an argument.
// Default config search paths: ["./config.yaml", "conf/config.yaml", "config/config.yaml"]
// Run from repo root: go run ./examples/plugins/main.go ./examples/plugins/config.yaml
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
