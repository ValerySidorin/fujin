package main

import (
	"context"
	"os/signal"

	"github.com/fujin-io/fujin/public/service"

	// Import all connector plugins at once:
	// _ "github.com/fujin-io/fujin/public/plugins/connector/all"

	// Or import specific connector plugins individually:
	// _ "github.com/fujin-io/fujin/public/plugins/connector/kafka/franz"

	// Note: For standard plugins, you need to build with the appropriate tags.
	// Available tags: [kafka nats_core rabbitmq_amqp09 azure_amqp1 resp_pubsub redis_streams mqtt nsq]
	// Example: go build -tags kafka ...

	// Import transport plugins:
	_ "github.com/fujin-io/fujin/public/plugins/transport/tcp"

	// Import your custom connector plugin:
	_ "github.com/fujin-io/fujin/examples/plugins/connector/faker"
	_ "github.com/fujin-io/fujin/examples/plugins/middleware/connector/ratelimit"
	_ "github.com/fujin-io/fujin/public/plugins/configurator/yaml"
)

// This example demonstrates how to run a Fujin server with custom plugins.
// It accepts an optional config file path as an argument.
// Run from repo root: FUJIN_CONFIGURATOR=yaml FUJIN_CONFIGURATOR_YAML_PATHS=./examples/plugins/config.yaml go run ./examples/plugins/main.go
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), service.ShutdownSignals()...)
	defer cancel()

	service.RunCLI(ctx)
}
