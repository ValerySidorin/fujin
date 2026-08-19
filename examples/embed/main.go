package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	cconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	nats_core "github.com/fujin-io/fujin/public/plugins/connector/nats/core"
	"github.com/fujin-io/fujin/public/plugins/transport"
	_ "github.com/fujin-io/fujin/public/plugins/transport/quic"
	"github.com/fujin-io/fujin/public/server"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"github.com/fujin-io/fujin/public/service"
	nats_server "github.com/nats-io/nats-server/v2/server"
)

var defaultEnabled = true

var DefaultGRPCServerTestConfig = serverconfig.GRPCServerConfig{
	Enabled: true,
	Addr:    ":4849",
	// TLS disabled
}

var DefaultTestConfigWithNats = serverconfig.Config{
	Transports: []transport.Config{{
		Type:    "quic",
		Enabled: &defaultEnabled,
		Settings: map[string]any{
			"addr": ":4848",
			"tls": map[string]any{
				"enabled":               true,
				"server_cert_pem_path":  "examples/assets/certs/fujin.io.pem",
				"server_key_pem_path":   "examples/assets/certs/fujin.io-key.pem",
			},
		},
	}},
	GRPC: DefaultGRPCServerTestConfig,
	Connectors: cconfig.ConnectorsConfig{
		"nats_core_connector": {
			Type: "nats_core",
			Settings: nats_core.Config{
				Common: nats_core.CommonSettings{
					URL: "nats://localhost:4222",
				},
				Routes: map[string]nats_core.RouteSettings{
					"client1": {
						Subject: "my_subject",
					},
					"client2": {
						Subject: "my_subject",
					},
				},
			},
		},
	},
}

// This example demonstrates how to embed the Fujin server into your Go application.
// It includes an embedded NATS broker as the underlying message broker.
// To run this example:
// 1. Import the nats/core plugin
// 2. Run from repo root: go run -tags grpc ./examples/embed/main.go
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), service.ShutdownSignals()...)
	defer cancel()

	s := RunServer(ctx)
	<-ctx.Done()
	<-s.Done()
}

func RunServer(ctx context.Context) *server.Server {
	opts := &nats_server.Options{}

	ns, err := nats_server.NewServer(opts)
	if err != nil {
		panic(fmt.Errorf("nats: new server: %w", err))
	}

	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		panic("nats: not ready for connections")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	s, err := server.NewServer(DefaultTestConfigWithNats, logger)
	if err != nil {
		panic(fmt.Errorf("unable to create fujin server: %w", err))
	}

	go func() {
		if err := s.ListenAndServe(ctx); err != nil {
			panic(fmt.Errorf("Unable to start fujin server: %w", err))
		}
	}()

	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start fujin server: timeout")
	}

	return s
}
