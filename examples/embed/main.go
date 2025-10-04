package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	fujin_server "github.com/ValerySidorin/fujin/internal/api/fujin/server"
	"github.com/ValerySidorin/fujin/public/connectors"
	"github.com/ValerySidorin/fujin/public/server"
	"github.com/ValerySidorin/fujin/public/server/config"
	nats_server "github.com/nats-io/nats-server/v2/server"

	reader_config "github.com/ValerySidorin/fujin/public/connectors/reader/config"
	writer_config "github.com/ValerySidorin/fujin/public/connectors/writer/config"

	_ "github.com/ValerySidorin/fujin/public/connectors/impl/nats/core"
	nats_core "github.com/ValerySidorin/fujin/public/connectors/impl/nats/core"
)

var DefaultFujinServerTestConfig = fujin_server.FujinServerConfig{
	Addr: ":4848",
	TLS:  generateTLSConfig(),
}

var DefaultTestConfigWithNats = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connectors.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nats_core",
				Settings: nats_core.ReaderConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nats_core",
				Settings: nats_core.WriterConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
				},
			},
		},
	},
}

// This example demonstrates how to embed the Fujin server into your Go application.
// It includes an embedded NATS broker as the underlying message broker.
// To run this example:
// 1. Import the nats/core plugin
// 2. Build with the "nats_core" tag
// 3. Run from repo root: go run -tags nats_core ./examples/embed/main.go
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
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

	s, _ := server.NewServer(DefaultTestConfigWithNats, logger)

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

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{"fujin/1"}}
}
