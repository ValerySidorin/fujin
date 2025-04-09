package client_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client"
	"github.com/ValerySidorin/fujin/server"
	"github.com/ValerySidorin/fujin/test"

	nats_server "github.com/nats-io/nats-server/v2/server"
)

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		_, shutdown := RunServer(ctx)
		shutdown()
	}()

	time.Sleep(1 * time.Second)

	addr := "localhost:4848"
	conn, err := client.Connect(ctx, addr, generateTLSConfig())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	time.Sleep(5 * time.Second)
}

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{"fujin"}}
}

func RunServer(ctx context.Context) (*server.Server, func()) {
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

	fs := test.RunDefaultServerWithNats(ctx)

	return fs, ns.Shutdown
}
