package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/ValerySidorin/fujin/client"
)

type TestMsg struct {
	Field string `json:"field"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	defer fmt.Println("disconnected")

	conn, err := client.Dial(ctx, "localhost:4848", generateTLSConfig(), nil,
		client.WithTimeout(100*time.Second),
		client.WithLogger(
			slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelDebug,
			})),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("client connected")

	defer conn.Close()

	s, err := conn.Connect("")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("stream connected")

	defer s.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := s.Fetch(ctx, "sub", 1, true)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(msgs)
			time.Sleep(100 * time.Millisecond)
		}
	}
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
