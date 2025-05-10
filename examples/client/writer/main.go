package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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

	conn, err := client.Connect(ctx, "localhost:4848", generateTLSConfig(), client.WithLogger(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		})),
	))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("client connected")

	defer conn.Close()

	w, err := conn.ConnectWriter("")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("writer connected")

	defer w.Close()

	msg := TestMsg{
		Field: "test",
	}

	data, err := json.Marshal(&msg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:

			if err := w.Write("pub", data); err != nil {
				log.Fatal(err)
			}
			fmt.Println("message sent")
			time.Sleep(1 * time.Second)
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
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{"fujin"}}
}
