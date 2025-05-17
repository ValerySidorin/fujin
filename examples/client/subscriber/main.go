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

	"github.com/ValerySidorin/fujin/client"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	defer fmt.Println("disconnected")

	conn, err := client.Connect(ctx, "localhost:4848", generateTLSConfig(), nil,
		client.WithLogger(
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

	sub, err := conn.ConnectSubscriber(
		client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic: "sub",
				// AutoCommit: true,
				Async: true,
			},
		},
		func(msg client.Msg) {
			fmt.Println(string(msg.Value))
			if err := msg.Ack(); err != nil {
				log.Fatal(err)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	fmt.Println("subscriber connected")

	<-ctx.Done()
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
