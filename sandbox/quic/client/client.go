package main

import (
	"context"
	"crypto/tls"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	session, err := quic.DialAddr(ctx, "localhost:4242", tlsConfig, nil)
	if err != nil {
		log.Fatal(err)
	}
	stream, err := session.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for {
		_, err := stream.Write([]byte("ping"))
		if err != nil {
			log.Fatal(err)
		}
		buf := make([]byte, 4)
		_, err = stream.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		if string(buf) == "pong" {
			log.Println("Received pong")
		}
		time.Sleep(1 * time.Second)
	}
}
