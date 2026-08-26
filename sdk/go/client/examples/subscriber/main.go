package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/fujin"
)

type TestMsg struct {
	Field string `json:"field"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	defer fmt.Println("disconnected")

	conn, err := fujin.Dial(ctx, "localhost:4848", &tls.Config{InsecureSkipVerify: true}, nil,
		fujin.WithTimeout(100*time.Second),
		fujin.WithLogger(
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

	s, err := conn.Bind(ctx, "connector")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("stream connected")

	defer fmt.Println("stream closed")
	defer s.Close(context.Background())

	sub, err := s.HSubscribe(ctx, "sub", true, func(msg client.Message) {
		fmt.Println("Value:", string(msg.Payload), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("subscription 1 connected")
	defer fmt.Println("subscription 1 closed")
	defer sub.Close(context.Background())

	sub2, err := s.HSubscribe(ctx, "sub", true, func(msg client.Message) {
		fmt.Println("Value:", string(msg.Payload), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fmt.Println("subscription 2 closed")
	defer sub2.Close(context.Background())

	fmt.Println("subscribed")

	<-ctx.Done()
}
