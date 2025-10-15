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

	"github.com/ValerySidorin/fujin/client/fujin"
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

	s, err := conn.Connect("")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("stream connected")

	defer fmt.Println("stream closed")
	defer s.Close()

	sub, err := s.HSubscribe("sub", true, func(msg fujin.Msg) {
		fmt.Println("Value:", string(msg.Value), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fmt.Println("subscription closed")
	defer sub.Close()

	sub2, err := s.HSubscribe("sub", true, func(msg fujin.Msg) {
		fmt.Println("Value:", string(msg.Value), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fmt.Println("subscription 2 closed")
	defer sub2.Close()

	fmt.Println("subscribed")

	<-ctx.Done()
}
