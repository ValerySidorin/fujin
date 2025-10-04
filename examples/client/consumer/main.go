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

	"github.com/ValerySidorin/fujin/client"
)

type TestMsg struct {
	Field string `json:"field"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	defer fmt.Println("disconnected")

	conn, err := client.Dial(ctx, "localhost:4848", &tls.Config{InsecureSkipVerify: true}, nil,
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
			msgs, err := s.HFetch(ctx, "sub", 1, true)
			if err != nil {
				log.Fatal(err)
			}
			for _, msg := range msgs {
				fmt.Println("Value:", string(msg.Value), "Headers:", msg.Headers)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
