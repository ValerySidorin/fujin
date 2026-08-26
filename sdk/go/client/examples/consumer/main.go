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

	defer s.Close(context.Background())

	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := s.Fetch(ctx, "sub", true, 1)
			if err != nil {
				log.Fatal(err)
			}
			for _, msg := range result.Messages {
				fmt.Println("Value:", string(msg.Payload), "Headers:", msg.Headers)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
