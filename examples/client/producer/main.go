package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
			if err := s.Produce("pub", data); err != nil {
				log.Fatal(err)
			}
			if err := s.HProduce("pub", data, map[string]string{
				"key": "value",
			}); err != nil {
				log.Fatal(err)
			}
			fmt.Println("message sent")
			time.Sleep(1 * time.Second)
		}
	}
}
