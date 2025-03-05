package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/reader"
	"github.com/ValerySidorin/fujin/mq/reader/config"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	rConf := config.Config{
		Protocol: "kafka",
		Kafka: kafka.ReaderConfig{
			Brokers:                []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			Topic:                  "my_pub_topic",
			Group:                  "fujin",
			AllowAutoTopicCreation: true,
		},
	}

	r, err := reader.New(rConf, slog.New(slog.DiscardHandler))
	if err != nil {
		log.Fatal(err)
	}

	if err := r.Subscribe(ctx, func(message []byte, args ...any) error {
		fmt.Println(string(message))
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
