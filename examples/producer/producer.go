package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/writer"
	"github.com/ValerySidorin/fujin/mq/writer/config"
)

func main() {
	wConf := config.Config{
		Protocol: "kafka",
		Kafka: kafka.WriterConfig{
			Brokers:                []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			Topic:                  "my_pub_topic",
			AllowAutoTopicCreation: true,
			Linger:                 10 * time.Millisecond,
		},
	}
	p, err := writer.NewWriter(wConf, "", slog.New(slog.DiscardHandler))
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for range 1000 {
		wg.Add(1)
		p.Write(context.Background(), []byte("hello"), func(err error) {
			defer wg.Done()
			if err != nil {
				fmt.Println(err)
			}
		})
	}

	wg.Wait()
}
