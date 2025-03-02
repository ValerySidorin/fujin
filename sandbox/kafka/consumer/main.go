package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092", "localhost:9093", "localhost:9094"),
		kgo.ConsumeTopics("my_pub_topic"),
		kgo.ConsumerGroup("fujin1"),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("before ping")
	if err := client.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("after ping")

	fmt.Println("started consuming")

	n := atomic.Int32{}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := client.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				err = fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
				log.Fatal(err)
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				n.Add(1)
				record := iter.Next()
				fmt.Println(time.Now(), string(record.Value), n.Load())
			}
		}
	}
}
