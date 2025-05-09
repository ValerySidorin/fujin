package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092", "localhost:9093", "localhost:9094"),
		// kgo.TransactionalID("id"),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer cl.Close()

	ctx := context.Background()

	fmt.Println("before ping")
	if err := cl.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("after ping")

	var wg sync.WaitGroup
	defer wg.Wait()

	err = cl.EndTransaction(ctx, kgo.TryCommit)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			wg.Add(1)
			record := &kgo.Record{Topic: "my_pub_topic", Value: []byte("bar")}
			fmt.Println("produce")
			cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					fmt.Printf("record had a produce error: %v\n", err)
				}
			})
			time.Sleep(1 * time.Second)
		}
	}
}
