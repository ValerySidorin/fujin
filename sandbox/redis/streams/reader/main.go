package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/rueidis"
)

func main() {
	// Create a new Redis client
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6379"},
	})
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer client.Close()

	// Create a context for the subscription
	ctx := context.Background()

	// Subscribe to a channel
	fmt.Println("started")

	streamsIds := map[string]string{
		"stream": "0",
	}

	// Listen for messages
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Println(streamsIds)
			resp, err := client.Do(ctx,
				client.B().Xread().Count(1000).Block(5000).Streams().Key(keys(streamsIds)...).Id(values(streamsIds)...).Build(),
			).AsXRead()
			if err != nil {
				log.Println(fmt.Errorf("Error reading from stream: %w", err))
				continue
			}

			for stream, msgs := range resp {
				var msg rueidis.XRangeEntry
				for _, msg = range msgs {
					fmt.Printf("Received message: id: %s: value: %s\n", msg.ID, msg.FieldValues)
				}
				streamsIds[stream] = msg.ID
			}
		}
	}
}

func keys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func values(m map[string]string) []string {
	values := make([]string, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}
