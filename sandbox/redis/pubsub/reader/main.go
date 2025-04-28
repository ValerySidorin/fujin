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
	channel := client.B().Subscribe().Channel("channel").Build()

	fmt.Println("started")

	// Listen for messages
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := client.Receive(ctx, channel, func(msg rueidis.PubSubMessage) {
				fmt.Println(msg.Message)
			}); err != nil {
				log.Fatal(err)
			}
		}
	}
}
