package main

import (
	"context"
	"log"
	"time"

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

	// Define the channel and message
	channel := "example-channel"
	message := "Hello, Redis!"

	// Publish messages in a loop
	for {
		err := client.Do(context.Background(), client.B().Publish().Channel(channel).Message(message).Build()).Error()
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
		} else {
			log.Printf("Published message: %s to channel: %s", message, channel)
		}

		// Wait for a second before publishing the next message
		time.Sleep(1 * time.Millisecond)
	}
}
