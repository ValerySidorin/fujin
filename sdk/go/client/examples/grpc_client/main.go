package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	v1 "github.com/fujin-io/fujin/sdk/go/client/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	}))

	// Create connection
	conn, err := v1.Dial("localhost:4849", logger,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 10 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             2 * time.Second,
			PermitWithoutStream: false,
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create stream
	stream, err := conn.Bind(ctx, "kafka_connector")
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close(context.Background())

	// Subscribe to topic with headers support using HSubscribe
	subscription, err := stream.HSubscribe(ctx, "client2", true, func(msg client.Message) {
		fmt.Printf("Received message:\n")
		fmt.Printf("   Subscription ID: %d\n", msg.SubscriptionID)
		fmt.Printf("   Message ID: %x\n", msg.MessageID)
		fmt.Printf("   Payload: %s\n", string(msg.Payload))
		for _, header := range msg.Headers {
			fmt.Printf("   %s: %s\n", header.Key, header.Value)
		}
	})
	if err != nil {
		log.Fatalf("Failed to hsubscribe: %v", err)
	}

	fmt.Printf("Subscribed to route 'client2' with ID %d, waiting for messages with headers...\n", subscription.ID())
	fmt.Println("Press Ctrl+C to exit")

	// Send some test messages
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		messageCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				message := fmt.Sprintf("Hello from simple producer at %s", time.Now().Format(time.RFC3339))

				// Alternate between regular Produce and HProduce with headers
				if messageCount%2 == 0 {
					if err := stream.Produce(ctx, "client1", []byte(message)); err != nil {
						log.Printf("Failed to send message: %v", err)
					} else {
						messageCount++
						fmt.Printf("✓ Sent message %d (without headers)\n", messageCount)
					}
				} else {
					headers := []client.Header{
						{Key: []byte("content-type"), Value: []byte("text/plain")},
						{Key: []byte("timestamp"), Value: []byte(time.Now().Format(time.RFC3339))},
						{Key: []byte("source"), Value: []byte("grpc-example")},
					}
					if err := stream.HProduce(ctx, "client1", []byte(message), headers); err != nil {
						log.Printf("Failed to send message with headers: %v", err)
					} else {
						messageCount++
						fmt.Printf("✓ Sent message %d (with headers)\n", messageCount)
					}
				}

				// Unsubscribe after 10 messages
				if messageCount == 1000 {
					fmt.Printf("Unsubscribing from subscription %d...\n", subscription.ID())
					if err := subscription.Close(ctx); err != nil {
						log.Printf("Failed to unsubscribe: %v", err)
					} else {
						fmt.Println("✓ Successfully unsubscribed")
					}
					return
				}
			}
		}
	}()

	// Wait for shutdown
	<-ctx.Done()
	fmt.Println("\nShutting down...")
}
