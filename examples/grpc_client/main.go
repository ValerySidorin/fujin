package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	v1 "github.com/ValerySidorin/fujin/client/grpc/v1"
	"github.com/ValerySidorin/fujin/client/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	conn, err := v1.NewConn("localhost:4849", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create stream
	stream, err := conn.Connect("simple-example")
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Subscribe to topic with headers support using HSubscribe
	subscriptionID, err := stream.HSubscribe("sub", true, func(msg models.Msg) {
		fmt.Printf("📨 Received message:\n")
		fmt.Printf("   Subscription ID: %d\n", msg.SubscriptionID)
		fmt.Printf("   Message ID: %x\n", msg.MessageID)
		fmt.Printf("   Payload: %s\n", string(msg.Payload))
		if len(msg.Headers) > 0 {
			fmt.Printf("   Headers:\n")
			for k, v := range msg.Headers {
				fmt.Printf("      %s: %s\n", k, v)
			}
		}
	})
	if err != nil {
		log.Fatalf("Failed to hsubscribe: %v", err)
	}

	fmt.Printf("✓ HSubscribed to topic 'sub' with ID %d, waiting for messages with headers...\n", subscriptionID)
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
					if err := stream.Produce("pub", []byte(message)); err != nil {
						log.Printf("Failed to send message: %v", err)
					} else {
						messageCount++
						fmt.Printf("✓ Sent message %d (without headers)\n", messageCount)
					}
				} else {
					headers := map[string]string{
						"content-type": "text/plain",
						"timestamp":    time.Now().Format(time.RFC3339),
						"source":       "grpc-example",
					}
					if err := stream.HProduce("pub", []byte(message), headers); err != nil {
						log.Printf("Failed to send message with headers: %v", err)
					} else {
						messageCount++
						fmt.Printf("✓ Sent message %d (with headers)\n", messageCount)
					}
				}

				// Unsubscribe after 10 messages
				if messageCount == 10 {
					fmt.Printf("Unsubscribing from subscription %d...\n", subscriptionID)
					if err := stream.Unsubscribe(subscriptionID); err != nil {
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
