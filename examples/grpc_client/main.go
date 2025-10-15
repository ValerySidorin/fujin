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
	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	}))

	// Create connection configuration
	connConfig := &v1.ConnConfig{
		Address:              "localhost:4849",
		TLSEnabled:           false,
		InsecureSkipVerify:   true,
		KeepAlive:            30 * time.Second,
		KeepAliveTimeout:     5 * time.Second,
		MaxRetries:           3,
		RetryDelay:           1 * time.Second,
		ConnectionTimeout:    10 * time.Second,
		MaxConcurrentStreams: 100,
	}

	// Create connection
	conn, err := v1.NewConn(connConfig, logger)
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

	// Subscribe to topic
	subscriptionID, err := stream.Subscribe("sub", true, func(msg *pb.FujinResponse_Message) {
		fmt.Printf("📨 Received message:\n")
		fmt.Printf("   Subscription ID: %d\n", msg.Message.CorrelationId)
		fmt.Printf("   Topic: %s\n", msg.Message.Topic)
		fmt.Printf("   Payload: %s\n", string(msg.Message.Payload))

		if len(msg.Message.Headers) > 0 {
			fmt.Printf("   Headers:\n")
			for _, h := range msg.Message.Headers {
				fmt.Printf("     %s: %s\n", string(h.Key), string(h.Value))
			}
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	fmt.Printf("✓ Subscribed to topic 'sub' with ID %d, waiting for messages...\n", subscriptionID)
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

				if err := stream.Produce("pub", []byte(message)); err != nil {
					log.Printf("Failed to send message: %v", err)
				} else {
					messageCount++
					fmt.Printf("✓ Sent message %d\n", messageCount)
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
