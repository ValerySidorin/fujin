package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	pb "github.com/ValerySidorin/fujin/internal/api/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Connect to gRPC server
	conn, err := grpc.NewClient("localhost:4849",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFujinServiceClient(conn)

	// Open bidirectional stream
	stream, err := client.Stream(ctx)
	if err != nil {
		log.Fatalf("Failed to open stream: %v", err)
	}

	// Start response reader goroutine
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("Receive error: %v", err)
				return
			}

			switch r := resp.Response.(type) {
			case *pb.FujinResponse_Connect:
				if r.Connect.Error != "" {
					log.Printf("Connect error: %s", r.Connect.Error)
				} else {
					fmt.Println("✓ Connected to Fujin gRPC server")
				}
			case *pb.FujinResponse_Produce:
				if r.Produce.Error != "" {
					log.Printf("Produce error (correlation_id=%d): %s",
						r.Produce.CorrelationId, r.Produce.Error)
				} else {
					fmt.Printf("✓ Message produced (correlation_id=%d)\n",
						r.Produce.CorrelationId)
				}
			}
		}
	}()

	// Send CONNECT request
	if err := stream.Send(&pb.FujinRequest{
		Request: &pb.FujinRequest_Connect{
			Connect: &pb.ConnectRequest{
				CorrelationId: 1,
				StreamId:      "producer-1",
			},
		},
	}); err != nil {
		log.Fatalf("Failed to send connect: %v", err)
	}

	// Wait a bit for connection
	time.Sleep(100 * time.Millisecond)

	// Send messages
	var correlationID uint32 = 2
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nShutting down...")
			return
		case <-ticker.C:
			// Send message without headers
			if err := stream.Send(&pb.FujinRequest{
				Request: &pb.FujinRequest_Produce{
					Produce: &pb.ProduceRequest{
						CorrelationId: correlationID,
						Topic:         "pub",
						Message:       fmt.Appendf(nil, "Hello from gRPC at %s", time.Now().Format(time.RFC3339)),
					},
				},
			}); err != nil {
				log.Printf("Failed to send message: %v", err)
				return
			}
			correlationID++
		}
	}
}
