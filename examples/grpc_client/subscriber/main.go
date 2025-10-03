package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

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

	// Send CONNECT request
	if err := stream.Send(&pb.FujinRequest{
		Request: &pb.FujinRequest_Connect{
			Connect: &pb.ConnectRequest{
				CorrelationId: 1,
				StreamId:      "subscriber-1",
			},
		},
	}); err != nil {
		log.Fatalf("Failed to send connect: %v", err)
	}

	// Send SUBSCRIBE request
	if err := stream.Send(&pb.FujinRequest{
		Request: &pb.FujinRequest_Subscribe{
			Subscribe: &pb.SubscribeRequest{
				CorrelationId: 2,
				Topic:         "sub",
				AutoCommit:    true,
				Headers: []*pb.Header{
					{Key: []byte("with_headers"), Value: []byte("true")},
				},
			},
		},
	}); err != nil {
		log.Fatalf("Failed to send subscribe: %v", err)
	}

	fmt.Println("✓ Subscribed to topic 'sub', waiting for messages...")
	fmt.Println("Press Ctrl+C to exit")

	// Receive messages
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, graceful shutdown
				fmt.Println("\nShutting down...")
				return
			}
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

		case *pb.FujinResponse_Subscribe:
			if r.Subscribe.Error != "" {
				log.Printf("Subscribe error: %s", r.Subscribe.Error)
			} else {
				fmt.Printf("✓ Subscribed successfully (subscription_id=%d)\n", r.Subscribe.SubscriptionId)
			}

		case *pb.FujinResponse_Message:
			fmt.Printf("\n📨 Received message:\n")
			fmt.Printf("   Subscription ID: %d\n", r.Message.CorrelationId)
			fmt.Printf("   Topic: %s\n", r.Message.Topic)
			fmt.Printf("   Payload: %s\n", string(r.Message.Payload))

			if len(r.Message.Headers) > 0 {
				fmt.Printf("   Headers:\n")
				for _, h := range r.Message.Headers {
					fmt.Printf("     %s: %s\n", string(h.Key), string(h.Value))
				}
			}

			if len(r.Message.DeliveryId) > 0 {
				fmt.Printf("   Delivery ID: %x\n", r.Message.DeliveryId)
			}

		case *pb.FujinResponse_Ack:
			if r.Ack.Error != "" {
				log.Printf("Ack error: %s", r.Ack.Error)
			} else {
				fmt.Printf("✓ Message acknowledged (correlation_id=%d)\n", r.Ack.CorrelationId)
			}

		default:
			fmt.Printf("Received unknown response type: %T\n", r)
		}
	}
}
