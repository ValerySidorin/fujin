package main

import (
	"context"
	"log"
	"sync"
	"time"

	fujinv1 "github.com/ValerySidorin/fujin/internal/api/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the gRPC server
	conn, err := grpc.Dial("localhost:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := fujinv1.NewFujinServiceClient(conn)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start bidirectional stream
	stream, err := client.Stream(ctx)
	if err != nil {
		log.Fatalf("Failed to start stream: %v", err)
	}

	// Start response receiver goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("Failed to receive response: %v", err)
				return
			}
			handleResponse(resp)
		}
	}()

	// Send Connect request first (required)
	correlationID := uint32(1)
	connectReq := &fujinv1.FujinRequest{
		Request: &fujinv1.FujinRequest_Connect{
			Connect: &fujinv1.ConnectRequest{
				CorrelationId: correlationID,
				StreamId:      "grpc-client-123", // This will be used as Kafka transactional ID
			},
		},
	}

	if err := stream.Send(connectReq); err != nil {
		log.Fatalf("Failed to send connect request: %v", err)
	}
	correlationID++

	// Wait a bit for connect response
	time.Sleep(500 * time.Millisecond)

	// Send a simple produce request
	produceReq := &fujinv1.FujinRequest{
		Request: &fujinv1.FujinRequest_Produce{
			Produce: &fujinv1.ProduceRequest{
				CorrelationId: correlationID,
				Topic:         "test-topic",
				Message:       []byte("Hello, Fujin gRPC!"),
			},
		},
	}

	if err := stream.Send(produceReq); err != nil {
		log.Fatalf("Failed to send produce request: %v", err)
	}
	correlationID++

	// Send a produce request with headers
	produceWithHeadersReq := &fujinv1.FujinRequest{
		Request: &fujinv1.FujinRequest_Produce{
			Produce: &fujinv1.ProduceRequest{
				CorrelationId: correlationID,
				Topic:         "test-topic-with-headers",
				Message:       []byte("Hello, Fujin gRPC with headers!"),
				Headers: []*fujinv1.Header{
					{Key: []byte("content-type"), Value: []byte("text/plain")},
					{Key: []byte("source"), Value: []byte("grpc-client")},
				},
			},
		},
	}

	if err := stream.Send(produceWithHeadersReq); err != nil {
		log.Fatalf("Failed to send produce with headers request: %v", err)
	}
	correlationID++

	// Send a subscribe request
	subscribeReq := &fujinv1.FujinRequest{
		Request: &fujinv1.FujinRequest_Subscribe{
			Subscribe: &fujinv1.SubscribeRequest{
				CorrelationId: correlationID,
				Topic:         "test-topic",
				AutoCommit:    true,
			},
		},
	}

	if err := stream.Send(subscribeReq); err != nil {
		log.Fatalf("Failed to send subscribe request: %v", err)
	}

	// Wait a bit for responses
	time.Sleep(2 * time.Second)

	// Close the stream
	if err := stream.CloseSend(); err != nil {
		log.Printf("Failed to close send: %v", err)
	}

	// Wait for response receiver to finish
	wg.Wait()
}

func handleResponse(resp *fujinv1.FujinResponse) {
	switch r := resp.Response.(type) {
	case *fujinv1.FujinResponse_Connect:
		connectResp := r.Connect
		if connectResp.Error != "" {
			log.Printf("Connect error (correlation_id=%d): %s", connectResp.CorrelationId, connectResp.Error)
		} else {
			log.Printf("Connect success (correlation_id=%d)", connectResp.CorrelationId)
		}

	case *fujinv1.FujinResponse_Produce:
		produceResp := r.Produce
		if produceResp.Error != "" {
			log.Printf("Produce error (correlation_id=%d): %s", produceResp.CorrelationId, produceResp.Error)
		} else {
			log.Printf("Produce success (correlation_id=%d)", produceResp.CorrelationId)
		}

	case *fujinv1.FujinResponse_Message:
		msg := r.Message
		log.Printf("Received message (correlation_id=%d): topic=%s, payload=%s",
			msg.CorrelationId, msg.Topic, string(msg.Payload))

	case *fujinv1.FujinResponse_Ack:
		ack := r.Ack
		if ack.Error != "" {
			log.Printf("Ack error (correlation_id=%d): %s", ack.CorrelationId, ack.Error)
		} else {
			log.Printf("Ack success (correlation_id=%d)", ack.CorrelationId)
		}

	case *fujinv1.FujinResponse_Nack:
		nack := r.Nack
		if nack.Error != "" {
			log.Printf("Nack error (correlation_id=%d): %s", nack.CorrelationId, nack.Error)
		} else {
			log.Printf("Nack success (correlation_id=%d)", nack.CorrelationId)
		}

	default:
		log.Printf("Unknown response type")
	}
}
