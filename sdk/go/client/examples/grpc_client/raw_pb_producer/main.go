package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	pb "github.com/fujin-io/fujin/sdk/go/client/grpc/v1/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	conn, err := grpc.NewClient("localhost:4849", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	stream, err := pb.NewFujinServiceClient(conn).Stream(ctx)
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer stream.CloseSend()

	if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: "connector"}}}); err != nil {
		log.Fatalf("send bind: %v", err)
	}
	bind := mustReceive(stream).GetBind()
	if bind == nil {
		log.Fatal("unexpected bind response")
	}
	if bind.Error != nil {
		log.Fatalf("bind: %s", bind.Error.Message)
	}

	var correlationID uint32 = 1
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{
				CorrelationId: correlationID,
				Route:         "pub",
				Message:       fmt.Appendf(nil, "Hello from gRPC at %s", now.Format(time.RFC3339)),
			}}}); err != nil {
				log.Fatalf("send produce: %v", err)
			}
			response := mustReceive(stream).GetProduce()
			if response == nil {
				log.Fatal("unexpected produce response")
			}
			if response.Error != nil {
				log.Fatalf("produce: %s", response.Error.Message)
			}
			fmt.Printf("produced correlation_id=%d\n", response.CorrelationId)
			correlationID++
		}
	}
}

func mustReceive(stream pb.FujinService_StreamClient) *pb.FujinResponse {
	response, err := stream.Recv()
	if err != nil {
		log.Fatalf("receive: %v", err)
	}
	return response
}
