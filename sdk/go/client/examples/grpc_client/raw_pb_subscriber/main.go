package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

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
	if bind == nil || bind.Error != nil {
		log.Fatalf("bind failed: %v", bind)
	}
	if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Subscribe{Subscribe: &pb.SubscribeRequest{
		CorrelationId: 1,
		Route:         "sub",
		AutoCommit:    true,
	}}}); err != nil {
		log.Fatalf("send subscribe: %v", err)
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatalf("receive: %v", err)
		}
		switch value := response.GetResponse().(type) {
		case *pb.FujinResponse_Subscribe:
			if value.Subscribe.Error != nil {
				log.Fatalf("subscribe: %s", value.Subscribe.Error.Message)
			}
			fmt.Printf("subscribed subscription_id=%d\n", value.Subscribe.SubscriptionId)
		case *pb.FujinResponse_Message:
			fmt.Printf("subscription_id=%d payload=%s\n", value.Message.SubscriptionId, value.Message.Payload)
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
