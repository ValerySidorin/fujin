package proto

import (
	"context"

	"google.golang.org/grpc"
)

// FujinService_Stream_FullMethodName is the canonical server endpoint.
const FujinService_Stream_FullMethodName = "/fujin.v1.FujinService/Stream"

// FujinServiceClient is the client API for FujinService.
type FujinServiceClient interface {
	Stream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[FujinRequest, FujinResponse], error)
}

type fujinServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewFujinServiceClient(cc grpc.ClientConnInterface) FujinServiceClient {
	return &fujinServiceClient{cc: cc}
}

func (c *fujinServiceClient) Stream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[FujinRequest, FujinResponse], error) {
	callOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &FujinService_ServiceDesc.Streams[0], FujinService_Stream_FullMethodName, callOpts...)
	if err != nil {
		return nil, err
	}
	return &grpc.GenericClientStream[FujinRequest, FujinResponse]{ClientStream: stream}, nil
}

// FujinService_StreamClient preserves the conventional generated client name.
type FujinService_StreamClient = grpc.BidiStreamingClient[FujinRequest, FujinResponse]

// FujinService_ServiceDesc supplies the stream metadata required by gRPC clients.
var FujinService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fujin.v1.FujinService",
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "fujin.proto",
}
