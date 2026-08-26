package v1_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	client "github.com/fujin-io/fujin/sdk/go/client"
	v1 "github.com/fujin-io/fujin/sdk/go/client/grpc/v1"
	pb "github.com/fujin-io/fujin/sdk/go/client/grpc/v1/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var benchmarkPayloadSizes = []int{1, 128, 1024, 32 * 1024, 1024 * 1024}
var benchmarkConcurrency = []int{1, 16, 128}

type benchmarkFujinService interface {
	Stream(grpc.ServerStream) error
}

type benchmarkServer struct{}

func (*benchmarkServer) Stream(stream grpc.ServerStream) error {
	for {
		request := new(pb.FujinRequest)
		if err := stream.RecvMsg(request); err != nil {
			return err
		}
		var response *pb.FujinResponse
		switch value := request.GetRequest().(type) {
		case *pb.FujinRequest_Bind:
			response = &pb.FujinResponse{Response: &pb.FujinResponse_Bind{Bind: &pb.BindResponse{Routes: map[string]*pb.RouteCapabilities{
				"pub": {Produce: true, Headers: true, ProduceGuarantee: pb.ProduceGuarantee_PRODUCE_GUARANTEE_LOCAL_ACCEPT},
			}}}}
		case *pb.FujinRequest_Produce:
			response = &pb.FujinResponse{Response: &pb.FujinResponse_Produce{Produce: &pb.ProduceResponse{CorrelationId: value.Produce.CorrelationId}}}
		case *pb.FujinRequest_Hproduce:
			response = &pb.FujinResponse{Response: &pb.FujinResponse_Hproduce{Hproduce: &pb.HProduceResponse{CorrelationId: value.Hproduce.CorrelationId}}}
		default:
			return fmt.Errorf("unsupported benchmark request %T", value)
		}
		if err := stream.SendMsg(response); err != nil {
			return err
		}
	}
}

func BenchmarkGRPCProduce(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	grpcServer.RegisterService(&grpc.ServiceDesc{
		ServiceName: "fujin.v1.FujinService",
		HandlerType: (*benchmarkFujinService)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Stream",
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(server any, stream grpc.ServerStream) error {
				return server.(benchmarkFujinService).Stream(stream)
			},
		}},
	}, &benchmarkServer{})
	go func() { _ = grpcServer.Serve(listener) }()
	b.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := v1.Dial(listener.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	stream, err := conn.Bind(context.Background(), "connector")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = stream.Close(context.Background()) })

	for _, size := range benchmarkPayloadSizes {
		payload := make([]byte, size)
		for _, concurrency := range benchmarkConcurrency {
			name := fmt.Sprintf("payload=%dB/concurrency=%d", size, concurrency)
			b.Run("produce/"+name, func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				runConcurrentBenchmark(b, concurrency, func() error {
					return stream.Produce(context.Background(), "pub", payload)
				})
			})
			b.Run("hproduce/"+name, func(b *testing.B) {
				headers := benchmarkHeaders()
				b.SetBytes(int64(size))
				b.ReportAllocs()
				runConcurrentBenchmark(b, concurrency, func() error {
					return stream.HProduce(context.Background(), "pub", payload, headers)
				})
			})
		}
	}
}

func benchmarkHeaders() []client.Header {
	return []client.Header{
		{Key: []byte("content-type"), Value: []byte("application/octet-stream")},
		{Key: []byte("tenant"), Value: []byte("performance")},
		{Key: []byte("trace-id"), Value: []byte("0123456789abcdef")},
		{Key: []byte("source"), Value: []byte("benchmark")},
	}
}

func runConcurrentBenchmark(b *testing.B, concurrency int, operation func() error) {
	b.Helper()
	var next atomic.Uint64
	var workers sync.WaitGroup
	errCh := make(chan error, 1)
	workers.Add(concurrency)
	b.ResetTimer()
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				operationIndex := next.Add(1) - 1
				if operationIndex >= uint64(b.N) {
					return
				}
				if err := operation(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	workers.Wait()
	b.StopTimer()
	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}
}
