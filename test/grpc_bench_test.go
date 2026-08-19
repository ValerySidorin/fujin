//go:build grpc

package test

import (
	"context"
	"fmt"
	"testing"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	pb "github.com/fujin-io/fujin/public/proto/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const perfGRPCAddr = "localhost:4849"

const grpcBenchmarkMaxInFlight = 1024

var benchmarkPayloadSizes = []struct {
	name string
	size int
}{
	{name: "1B", size: 1},
	{name: "128B", size: 128},
	{name: "1KiB", size: 1024},
	{name: "32KiB", size: 32 * 1024},
	{name: "1MiB", size: 1024 * 1024},
}

func Benchmark_Produce_Nop_GRPC(b *testing.B) {
	for _, payloadSize := range benchmarkPayloadSizes {
		b.Run(payloadSize.name, func(b *testing.B) {
			benchProduceGRPC(b, sizedBytes(payloadSize.size))
		})
	}
}

func Benchmark_Fetch_Nop_GRPC(b *testing.B) {
	ctx, cleanup, stream := newGRPCBenchStream(b, connectorconfig.ConnectorsConfig{
		"connector": {Type: "nop"},
	})
	defer cleanup()

	bindGRPCBenchStream(b, stream)
	req := &pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: true,
		BatchSize: 1}}}
	done := receiveGRPCResponses(ctx, stream, b.N, func(resp *pb.FujinResponse) error {
		fetch, ok := resp.Response.(*pb.FujinResponse_Fetch)
		if !ok {
			return fmt.Errorf("unexpected response type %T", resp.Response)
		}
		if fetch.Fetch.Error != nil {
			return fmt.Errorf("fetch: %v", fetch.Fetch.Error)
		}
		return nil
	})

	b.ResetTimer()
	for b.Loop() {
		if err := stream.Send(req); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func Benchmark_Subscribe_Gen_GRPC(b *testing.B) {
	for _, payloadSize := range benchmarkPayloadSizes {
		b.Run(payloadSize.name, func(b *testing.B) {
			benchSubscribeGRPC(b, payloadSize.size)
		})
	}
}

func Benchmark_HProduce_Session_GRPC(b *testing.B) {
	for _, payloadSize := range benchmarkPayloadSizes {
		b.Run(payloadSize.name, func(b *testing.B) {
			ctx, cleanup, stream := newGRPCBenchStream(b, MakeSessionBenchConfig(payloadSize.size))
			defer cleanup()
			bindGRPCBenchStream(b, stream)
			req := &pb.FujinRequest{Request: &pb.FujinRequest_Hproduce{Hproduce: &pb.HProduceRequest{CorrelationId: 1, Route: "pub", Message: sizedBytes(payloadSize.size),
				Headers: []*pb.KV{{Key: []byte("content-type"), Value: []byte("application/octet-stream")}}}}}
			done := receiveGRPCResponses(ctx, stream, b.N, func(resp *pb.FujinResponse) error {
				value, ok := resp.Response.(*pb.FujinResponse_Hproduce)
				if !ok || value.Hproduce.Error != nil {
					return fmt.Errorf("hproduce response: %T %v", resp.Response, value.Hproduce.Error)
				}
				return nil
			})
			b.SetBytes(int64(payloadSize.size))
			b.ResetTimer()
			for b.Loop() {
				if err := stream.Send(req); err != nil {
					b.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		})
	}
}

func Benchmark_Fetch_Session_GRPC(b *testing.B) {
	benchmarkFetchGRPC(b, false)
}

func Benchmark_HFetch_Session_GRPC(b *testing.B) {
	benchmarkFetchGRPC(b, true)
}

func benchmarkFetchGRPC(b *testing.B, withHeaders bool) {
	for _, payloadSize := range benchmarkPayloadSizes {
		for _, batchSize := range benchmarkBatchSizes(payloadSize.size) {
			name := fmt.Sprintf("%s/batch=%d", payloadSize.name, batchSize)
			b.Run(name, func(b *testing.B) {
				ctx, cleanup, stream := newGRPCBenchStream(b, MakeSessionBenchConfig(payloadSize.size))
				defer cleanup()
				bindGRPCBenchStream(b, stream)
				var req *pb.FujinRequest
				if withHeaders {
					req = &pb.FujinRequest{Request: &pb.FujinRequest_Hfetch{Hfetch: &pb.HFetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: true, BatchSize: uint32(batchSize)}}}
				} else {
					req = &pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: true, BatchSize: uint32(batchSize)}}}
				}
				done := receiveGRPCResponses(ctx, stream, b.N, func(resp *pb.FujinResponse) error {
					if withHeaders {
						value, ok := resp.Response.(*pb.FujinResponse_Hfetch)
						if !ok || value.Hfetch.Error != nil || len(value.Hfetch.Messages) != batchSize {
							return fmt.Errorf("hfetch response: %T error=%v messages=%d", resp.Response, value.Hfetch.Error, len(value.Hfetch.Messages))
						}
						return nil
					}
					value, ok := resp.Response.(*pb.FujinResponse_Fetch)
					if !ok || value.Fetch.Error != nil || len(value.Fetch.Messages) != batchSize {
						return fmt.Errorf("fetch response: %T error=%v messages=%d", resp.Response, value.Fetch.Error, len(value.Fetch.Messages))
					}
					return nil
				})
				b.SetBytes(int64(payloadSize.size * batchSize))
				b.ResetTimer()
				for b.Loop() {
					if err := stream.Send(req); err != nil {
						b.Fatal(err)
					}
				}
				if err := <-done; err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
			})
		}
	}
}

func Benchmark_HSubscribe_Session_GRPC(b *testing.B) {
	for _, payloadSize := range benchmarkPayloadSizes {
		b.Run(payloadSize.name, func(b *testing.B) {
			_, cleanup, stream := newGRPCBenchStream(b, MakeSessionBenchConfig(payloadSize.size))
			defer cleanup()
			bindGRPCBenchStream(b, stream)
			if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Hsubscribe{Hsubscribe: &pb.HSubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}}); err != nil {
				b.Fatal(err)
			}
			if resp, err := stream.Recv(); err != nil {
				b.Fatal(err)
			} else if value, ok := resp.Response.(*pb.FujinResponse_Hsubscribe); !ok || value.Hsubscribe.Error != nil {
				b.Fatalf("hsubscribe response: %T", resp.Response)
			}
			b.SetBytes(int64(payloadSize.size))
			b.ResetTimer()
			for b.Loop() {
				resp, err := stream.Recv()
				if err != nil {
					b.Fatal(err)
				}
				if _, ok := resp.Response.(*pb.FujinResponse_Hmessage); !ok {
					b.Fatalf("unexpected response %T", resp.Response)
				}
			}
			b.StopTimer()
		})
	}
}

func Benchmark_Ack_Session_GRPC(b *testing.B) {
	benchmarkAckGRPC(b, false)
}

func Benchmark_Nack_Session_GRPC(b *testing.B) {
	benchmarkAckGRPC(b, true)
}

func benchmarkAckGRPC(b *testing.B, nack bool) {
	for _, batchSize := range approvedPerformanceContract.BatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			ctx, cleanup, stream := newGRPCBenchStream(b, MakeSessionBenchConfig(128))
			defer cleanup()
			bindGRPCBenchStream(b, stream)
			if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: false, BatchSize: uint32(batchSize)}}}); err != nil {
				b.Fatal(err)
			}
			resp, err := stream.Recv()
			if err != nil {
				b.Fatal(err)
			}
			fetch, ok := resp.Response.(*pb.FujinResponse_Fetch)
			if !ok || fetch.Fetch.Error != nil || len(fetch.Fetch.Messages) != batchSize {
				b.Fatalf("fetch setup: %T error=%v", resp.Response, fetch.Fetch.Error)
			}
			ids := make([][]byte, batchSize)
			for i := range fetch.Fetch.Messages {
				ids[i] = fetch.Fetch.Messages[i].MessageId
			}
			var req *pb.FujinRequest
			if nack {
				req = &pb.FujinRequest{Request: &pb.FujinRequest_Nack{Nack: &pb.NackRequest{CorrelationId: 2, SubscriptionId: fetch.Fetch.SubscriptionId, MessageIds: ids}}}
			} else {
				req = &pb.FujinRequest{Request: &pb.FujinRequest_Ack{Ack: &pb.AckRequest{CorrelationId: 2, SubscriptionId: fetch.Fetch.SubscriptionId, MessageIds: ids}}}
			}
			done := receiveGRPCResponses(ctx, stream, b.N, func(resp *pb.FujinResponse) error {
				if nack {
					value, ok := resp.Response.(*pb.FujinResponse_Nack)
					if !ok || value.Nack.Error != nil || len(value.Nack.Results) != batchSize {
						return fmt.Errorf("nack response: %T", resp.Response)
					}
					return nil
				}
				value, ok := resp.Response.(*pb.FujinResponse_Ack)
				if !ok || value.Ack.Error != nil || len(value.Ack.Results) != batchSize {
					return fmt.Errorf("ack response: %T", resp.Response)
				}
				return nil
			})
			b.ResetTimer()
			for b.Loop() {
				if err := stream.Send(req); err != nil {
					b.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		})
	}
}

func Benchmark_Transaction_Session_GRPC(b *testing.B) {
	ctx, cleanup, stream := newGRPCBenchStream(b, MakeSessionBenchConfig(128))
	defer cleanup()
	bindGRPCBenchStream(b, stream)
	done := receiveGRPCResponses(ctx, stream, b.N*3, func(resp *pb.FujinResponse) error {
		switch value := resp.Response.(type) {
		case *pb.FujinResponse_BeginTx:
			if value.BeginTx.Error != nil {
				return fmt.Errorf("begin: %v", value.BeginTx.Error)
			}
		case *pb.FujinResponse_TxProduce:
			if value.TxProduce.Error != nil {
				return fmt.Errorf("tx produce: %v", value.TxProduce.Error)
			}
		case *pb.FujinResponse_CommitTx:
			if value.CommitTx.Error != nil {
				return fmt.Errorf("commit: %v", value.CommitTx.Error)
			}
		default:
			return fmt.Errorf("unexpected transaction response %T", resp.Response)
		}
		return nil
	})
	payload := sizedBytes(128)
	b.ResetTimer()
	for b.Loop() {
		if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_BeginTx{BeginTx: &pb.BeginTxRequest{CorrelationId: 1, Route: "pub"}}}); err != nil {
			b.Fatal(err)
		}
		if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_TxProduce{TxProduce: &pb.TxProduceRequest{CorrelationId: 2, Message: payload}}}); err != nil {
			b.Fatal(err)
		}
		if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_CommitTx{CommitTx: &pb.CommitTxRequest{CorrelationId: 3}}}); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func Benchmark_Bind_GRPC(b *testing.B) {
	_, cleanup, client := newGRPCBenchClient(b, MakeSessionBenchConfig(1))
	defer cleanup()
	b.ResetTimer()
	for b.Loop() {
		stream, err := client.Stream(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: "connector"}}}); err != nil {
			b.Fatal(err)
		}
		resp, err := stream.Recv()
		if err != nil {
			b.Fatal(err)
		}
		if value, ok := resp.Response.(*pb.FujinResponse_Bind); !ok || value.Bind.Error != nil {
			b.Fatalf("bind response: %T", resp.Response)
		}
		if err := stream.CloseSend(); err != nil {
			b.Fatal(err)
		}
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
	}
}

func Benchmark_Cleanup_GRPC(b *testing.B) {
	Benchmark_Bind_GRPC(b)
}

func benchProduceGRPC(b *testing.B, payload []byte) {
	ctx, cleanup, stream := newGRPCBenchStream(b, connectorconfig.ConnectorsConfig{
		"connector": {Type: "nop"},
	})
	defer cleanup()

	bindGRPCBenchStream(b, stream)
	req := &pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{CorrelationId: 1, Route: "pub", Message: payload}}}
	credits := make(chan struct{}, grpcBenchmarkMaxInFlight)
	for range cap(credits) {
		credits <- struct{}{}
	}
	done := receiveGRPCResponsesWithCredits(ctx, stream, b.N, credits, func(resp *pb.FujinResponse) error {
		produce, ok := resp.Response.(*pb.FujinResponse_Produce)
		if !ok {
			return fmt.Errorf("unexpected response type %T", resp.Response)
		}
		if produce.Produce.Error != nil {
			return fmt.Errorf("produce: %v", produce.Produce.Error)
		}
		return nil
	})

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		select {
		case <-credits:
		case err := <-done:
			b.Fatal(err)
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		}
		if err := stream.Send(req); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchSubscribeGRPC(b *testing.B, msgSize int) {
	_, cleanup, stream := newGRPCBenchStream(b, MakeGenConfig(msgSize))
	defer cleanup()

	bindGRPCBenchStream(b, stream)
	if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Subscribe{Subscribe: &pb.SubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}}); err != nil {
		b.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		b.Fatal(err)
	}
	if _, ok := resp.Response.(*pb.FujinResponse_Subscribe); !ok {
		b.Fatalf("unexpected response type %T", resp.Response)
	}

	b.SetBytes(int64(msgSize))
	b.ResetTimer()
	for range b.N {
		resp, err := stream.Recv()
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := resp.Response.(*pb.FujinResponse_Message); !ok {
			b.Fatalf("unexpected response type %T", resp.Response)
		}
	}
	b.StopTimer()
}

func newGRPCBenchClient(
	b *testing.B,
	connectors connectorconfig.ConnectorsConfig,
) (context.Context, func(), pb.FujinServiceClient) {
	b.Helper()
	serverCtx, serverCancel := context.WithCancel(b.Context())
	conf := MakeConfigWithGRPCAndOptionalTCP(connectors)
	srv := RunServer(serverCtx, conf)

	conn, err := grpc.NewClient(perfGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		serverCancel()
		<-srv.Done()
		b.Fatal(err)
	}
	clientCtx, clientCancel := context.WithCancel(b.Context())
	cleanup := func() {
		clientCancel()
		_ = conn.Close()
		serverCancel()
		<-srv.Done()
	}
	return clientCtx, cleanup, pb.NewFujinServiceClient(conn)
}

func newGRPCBenchStream(
	b *testing.B,
	connectors connectorconfig.ConnectorsConfig,
) (context.Context, func(), pb.FujinService_StreamClient) {
	b.Helper()
	clientCtx, cleanupClient, client := newGRPCBenchClient(b, connectors)
	streamCtx, streamCancel := context.WithCancel(clientCtx)
	stream, err := client.Stream(streamCtx)
	if err != nil {
		streamCancel()
		cleanupClient()
		b.Fatal(err)
	}
	cleanup := func() {
		_ = stream.CloseSend()
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
		streamCancel()
		cleanupClient()
	}
	return streamCtx, cleanup, stream
}

func bindGRPCBenchStream(b *testing.B, stream pb.FujinService_StreamClient) {
	b.Helper()
	if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{
		Connector: "connector",
	}}}); err != nil {
		b.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		b.Fatal(err)
	}
	bind, ok := resp.Response.(*pb.FujinResponse_Bind)
	if !ok {
		b.Fatalf("unexpected response type %T", resp.Response)
	}
	if bind.Bind.Error != nil {
		b.Fatalf("bind: %v", bind.Bind.Error)
	}
}

func receiveGRPCResponses(
	ctx context.Context,
	stream pb.FujinService_StreamClient,
	n int,
	check func(*pb.FujinResponse) error,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		for range n {
			resp, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			if err := check(resp); err != nil {
				done <- err
				return
			}
		}
		select {
		case done <- nil:
		case <-ctx.Done():
		}
	}()
	return done
}

func receiveGRPCResponsesWithCredits(
	ctx context.Context,
	stream pb.FujinService_StreamClient,
	n int,
	credits chan<- struct{},
	check func(*pb.FujinResponse) error,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		for range n {
			resp, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			if err := check(resp); err != nil {
				done <- err
				return
			}
			select {
			case credits <- struct{}{}:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
		}
		select {
		case done <- nil:
		case <-ctx.Done():
		}
	}()
	return done
}
