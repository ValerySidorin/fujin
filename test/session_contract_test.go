//go:build grpc

package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	pb "github.com/fujin-io/fujin/public/proto/grpc/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSessionContractNativeAndGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conf := MakeConfigWithGRPC(MakeSessionBenchConfig(64))
	if _, _, nativeEnabled := transport.Get("tcp"); nativeEnabled {
		conf.Transports = append(conf.Transports, DefaultTCPTransportConfig())
	}
	srv := RunServer(ctx, conf)
	t.Cleanup(func() {
		cancel()
		<-srv.Done()
	})

	if _, _, nativeEnabled := transport.Get("tcp"); nativeEnabled {
		t.Run("native", testNativeSessionContract)
	}
	t.Run("grpc", testGRPCSessionContract)
}

func testNativeSessionContract(t *testing.T) {
	conn := createTCPClientConn(PERF_TCP_ADDR)
	defer conn.Close()
	reader := newProtoReader(conn)
	nativeWrite(t, conn, bindCmd("connector", nil, nil))
	routes, err := reader.readBindCapabilities()
	require.NoError(t, err)
	assertNativeSessionBenchCapabilities(t, routes)

	nativeWrite(t, conn, buildProduceCmd(1, "pub", "message"))
	cid, err := reader.readProduceResp()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), cid)

	nativeWrite(t, conn, buildHProduceCmd(2, "pub", [][2]string{{"key", "value"}}, "message"))
	cid, err = reader.readHProduceResp()
	require.NoError(t, err)
	assert.Equal(t, uint32(2), cid)

	nativeWrite(t, conn, buildTxBeginCmd(3, "tx"))
	cid, err = reader.readTxResp(v1.RESP_CODE_TX_BEGIN)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), cid)
	nativeWrite(t, conn, buildTxHProduceCmd(4, [][2]string{{"key", "value"}}, "message"))
	_, err = reader.readTxResp(v1.RESP_CODE_TX_HPRODUCE)
	require.NoError(t, err)
	nativeWrite(t, conn, buildTxCommitCmd(5))
	_, err = reader.readTxResp(v1.RESP_CODE_TX_COMMIT)
	require.NoError(t, err)

	nativeWrite(t, conn, buildTxBeginCmd(6, "tx"))
	_, err = reader.readTxResp(v1.RESP_CODE_TX_BEGIN)
	require.NoError(t, err)
	nativeWrite(t, conn, buildTxProduceCmd(7, "message"))
	_, err = reader.readTxResp(v1.RESP_CODE_TX_PRODUCE)
	require.NoError(t, err)
	nativeWrite(t, conn, buildTxRollbackCmd(8))
	_, err = reader.readTxResp(v1.RESP_CODE_TX_ROLLBACK)
	require.NoError(t, err)

	nativeWrite(t, conn, buildFetchCmd2(9, false, "sub", 3))
	cid, subID, messages, err := reader.readFetchResp(false)
	require.NoError(t, err)
	assert.Equal(t, uint32(9), cid)
	require.Len(t, messages, 3)
	for _, message := range messages {
		assert.Len(t, message.Payload, 64)
		assert.NotEmpty(t, message.MsgID)
	}

	ids := make([][]byte, len(messages))
	for i := range messages {
		ids[i] = messages[i].MsgID
	}
	nativeWrite(t, conn, buildAckCmd(10, subID, ids))
	cid, results, err := reader.readAckResp()
	require.NoError(t, err)
	assert.Equal(t, uint32(10), cid)
	require.Len(t, results, len(ids))
	for _, result := range results {
		assert.NoError(t, result.Err)
	}

	nativeWrite(t, conn, buildHFetchCmd(11, false, "sub", 3))
	cid, nackSubID, hmessages, err := reader.readHFetchResp(false)
	require.NoError(t, err)
	assert.Equal(t, uint32(11), cid)
	require.Len(t, hmessages, 3)
	nackIDs := make([][]byte, len(hmessages))
	for i, message := range hmessages {
		assert.Equal(t, [][]byte{[]byte("content-type"), []byte("application/octet-stream")}, message.Headers)
		assert.Len(t, message.Payload, 64)
		require.NotEmpty(t, message.MsgID)
		nackIDs[i] = message.MsgID
	}
	nativeWrite(t, conn, buildNackCmd(12, nackSubID, nackIDs))
	cid, results, err = reader.readNackResp()
	require.NoError(t, err)
	assert.Equal(t, uint32(12), cid)
	require.Len(t, results, len(nackIDs))
	for _, result := range results {
		assert.NoError(t, result.Err)
	}

	nativeWrite(t, conn, buildAckCmd(13, 254, [][]byte{[]byte("unknown")}))
	_, _, err = reader.readAckResp()
	var nativeOperationErr *v1.OperationError
	require.ErrorAs(t, err, &nativeOperationErr)
	assert.Equal(t, v1.STATUS_NOT_FOUND, nativeOperationErr.Code)
	assert.Equal(t, v1.OUTCOME_NOT_APPLIED, nativeOperationErr.Outcome)
	assert.Equal(t, "SUBSCRIPTION_NOT_FOUND", nativeOperationErr.Reason)
	assert.Contains(t, nativeOperationErr.Message, "subscription not found")

	testNativeSubscribeContract(t, false)
	testNativeSubscribeContract(t, true)
}

func testNativeSubscribeContract(t *testing.T, withHeaders bool) {
	conn := createTCPClientConn(PERF_TCP_ADDR)
	defer conn.Close()
	reader := newProtoReader(conn)
	nativeWrite(t, conn, bindCmd("connector", nil, nil))
	require.NoError(t, reader.readBindResp())
	if withHeaders {
		nativeWrite(t, conn, buildHSubscribeCmd(1, true, "sub"))
	} else {
		nativeWrite(t, conn, buildSubscribeCmd2(1, true, "sub"))
	}
	_, subID, err := reader.readSubscribeResp()
	require.NoError(t, err)
	if withHeaders {
		messageSubID, headers, payload, err := reader.readHMsg()
		require.NoError(t, err)
		assert.Equal(t, subID, messageSubID)
		assert.Equal(t, [][]byte{[]byte("content-type"), []byte("application/octet-stream")}, headers)
		assert.Len(t, payload, 64)
		return
	}
	messageSubID, payload, err := reader.readMsg()
	require.NoError(t, err)
	assert.Equal(t, subID, messageSubID)
	assert.Len(t, payload, 64)
}

func testGRPCSessionContract(t *testing.T) {
	conn, err := grpc.NewClient(perfGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := pb.NewFujinServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Stream(ctx)
	require.NoError(t, err)
	defer stream.CloseSend()

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: "connector"}}})
	bind := grpcReceive(t, stream).GetBind()
	require.NotNil(t, bind)
	require.Empty(t, bind.Error)
	assertGRPCSessionBenchCapabilities(t, bind.Routes)

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{CorrelationId: 1, Route: "pub", Message: []byte("message")}}})
	produce := grpcReceive(t, stream).GetProduce()
	require.NotNil(t, produce)
	assert.Equal(t, uint32(1), produce.CorrelationId)
	require.Empty(t, produce.Error)

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Hproduce{Hproduce: &pb.HProduceRequest{CorrelationId: 2, Route: "pub", Message: []byte("message"), Headers: []*pb.KV{{Key: []byte("key"), Value: []byte("value")}}}}})
	hproduce := grpcReceive(t, stream).GetHproduce()
	require.NotNil(t, hproduce)
	require.Empty(t, hproduce.Error)

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_BeginTx{BeginTx: &pb.BeginTxRequest{CorrelationId: 3, Route: "tx"}}})
	require.Empty(t, grpcReceive(t, stream).GetBeginTx().Error)
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{CorrelationId: 30, Route: "tx", Message: []byte("wrong-command")}}})
	require.NotEmpty(t, grpcReceive(t, stream).GetProduce().Error)
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_TxHproduce{TxHproduce: &pb.TxHProduceRequest{CorrelationId: 4, Message: []byte("message"), Headers: []*pb.KV{{Key: []byte("key"), Value: []byte("value")}}}}})
	require.Empty(t, grpcReceive(t, stream).GetTxHproduce().Error)
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_CommitTx{CommitTx: &pb.CommitTxRequest{CorrelationId: 5}}})
	require.Empty(t, grpcReceive(t, stream).GetCommitTx().Error)

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_BeginTx{BeginTx: &pb.BeginTxRequest{CorrelationId: 6, Route: "tx"}}})
	require.Empty(t, grpcReceive(t, stream).GetBeginTx().Error)
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_TxProduce{TxProduce: &pb.TxProduceRequest{CorrelationId: 7, Message: []byte("message")}}})
	require.Empty(t, grpcReceive(t, stream).GetTxProduce().Error)
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_RollbackTx{RollbackTx: &pb.RollbackTxRequest{CorrelationId: 8}}})
	require.Empty(t, grpcReceive(t, stream).GetRollbackTx().Error)

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 9, Route: "sub", AutoCommit: false, BatchSize: 3}}})
	fetch := grpcReceive(t, stream).GetFetch()
	require.NotNil(t, fetch)
	require.Empty(t, fetch.Error)
	require.Len(t, fetch.Messages, 3)
	ids := make([][]byte, len(fetch.Messages))
	for i, message := range fetch.Messages {
		assert.Len(t, message.Payload, 64)
		assert.NotEmpty(t, message.MessageId)
		ids[i] = message.MessageId
	}

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Ack{Ack: &pb.AckRequest{CorrelationId: 10, SubscriptionId: fetch.SubscriptionId, MessageIds: ids}}})
	ack := grpcReceive(t, stream).GetAck()
	require.NotNil(t, ack)
	require.Empty(t, ack.Error)
	require.Len(t, ack.Results, len(ids))

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Hfetch{Hfetch: &pb.HFetchRequest{CorrelationId: 11, Route: "sub", AutoCommit: false, BatchSize: 3}}})
	hfetch := grpcReceive(t, stream).GetHfetch()
	require.NotNil(t, hfetch)
	require.Empty(t, hfetch.Error)
	require.Len(t, hfetch.Messages, 3)
	nackIDs := make([][]byte, len(hfetch.Messages))
	for i, message := range hfetch.Messages {
		assert.Len(t, message.Payload, 64)
		require.Len(t, message.Headers, 1)
		assert.Equal(t, []byte("content-type"), message.Headers[0].Key)
		assert.Equal(t, []byte("application/octet-stream"), message.Headers[0].Value)
		require.NotEmpty(t, message.MessageId)
		nackIDs[i] = message.MessageId
	}

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Nack{Nack: &pb.NackRequest{CorrelationId: 12, SubscriptionId: hfetch.SubscriptionId, MessageIds: nackIDs}}})
	nack := grpcReceive(t, stream).GetNack()
	require.NotNil(t, nack)
	require.Empty(t, nack.Error)
	require.Len(t, nack.Results, len(nackIDs))

	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Ack{Ack: &pb.AckRequest{CorrelationId: 13, SubscriptionId: 254, MessageIds: [][]byte{[]byte("unknown")}}}})
	unknown := grpcReceive(t, stream).GetAck()
	require.NotNil(t, unknown)
	require.NotNil(t, unknown.Error)
	assert.Equal(t, pb.StatusCode_STATUS_NOT_FOUND, unknown.Error.Code)
	assert.Equal(t, pb.OperationOutcome_OUTCOME_NOT_APPLIED, unknown.Error.Outcome)
	assert.Equal(t, "SUBSCRIPTION_NOT_FOUND", unknown.Error.Reason)
	assert.Contains(t, unknown.Error.Message, "subscription not found")
	assert.Empty(t, unknown.Results)

	testGRPCSubscribeContract(t, client, false)
	testGRPCSubscribeContract(t, client, true)
}

func assertNativeSessionBenchCapabilities(t *testing.T, routes map[string]nativeRouteCapabilities) {
	t.Helper()
	require.Len(t, routes, 3)
	assert.Equal(t, nativeRouteCapabilities{
		Produce: true, Headers: true, ProduceGuarantee: v1.PRODUCE_GUARANTEE_LOCAL_ACCEPT,
	}, routes["pub"])
	assert.Equal(t, nativeRouteCapabilities{
		Produce: true, Headers: true, Transactions: true, ProduceGuarantee: v1.PRODUCE_GUARANTEE_LOCAL_ACCEPT,
	}, routes["tx"])
	assert.Equal(t, nativeRouteCapabilities{
		Headers: true, Subscribe: true, Fetch: true, ManualSettlement: true,
		AckGranularity: v1.ACK_GRANULARITY_SINGLE, NackEffect: v1.NACK_EFFECT_DROP,
	}, routes["sub"])
}

func assertGRPCSessionBenchCapabilities(t *testing.T, routes map[string]*pb.RouteCapabilities) {
	t.Helper()
	require.Len(t, routes, 3)
	assert.Equal(t, &pb.RouteCapabilities{
		Produce: true, Headers: true, ProduceGuarantee: pb.ProduceGuarantee_PRODUCE_GUARANTEE_LOCAL_ACCEPT,
	}, routes["pub"])
	assert.Equal(t, &pb.RouteCapabilities{
		Produce: true, Headers: true, Transactions: true,
		ProduceGuarantee: pb.ProduceGuarantee_PRODUCE_GUARANTEE_LOCAL_ACCEPT,
	}, routes["tx"])
	assert.Equal(t, &pb.RouteCapabilities{
		Headers: true, Subscribe: true, Fetch: true, ManualSettlement: true,
		AckGranularity: pb.AckGranularity_ACK_GRANULARITY_SINGLE,
		NackEffect:     pb.NackEffect_NACK_EFFECT_DROP,
	}, routes["sub"])
}

func testGRPCSubscribeContract(t *testing.T, client pb.FujinServiceClient, withHeaders bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Stream(ctx)
	require.NoError(t, err)
	defer stream.CloseSend()
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: "connector"}}})
	require.Empty(t, grpcReceive(t, stream).GetBind().Error)
	if withHeaders {
		grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Hsubscribe{Hsubscribe: &pb.HSubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}})
		subscribe := grpcReceive(t, stream).GetHsubscribe()
		require.NotNil(t, subscribe)
		require.Empty(t, subscribe.Error)
		message := grpcReceive(t, stream).GetHmessage()
		require.NotNil(t, message)
		assert.Equal(t, subscribe.SubscriptionId, message.SubscriptionId)
		assert.Len(t, message.Payload, 64)
		require.Len(t, message.Headers, 1)
		return
	}
	grpcSend(t, stream, &pb.FujinRequest{Request: &pb.FujinRequest_Subscribe{Subscribe: &pb.SubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}})
	subscribe := grpcReceive(t, stream).GetSubscribe()
	require.NotNil(t, subscribe)
	require.Empty(t, subscribe.Error)
	message := grpcReceive(t, stream).GetMessage()
	require.NotNil(t, message)
	assert.Equal(t, subscribe.SubscriptionId, message.SubscriptionId)
	assert.Len(t, message.Payload, 64)
}

func TestRespondToPingRepliesWhenReadReturnsDataWithoutEOF(t *testing.T) {
	stream := bytes.NewBuffer([]byte{byte(v1.OP_CODE_PING)})
	require.NoError(t, respondToPing(stream))
	assert.Equal(t, []byte{byte(v1.RESP_CODE_PONG)}, stream.Bytes())
}

func TestSessionBenchSubscribeWaitsForStart(t *testing.T) {
	start := make(chan struct{})
	reader := &genReader{subscribeStart: start}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- reader.Subscribe(ctx, func() error { return nil }, func([]byte, string, ...any) {
			received <- struct{}{}
			cancel()
		})
	}()

	select {
	case <-received:
		t.Fatal("subscription emitted before benchmark workers were ready")
	case <-time.After(25 * time.Millisecond):
	}
	close(start)
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("subscription did not start after release")
	}
	require.NoError(t, <-done)
}

func TestGenConnectorUsesClonedMapSettings(t *testing.T) {
	cloned, err := config.CloneConnectorConfig(config.ConnectorConfig{
		Type: "gen", Settings: GenConfig{MsgSize: 8, SubscribeLimit: 3},
	})
	require.NoError(t, err)

	connectorInstance, err := newGenConnector(cloned.Settings)
	require.NoError(t, err)
	assert.Equal(t, 8, connectorInstance.msgSize)
	assert.Equal(t, uint64(3), connectorInstance.subscribeLimit)
}

func TestGenConnectorSubscribeLimitStopsUntilCancellation(t *testing.T) {
	connectorInstance, err := newGenConnector(GenConfig{MsgSize: 8, SubscribeLimit: 3})
	require.NoError(t, err)
	readerInstance, err := connectorInstance.NewReader(true, nil)
	require.NoError(t, err)
	reader := readerInstance.(*genReader)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	received := make(chan int, 3)
	go func() {
		done <- reader.Subscribe(ctx, func() error { return nil }, func(message []byte, _ string, _ ...any) {
			received <- len(message)
		})
	}()

	for range 3 {
		select {
		case size := <-received:
			assert.Equal(t, 8, size)
		case <-time.After(time.Second):
			t.Fatal("subscription did not reach configured limit")
		}
	}
	select {
	case <-received:
		t.Fatal("subscription exceeded configured limit")
	case err := <-done:
		t.Fatalf("subscription returned before cancellation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	require.NoError(t, <-done)
}

func TestSessionBenchmarkWarmupRunsWorkersConcurrently(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})
	workers := make([]sessionBenchmarkWorker, 2)
	for i := range workers {
		workerID := i
		workers[i].run = func() error {
			entered <- workerID
			<-release
			return nil
		}
	}

	done := make(chan error, 1)
	go func() { done <- warmSessionBenchmarkWorkers(workers) }()
	for range workers {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("warmup workers did not enter concurrently")
		}
	}
	close(release)
	require.NoError(t, <-done)
}

func TestSessionBenchmarkOperationCountsDistributeExactTotal(t *testing.T) {
	counts := sessionBenchmarkOperationCounts(16, 100)
	require.Len(t, counts, 16)
	assert.Equal(t, []int{7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}, counts)
}

func TestSessionBenchmarkSettlementIDsAdvanceWithoutMutatingSeed(t *testing.T) {
	seed := [][]byte{
		{1, 0, 0, 0, 1, 0, 0, 0, 10},
		{1, 0, 0, 0, 1, 0, 0, 0, 11},
	}
	original := [][]byte{append([]byte(nil), seed[0]...), append([]byte(nil), seed[1]...)}

	ids, err := cloneSessionBenchmarkSettlementIDs(seed)
	require.NoError(t, err)
	require.NoError(t, advanceSessionBenchmarkSettlementIDs(ids))

	assert.Equal(t, original, seed)
	assert.Equal(t, uint64(1)<<32|12, binary.BigEndian.Uint64(ids[0][sessionBenchmarkMessageIDSequenceOffset:sessionBenchmarkMessageIDEnvelopeLen]))
	assert.Equal(t, uint64(1)<<32|13, binary.BigEndian.Uint64(ids[1][sessionBenchmarkMessageIDSequenceOffset:sessionBenchmarkMessageIDEnvelopeLen]))
	assert.NotSame(t, &seed[0][0], &ids[0][0])
}

func TestSessionBenchmarkSettlementFrameAdvancesMessageIDs(t *testing.T) {
	frame := buildAckCmd(2, 7, [][]byte{
		{1, 0, 0, 0, 1, 0, 0, 0, 10},
		{1, 0, 0, 0, 1, 0, 0, 0, 11},
	})

	require.NoError(t, advanceSessionBenchmarkSettlementFrame(frame))

	firstOffset := 10 + 4 + sessionBenchmarkMessageIDSequenceOffset
	secondOffset := 10 + 4 + sessionBenchmarkMessageIDEnvelopeLen + 4 + sessionBenchmarkMessageIDSequenceOffset
	assert.Equal(t, uint64(1)<<32|12, binary.BigEndian.Uint64(frame[firstOffset:firstOffset+sessionBenchmarkMessageIDSequenceLen]))
	assert.Equal(t, uint64(1)<<32|13, binary.BigEndian.Uint64(frame[secondOffset:secondOffset+sessionBenchmarkMessageIDSequenceLen]))
}

func TestSessionBenchConfigDistributesSubscribeLimitsAcrossConnectors(t *testing.T) {
	config := makeSessionBenchConfig(1, []int{7, 6}, false, nil)
	settings := config["connector"].Settings

	firstConnector := newSessionBenchConnector(settings)
	firstReader, err := firstConnector.NewReader(true, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), firstReader.(*genReader).subscribeLimit)

	secondConnector := newSessionBenchConnector(settings)
	secondReader, err := secondConnector.NewReader(true, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), secondReader.(*genReader).subscribeLimit)
}

func nativeWrite(t *testing.T, writer io.Writer, payload []byte) {
	t.Helper()
	_, err := writer.Write(payload)
	require.NoError(t, err)
}

func grpcSend(t *testing.T, stream pb.FujinService_StreamClient, request *pb.FujinRequest) {
	t.Helper()
	require.NoError(t, stream.Send(request))
}

func grpcReceive(t *testing.T, stream pb.FujinService_StreamClient) *pb.FujinResponse {
	t.Helper()
	response, err := stream.Recv()
	require.NoError(t, err)
	return response
}
