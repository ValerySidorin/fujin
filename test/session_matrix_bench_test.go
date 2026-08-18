//go:build grpc

package test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	pb "github.com/fujin-io/fujin/public/proto/grpc/v1"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	quicgo "github.com/quic-go/quic-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type sessionBenchmarkOperation string

const (
	benchmarkProduce     sessionBenchmarkOperation = "produce"
	benchmarkHProduce    sessionBenchmarkOperation = "hproduce"
	benchmarkFetch       sessionBenchmarkOperation = "fetch"
	benchmarkHFetch      sessionBenchmarkOperation = "hfetch"
	benchmarkSubscribe   sessionBenchmarkOperation = "subscribe"
	benchmarkHSubscribe  sessionBenchmarkOperation = "hsubscribe"
	benchmarkAck         sessionBenchmarkOperation = "ack"
	benchmarkNack        sessionBenchmarkOperation = "nack"
	benchmarkTransaction sessionBenchmarkOperation = "transaction"
)

type sessionBenchmarkWorker struct {
	run     func() error
	abort   func()
	close   func() error
	latency []int64
}

func Benchmark_Session_Produce_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkProduce, false)
}

func Benchmark_Session_HProduce_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkHProduce, false)
}

func Benchmark_Session_Fetch_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkFetch, true)
}

func Benchmark_Session_HFetch_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkHFetch, true)
}

func Benchmark_Session_Subscribe_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkSubscribe, false)
}

func Benchmark_Session_HSubscribe_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkHSubscribe, false)
}

func Benchmark_Session_Ack_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkAck, true)
}

func Benchmark_Session_Nack_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkNack, true)
}

func Benchmark_Session_Transaction_Native(b *testing.B) {
	benchmarkNativeSessionMatrix(b, benchmarkTransaction, false)
}

func Benchmark_Session_Produce_TCP_WebSocket(b *testing.B) {
	benchmarkNativeSessionTransports(b, benchmarkProduce, false, []string{"tcp", "websocket"})
}

func Benchmark_Session_Fetch_TCP_WebSocket(b *testing.B) {
	benchmarkNativeSessionTransports(b, benchmarkFetch, true, []string{"tcp", "websocket"})
}

func Benchmark_Session_Produce_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkProduce, false)
}

func Benchmark_Session_HProduce_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkHProduce, false)
}

func Benchmark_Session_Fetch_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkFetch, true)
}

func Benchmark_Session_HFetch_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkHFetch, true)
}

func Benchmark_Session_Subscribe_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkSubscribe, false)
}

func Benchmark_Session_HSubscribe_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkHSubscribe, false)
}

func Benchmark_Session_Ack_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkAck, true)
}

func Benchmark_Session_Nack_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkNack, true)
}

func Benchmark_Session_Transaction_GRPC(b *testing.B) {
	benchmarkGRPCSessionMatrix(b, benchmarkTransaction, false)
}

func benchmarkNativeSessionMatrix(b *testing.B, operation sessionBenchmarkOperation, batched bool) {
	benchmarkNativeSessionTransports(b, operation, batched, []string{"tcp", "quic", "unix"})
}

func benchmarkNativeSessionTransports(
	b *testing.B,
	operation sessionBenchmarkOperation,
	batched bool,
	transports []string,
) {
	for _, transport := range transports {
		for _, payload := range benchmarkPayloadSizes {
			if !sessionBenchmarkPayloadEnabled(payload.name) {
				continue
			}
			batchSizes := []int{1}
			if batched {
				batchSizes = benchmarkBatchSizes(payload.size)
			}
			for _, batchSize := range batchSizes {
				if !sessionBenchmarkBatchEnabled(batchSize) {
					continue
				}
				for _, concurrency := range approvedPerformanceContract.Concurrency {
					if !sessionBenchmarkConcurrencyEnabled(concurrency) {
						continue
					}
					name := fmt.Sprintf("connector=%s/transport=%s/payload=%s/batch=%d/concurrency=%d", sessionBenchmarkConnectorName(operation), transport, payload.name, batchSize, concurrency)
					b.Run(name, func(b *testing.B) {
						isSubscription := operation == benchmarkSubscribe || operation == benchmarkHSubscribe
						workerCount := sessionBenchmarkWorkerCount(concurrency, b.N)
						var subscribeLimits []int
						var subscribeStart chan struct{}
						if isSubscription {
							subscribeLimits = sessionBenchmarkOperationCounts(workerCount, b.N)
							subscribeStart = make(chan struct{})
						}
						config := sessionBenchmarkConnectors(operation, payload.size, subscribeLimits, true, subscribeStart)
						ctx, cleanupServer := startNativeSessionBenchmarkServer(b, transport, config)
						defer cleanupServer()
						var sharedQUIC *quicgo.Conn
						if transport == "quic" {
							sharedQUIC = createClientConn(ctx, PERF_ADDR)
							defer sharedQUIC.CloseWithError(0, "")
						}
						workers := make([]sessionBenchmarkWorker, workerCount)
						for i := range workers {
							workers[i] = newNativeSessionBenchmarkWorker(b, ctx, transport, operation, payload.size, batchSize, sharedQUIC)
						}
						runSessionBenchmarkWorkers(b, workers, payload.size*batchSize, sessionBenchmarkNeedsWarmup(operation), func() {
							if subscribeStart != nil {
								close(subscribeStart)
							}
						})
					})
				}
			}
		}
	}
}

func benchmarkGRPCSessionMatrix(b *testing.B, operation sessionBenchmarkOperation, batched bool) {
	for _, payload := range benchmarkPayloadSizes {
		if !sessionBenchmarkPayloadEnabled(payload.name) {
			continue
		}
		batchSizes := []int{1}
		if batched {
			batchSizes = benchmarkBatchSizes(payload.size)
		}
		for _, batchSize := range batchSizes {
			if !sessionBenchmarkBatchEnabled(batchSize) {
				continue
			}
			for _, concurrency := range approvedPerformanceContract.Concurrency {
				if !sessionBenchmarkConcurrencyEnabled(concurrency) {
					continue
				}
				name := fmt.Sprintf("connector=%s/payload=%s/batch=%d/concurrency=%d", sessionBenchmarkConnectorName(operation), payload.name, batchSize, concurrency)
				b.Run(name, func(b *testing.B) {
					isSubscription := operation == benchmarkSubscribe || operation == benchmarkHSubscribe
					workerCount := sessionBenchmarkWorkerCount(concurrency, b.N)
					var subscribeLimits []int
					var subscribeStart chan struct{}
					if isSubscription {
						subscribeLimits = sessionBenchmarkOperationCounts(workerCount, b.N)
						subscribeStart = make(chan struct{})
					}
					ctx, cleanupServer := startGRPCSessionBenchmarkServer(b, sessionBenchmarkConnectors(operation, payload.size, subscribeLimits, false, subscribeStart))
					defer cleanupServer()
					conn, err := grpc.NewClient(perfGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						b.Fatal(err)
					}
					defer conn.Close()
					workers := make([]sessionBenchmarkWorker, workerCount)
					for i := range workers {
						workers[i] = newGRPCSessionBenchmarkWorker(b, ctx, conn, operation, payload.size, batchSize)
					}
					runSessionBenchmarkWorkers(b, workers, payload.size*batchSize, sessionBenchmarkNeedsWarmup(operation), func() {
						if subscribeStart != nil {
							close(subscribeStart)
						}
					})
				})
			}
		}
	}
}

func sessionBenchmarkNeedsWarmup(operation sessionBenchmarkOperation) bool {
	return operation != benchmarkSubscribe && operation != benchmarkHSubscribe && operation != benchmarkAck && operation != benchmarkNack
}

func sessionBenchmarkBatchEnabled(batchSize int) bool {
	return sessionBenchmarkValueEnabled(strconv.Itoa(batchSize), os.Getenv("FUJIN_BENCH_BATCH"))
}

func sessionBenchmarkConnectorName(operation sessionBenchmarkOperation) string {
	if operation == benchmarkProduce {
		return "nop"
	}
	return "session_bench"
}

func sessionBenchmarkConnectors(operation sessionBenchmarkOperation, msgSize int, limits []int, ackDoneFirst bool, subscribeStart <-chan struct{}) connectorconfig.ConnectorsConfig {
	if operation == benchmarkProduce {
		return connectorconfig.ConnectorsConfig{"connector": {Type: "nop"}}
	}
	return makeSessionBenchConfig(msgSize, limits, ackDoneFirst, subscribeStart)
}

func sessionBenchmarkConcurrencyEnabled(concurrency int) bool {
	return sessionBenchmarkValueEnabled(strconv.Itoa(concurrency), os.Getenv("FUJIN_BENCH_CONCURRENCY"))
}

func sessionBenchmarkValueEnabled(value, filter string) bool {
	if filter == "" {
		return true
	}
	for _, candidate := range strings.Split(filter, ",") {
		if strings.TrimSpace(candidate) == value {
			return true
		}
	}
	return false
}

func TestSessionBenchmarkValueEnabled(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  string
		filter string
		want   bool
	}{
		{name: "empty filter", value: "16", want: true},
		{name: "single matching value", value: "16", filter: "16", want: true},
		{name: "multiple matching values", value: "16", filter: "1,16,128", want: true},
		{name: "whitespace", value: "16", filter: "1, 16, 128", want: true},
		{name: "missing value", value: "16", filter: "1,128", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := sessionBenchmarkValueEnabled(test.value, test.filter); got != test.want {
				t.Fatalf("sessionBenchmarkValueEnabled(%q, %q): got %t, want %t", test.value, test.filter, got, test.want)
			}
		})
	}
}

func sessionBenchmarkWorkerCount(concurrency, operations int) int {
	if operations < concurrency {
		return operations
	}
	return concurrency
}

func sessionBenchmarkOperationCounts(workers, operations int) []int {
	counts := make([]int, workers)
	for i := range operations {
		counts[i%workers]++
	}
	return counts
}

const (
	sessionBenchmarkMessageIDSequenceOffset = 1
	sessionBenchmarkMessageIDSequenceLen    = 8
	sessionBenchmarkMessageIDEnvelopeLen    = sessionBenchmarkMessageIDSequenceOffset + sessionBenchmarkMessageIDSequenceLen
)

func cloneSessionBenchmarkSettlementIDs(seed [][]byte) ([][]byte, error) {
	ids := make([][]byte, len(seed))
	for i, id := range seed {
		if len(id) < sessionBenchmarkMessageIDEnvelopeLen {
			return nil, fmt.Errorf("settlement message ID %d is %d bytes, want at least %d", i, len(id), sessionBenchmarkMessageIDEnvelopeLen)
		}
		ids[i] = append([]byte(nil), id...)
	}
	return ids, nil
}

func advanceSessionBenchmarkSettlementIDs(ids [][]byte) error {
	increment := uint64(len(ids))
	for i, id := range ids {
		if len(id) < sessionBenchmarkMessageIDEnvelopeLen {
			return fmt.Errorf("settlement message ID %d is %d bytes, want at least %d", i, len(id), sessionBenchmarkMessageIDEnvelopeLen)
		}
		sequence := binary.BigEndian.Uint64(id[sessionBenchmarkMessageIDSequenceOffset:sessionBenchmarkMessageIDEnvelopeLen])
		if sequence > ^uint64(0)-increment {
			return fmt.Errorf("settlement message ID %d sequence %d overflows uint64", i, sequence)
		}
		binary.BigEndian.PutUint64(id[sessionBenchmarkMessageIDSequenceOffset:sessionBenchmarkMessageIDEnvelopeLen], sequence+increment)
	}
	return nil
}

func advanceSessionBenchmarkSettlementFrame(frame []byte) error {
	const headerLen = 1 + 4 + 1 + 4
	if len(frame) < headerLen {
		return fmt.Errorf("settlement frame is %d bytes, want at least %d", len(frame), headerLen)
	}
	count := binary.BigEndian.Uint32(frame[6:10])
	offset := headerLen
	for i := uint32(0); i < count; i++ {
		if len(frame)-offset < 4 {
			return fmt.Errorf("settlement frame message ID %d length is truncated", i)
		}
		idLen := int(binary.BigEndian.Uint32(frame[offset : offset+4]))
		offset += 4
		if idLen < sessionBenchmarkMessageIDEnvelopeLen || len(frame)-offset < idLen {
			return fmt.Errorf("settlement frame message ID %d has invalid length %d", i, idLen)
		}
		sequenceOffset := offset + sessionBenchmarkMessageIDSequenceOffset
		sequenceEnd := sequenceOffset + sessionBenchmarkMessageIDSequenceLen
		sequence := binary.BigEndian.Uint64(frame[sequenceOffset:sequenceEnd])
		if sequence > ^uint64(0)-uint64(count) {
			return fmt.Errorf("settlement frame message ID %d sequence %d overflows uint64", i, sequence)
		}
		binary.BigEndian.PutUint64(frame[sequenceOffset:sequenceEnd], sequence+uint64(count))
		offset += idLen
	}
	if offset != len(frame) {
		return fmt.Errorf("settlement frame has %d trailing bytes", len(frame)-offset)
	}
	return nil
}

func runSessionBenchmarkWorkers(b *testing.B, workers []sessionBenchmarkWorker, bytesPerOperation int, warmup bool, beforeStart func()) {
	b.Helper()
	counts := sessionBenchmarkOperationCounts(len(workers), b.N)
	for i := range workers {
		workers[i].latency = make([]int64, 0, counts[i])
	}
	deadline := time.AfterFunc(sessionBenchmarkDeadline(b), func() {
		for i := range workers {
			if workers[i].abort != nil {
				workers[i].abort()
			}
		}
	})
	defer deadline.Stop()
	if warmup {
		if err := warmSessionBenchmarkWorkers(workers); err != nil {
			b.Fatal(err)
		}
	}
	start := make(chan struct{})
	errs := make(chan error, len(workers))
	var wg sync.WaitGroup
	for i := range workers {
		workerID := i
		worker := &workers[i]
		count := counts[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for operationIndex := range count {
				started := time.Now()
				if err := worker.run(); err != nil {
					errs <- fmt.Errorf("worker %d operation %d/%d: %w", workerID, operationIndex+1, count, err)
					return
				}
				worker.latency = append(worker.latency, time.Since(started).Nanoseconds())
			}
		}()
	}
	b.SetBytes(int64(bytesPerOperation))
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	if beforeStart != nil {
		beforeStart()
	}
	wg.Wait()
	b.StopTimer()
	close(errs)
	for err := range errs {
		if err != nil {
			b.Fatal(err)
		}
	}
	var closeErr error
	for i := range workers {
		if err := workers[i].close(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	var latencies []int64
	for i := range workers {
		latencies = append(latencies, workers[i].latency...)
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		index := (99*len(latencies)+99)/100 - 1
		b.ReportMetric(float64(latencies[index]), "p99-ns")
	}
}
func warmSessionBenchmarkWorkers(workers []sessionBenchmarkWorker) error {
	errs := make(chan error, len(workers))
	var wg sync.WaitGroup
	for i := range workers {
		workerID := i
		worker := &workers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.run(); err != nil {
				errs <- fmt.Errorf("warmup worker %d: %w", workerID, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}

func sessionBenchmarkDeadline(b *testing.B) time.Duration {
	b.Helper()
	value := os.Getenv("FUJIN_BENCH_DEADLINE")
	if value == "" {
		return 5 * time.Minute
	}
	deadline, err := time.ParseDuration(value)
	if err != nil || deadline <= 0 {
		b.Fatalf("invalid FUJIN_BENCH_DEADLINE %q", value)
	}
	return deadline
}

func startNativeSessionBenchmarkServer(b *testing.B, transport string, connectors connectorconfig.ConnectorsConfig) (context.Context, func()) {
	b.Helper()
	ctx, cancel := context.WithCancel(b.Context())
	var config serverconfig.Config
	switch transport {
	case "tcp":
		config = MakeConfigWithTCP(connectors)
	case "quic":
		config = MakeConfigWithQUIC(connectors)
	case "unix":
		config = MakeConfigWithUnix(connectors)
	case "websocket":
		config = MakeConfigWithWebSocket(connectors)
	default:
		b.Fatalf("unknown transport %q", transport)
	}
	server := RunServer(ctx, config)
	return ctx, func() {
		cancel()
		<-server.Done()
	}
}

func startGRPCSessionBenchmarkServer(b *testing.B, connectors connectorconfig.ConnectorsConfig) (context.Context, func()) {
	b.Helper()
	ctx, cancel := context.WithCancel(b.Context())
	config := MakeConfigWithGRPCAndOptionalTCP(connectors)
	server := RunServer(ctx, config)
	return ctx, func() {
		cancel()
		<-server.Done()
	}
}

func newNativeSessionBenchmarkWorker(b *testing.B, ctx context.Context, transport string, operation sessionBenchmarkOperation, payloadSize, batchSize int, sharedQUIC *quicgo.Conn) sessionBenchmarkWorker {
	b.Helper()
	session := openNativeBenchmarkSession(b, ctx, transport, sharedQUIC)
	payload := sizedString(payloadSize)
	var run func() error
	switch operation {
	case benchmarkProduce:
		request := buildProduceCmd(1, "pub", payload)
		run = func() error {
			if err := writeBenchmarkFrame(session.rw, request); err != nil {
				return err
			}
			_, err := session.reader.readProduceResp()
			return err
		}
	case benchmarkHProduce:
		request := buildHProduceCmd(1, "pub", [][2]string{{"content-type", "application/octet-stream"}}, payload)
		run = func() error {
			if err := writeBenchmarkFrame(session.rw, request); err != nil {
				return err
			}
			_, err := session.reader.readHProduceResp()
			return err
		}
	case benchmarkFetch, benchmarkHFetch:
		withHeaders := operation == benchmarkHFetch
		request := buildFetchCmd2(1, true, "sub", uint32(batchSize))
		if withHeaders {
			request = buildHFetchCmd(1, true, "sub", uint32(batchSize))
		}
		run = func() error {
			if err := writeBenchmarkFrame(session.rw, request); err != nil {
				return err
			}
			if withHeaders {
				_, _, messages, err := session.reader.readHFetchResp(true)
				if err != nil {
					return err
				}
				if len(messages) != batchSize {
					return fmt.Errorf("hfetch messages: got %d, want %d", len(messages), batchSize)
				}
				return validateFetchedPayloads(messages, payloadSize, true)
			}
			_, _, messages, err := session.reader.readFetchResp(true)
			if err != nil {
				return err
			}
			if len(messages) != batchSize {
				return fmt.Errorf("fetch messages: got %d, want %d", len(messages), batchSize)
			}
			return validateFetchedPayloads(messages, payloadSize, false)
		}
	case benchmarkSubscribe, benchmarkHSubscribe:
		withHeaders := operation == benchmarkHSubscribe
		request := buildSubscribeCmd2(1, true, "sub")
		if withHeaders {
			request = buildHSubscribeCmd(1, true, "sub")
		}
		if err := writeBenchmarkFrame(session.rw, request); err != nil {
			b.Fatal(err)
		}
		_, subscriptionID, err := session.reader.readSubscribeResp()
		if err != nil {
			b.Fatal(err)
		}
		run = func() error {
			if withHeaders {
				id, headers, payload, err := session.reader.readHMsg()
				if err != nil {
					return err
				}
				if id != subscriptionID || len(payload) != payloadSize || len(headers) != 2 {
					return fmt.Errorf("invalid hsubscribe message")
				}
				return nil
			}
			id, payload, err := session.reader.readMsg()
			if err != nil {
				return err
			}
			if id != subscriptionID || len(payload) != payloadSize {
				return fmt.Errorf("invalid subscribe message")
			}
			return nil
		}
	case benchmarkAck, benchmarkNack:
		if err := writeBenchmarkFrame(session.rw, buildFetchCmd2(1, false, "sub", uint32(batchSize))); err != nil {
			b.Fatal(err)
		}
		_, subscriptionID, messages, err := session.reader.readFetchResp(false)
		if err != nil {
			b.Fatal(err)
		}
		ids := make([][]byte, len(messages))
		for i := range messages {
			ids[i] = messages[i].MsgID
		}
		request := buildAckCmd(2, subscriptionID, ids)
		if operation == benchmarkNack {
			request = buildNackCmd(2, subscriptionID, ids)
		}
		settlementScratch := make([]byte, 0, 64)
		run = func() error {
			if err := writeBenchmarkFrame(session.rw, request); err != nil {
				return err
			}
			expectedCode := v1.RESP_CODE_ACK
			if operation == benchmarkNack {
				expectedCode = v1.RESP_CODE_NACK
			}
			count, err := readBenchmarkSettlementResp(session.reader, expectedCode, &settlementScratch)
			if err != nil {
				return err
			}
			if count != batchSize {
				return fmt.Errorf("settlement results: got %d, want %d", count, batchSize)
			}
			return advanceSessionBenchmarkSettlementFrame(request)
		}
	case benchmarkTransaction:
		begin := buildTxBeginCmd(1, "tx")
		produce := buildTxProduceCmd(2, payload)
		commit := buildTxCommitCmd(3)
		run = func() error {
			if err := writeBenchmarkFrame(session.rw, begin); err != nil {
				return err
			}
			if _, err := session.reader.readTxResp(v1.RESP_CODE_TX_BEGIN); err != nil {
				return err
			}
			if err := writeBenchmarkFrame(session.rw, produce); err != nil {
				return err
			}
			if _, err := session.reader.readTxResp(v1.RESP_CODE_TX_PRODUCE); err != nil {
				return err
			}
			if err := writeBenchmarkFrame(session.rw, commit); err != nil {
				return err
			}
			_, err := session.reader.readTxResp(v1.RESP_CODE_TX_COMMIT)
			return err
		}
	default:
		b.Fatalf("unknown operation %q", operation)
	}
	return sessionBenchmarkWorker{run: run, close: session.close}
}

type nativeBenchmarkSession struct {
	rw     io.ReadWriteCloser
	reader *protoReader
	close  func() error
}

func openNativeBenchmarkSession(b *testing.B, ctx context.Context, transport string, sharedQUIC *quicgo.Conn) nativeBenchmarkSession {
	b.Helper()
	var rw io.ReadWriteCloser
	closeTransport := func() error { return nil }
	switch transport {
	case "tcp":
		rw = createTCPClientConn(PERF_TCP_ADDR)
	case "websocket":
		rw = createWebSocketBenchmarkConn()
	case "unix":
		rw = createUnixClientConn(PERF_UNIX_PATH)
	case "quic":
		connection := sharedQUIC
		if connection == nil {
			connection = createClientConn(ctx, PERF_ADDR)
			closeTransport = func() error { return connection.CloseWithError(0, "") }
		}
		stream, err := connection.OpenStream()
		if err != nil {
			b.Fatal(err)
		}
		rw = stream
	default:
		b.Fatalf("unknown transport %q", transport)
	}
	if deadline, ok := rw.(interface{ SetDeadline(time.Time) error }); ok {
		_ = deadline.SetDeadline(time.Now().Add(sessionBenchmarkDeadline(b)))
	}
	reader := newProtoReaderFromReadWriter(rw)
	if err := writeBenchmarkFrame(rw, bindCmd("connector", nil, nil)); err != nil {
		b.Fatal(err)
	}
	if err := reader.readBindResp(); err != nil {
		b.Fatal(err)
	}
	closeSession := func() error {
		if err := writeBenchmarkFrame(rw, buildDisconnectCmd()); err != nil {
			return err
		}
		if _, err := reader.readByte(); err != nil {
			return err
		}
		if sharedQUIC != nil {
			stream := rw.(*quicgo.Stream)
			stream.CancelRead(v1.NoErr)
			stream.CancelWrite(v1.NoErr)
			return nil
		}
		return errors.Join(rw.Close(), closeTransport())
	}
	return nativeBenchmarkSession{rw: rw, reader: reader, close: closeSession}
}

func newProtoReaderFromReadWriter(reader io.Reader) *protoReader {
	return newProtoReader(reader)
}

func newGRPCSessionBenchmarkWorker(b *testing.B, ctx context.Context, conn *grpc.ClientConn, operation sessionBenchmarkOperation, payloadSize, batchSize int) sessionBenchmarkWorker {
	b.Helper()
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := pb.NewFujinServiceClient(conn).Stream(streamCtx)
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: "connector"}}}); err != nil {
		b.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || response.GetBind() == nil || response.GetBind().Error != nil {
		b.Fatalf("grpc bind: response=%v err=%v", response, err)
	}
	payload := sizedBytes(payloadSize)
	var run func() error
	switch operation {
	case benchmarkProduce:
		request := &pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{CorrelationId: 1, Route: "pub", Message: payload}}}
		run = grpcRoundTrip(stream, request, func(response *pb.FujinResponse) error {
			value := response.GetProduce()
			if value == nil || value.Error != nil {
				return fmt.Errorf("produce response: %v", response)
			}
			return nil
		})
	case benchmarkHProduce:
		request := &pb.FujinRequest{Request: &pb.FujinRequest_Hproduce{Hproduce: &pb.HProduceRequest{CorrelationId: 1, Route: "pub", Message: payload, Headers: []*pb.KV{{Key: []byte("content-type"), Value: []byte("application/octet-stream")}}}}}
		run = grpcRoundTrip(stream, request, func(response *pb.FujinResponse) error {
			value := response.GetHproduce()
			if value == nil || value.Error != nil {
				return fmt.Errorf("hproduce response: %v", response)
			}
			return nil
		})
	case benchmarkFetch, benchmarkHFetch:
		withHeaders := operation == benchmarkHFetch
		var request *pb.FujinRequest
		if withHeaders {
			request = &pb.FujinRequest{Request: &pb.FujinRequest_Hfetch{Hfetch: &pb.HFetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: true, BatchSize: uint32(batchSize)}}}
		} else {
			request = &pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: true, BatchSize: uint32(batchSize)}}}
		}
		run = grpcRoundTrip(stream, request, func(response *pb.FujinResponse) error {
			if withHeaders {
				value := response.GetHfetch()
				if value == nil || value.Error != nil || len(value.Messages) != batchSize {
					return fmt.Errorf("hfetch response: %v", response)
				}
				for _, message := range value.Messages {
					if len(message.Payload) != payloadSize || len(message.Headers) != 1 {
						return fmt.Errorf("invalid hfetch message")
					}
				}
				return nil
			}
			value := response.GetFetch()
			if value == nil || value.Error != nil || len(value.Messages) != batchSize {
				return fmt.Errorf("fetch response: %v", response)
			}
			for _, message := range value.Messages {
				if len(message.Payload) != payloadSize {
					return fmt.Errorf("invalid fetch payload")
				}
			}
			return nil
		})
	case benchmarkSubscribe, benchmarkHSubscribe:
		withHeaders := operation == benchmarkHSubscribe
		if withHeaders {
			if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Hsubscribe{Hsubscribe: &pb.HSubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}}); err != nil {
				b.Fatal(err)
			}
			response, err := stream.Recv()
			if err != nil || response.GetHsubscribe() == nil || response.GetHsubscribe().Error != nil {
				b.Fatalf("hsubscribe response: %v %v", response, err)
			}
			subscriptionID := response.GetHsubscribe().SubscriptionId
			run = func() error {
				response, err := stream.Recv()
				if err != nil {
					return err
				}
				message := response.GetHmessage()
				if message == nil || message.SubscriptionId != subscriptionID || len(message.Payload) != payloadSize || len(message.Headers) != 1 {
					return fmt.Errorf("invalid hsubscribe response")
				}
				return nil
			}
		} else {
			if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Subscribe{Subscribe: &pb.SubscribeRequest{CorrelationId: 1, Route: "sub", AutoCommit: true}}}); err != nil {
				b.Fatal(err)
			}
			response, err := stream.Recv()
			if err != nil || response.GetSubscribe() == nil || response.GetSubscribe().Error != nil {
				b.Fatalf("subscribe response: %v %v", response, err)
			}
			subscriptionID := response.GetSubscribe().SubscriptionId
			run = func() error {
				response, err := stream.Recv()
				if err != nil {
					return err
				}
				message := response.GetMessage()
				if message == nil || message.SubscriptionId != subscriptionID || len(message.Payload) != payloadSize {
					return fmt.Errorf("invalid subscribe response")
				}
				return nil
			}
		}
	case benchmarkAck, benchmarkNack:
		if err := stream.Send(&pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: 1, Route: "sub", AutoCommit: false, BatchSize: uint32(batchSize)}}}); err != nil {
			b.Fatal(err)
		}
		response, err := stream.Recv()
		if err != nil || response.GetFetch() == nil || response.GetFetch().Error != nil || len(response.GetFetch().Messages) != batchSize {
			b.Fatalf("ack fetch setup: %v %v", response, err)
		}
		fetch := response.GetFetch()
		seedIDs := make([][]byte, len(fetch.Messages))
		for i := range fetch.Messages {
			seedIDs[i] = fetch.Messages[i].MessageId
		}
		ids, err := cloneSessionBenchmarkSettlementIDs(seedIDs)
		if err != nil {
			b.Fatal(err)
		}
		var request *pb.FujinRequest
		if operation == benchmarkNack {
			request = &pb.FujinRequest{Request: &pb.FujinRequest_Nack{Nack: &pb.NackRequest{CorrelationId: 2, SubscriptionId: fetch.SubscriptionId, MessageIds: ids}}}
		} else {
			request = &pb.FujinRequest{Request: &pb.FujinRequest_Ack{Ack: &pb.AckRequest{CorrelationId: 2, SubscriptionId: fetch.SubscriptionId, MessageIds: ids}}}
		}
		run = func() error {
			if err := grpcRoundTrip(stream, request, func(response *pb.FujinResponse) error {
				if operation == benchmarkNack {
					value := response.GetNack()
					if value == nil || value.Error != nil || len(value.Results) != batchSize {
						return fmt.Errorf("nack response: %v", response)
					}
					for _, result := range value.Results {
						if result.Error != nil {
							return fmt.Errorf("nack result: %v", result.Error)
						}
					}
					return nil
				}
				value := response.GetAck()
				if value == nil || value.Error != nil || len(value.Results) != batchSize {
					return fmt.Errorf("ack response: %v", response)
				}
				for _, result := range value.Results {
					if result.Error != nil {
						return fmt.Errorf("ack result: %v", result.Error)
					}
				}
				return nil
			})(); err != nil {
				return err
			}
			return advanceSessionBenchmarkSettlementIDs(ids)
		}
	case benchmarkTransaction:
		begin := &pb.FujinRequest{Request: &pb.FujinRequest_BeginTx{BeginTx: &pb.BeginTxRequest{CorrelationId: 1, Route: "tx"}}}
		produce := &pb.FujinRequest{Request: &pb.FujinRequest_TxProduce{TxProduce: &pb.TxProduceRequest{CorrelationId: 2, Message: payload}}}
		commit := &pb.FujinRequest{Request: &pb.FujinRequest_CommitTx{CommitTx: &pb.CommitTxRequest{CorrelationId: 3}}}
		run = func() error {
			if err := stream.Send(begin); err != nil {
				return err
			}
			if response, err := stream.Recv(); err != nil || response.GetBeginTx() == nil || response.GetBeginTx().Error != nil {
				return fmt.Errorf("begin response: %v %v", response, err)
			}
			if err := stream.Send(produce); err != nil {
				return err
			}
			if response, err := stream.Recv(); err != nil || response.GetTxProduce() == nil || response.GetTxProduce().Error != nil {
				return fmt.Errorf("tx produce response: %v %v", response, err)
			}
			if err := stream.Send(commit); err != nil {
				return err
			}
			if response, err := stream.Recv(); err != nil || response.GetCommitTx() == nil || response.GetCommitTx().Error != nil {
				return fmt.Errorf("commit response: %v %v", response, err)
			}
			return nil
		}
	default:
		b.Fatalf("unknown operation %q", operation)
	}
	closeWorker := func() error {
		_ = stream.CloseSend()
		cancel()
		return nil
	}
	return sessionBenchmarkWorker{run: run, abort: cancel, close: closeWorker}
}

func grpcRoundTrip(stream pb.FujinService_StreamClient, request *pb.FujinRequest, validate func(*pb.FujinResponse) error) func() error {
	return func() error {
		if err := stream.Send(request); err != nil {
			return err
		}
		response, err := stream.Recv()
		if err != nil {
			return err
		}
		return validate(response)
	}
}

func validateFetchedPayloads(messages []fetchedMsg, payloadSize int, withHeaders bool) error {
	for _, message := range messages {
		if len(message.Payload) != payloadSize {
			return fmt.Errorf("payload size: got %d, want %d", len(message.Payload), payloadSize)
		}
		if withHeaders && len(message.Headers) != 2 {
			return fmt.Errorf("headers: got %d, want 2", len(message.Headers))
		}
	}
	return nil
}
func readBenchmarkSettlementResp(reader *protoReader, expectedCode v1.RespCode, scratch *[]byte) (int, error) {
	code, err := reader.readByte()
	if err != nil {
		return 0, err
	}
	if code != byte(expectedCode) {
		return 0, fmt.Errorf("expected settlement response code %d, got %d", expectedCode, code)
	}
	if _, err := reader.readCID(); err != nil {
		return 0, err
	}
	status, err := reader.readByte()
	if err != nil {
		return 0, err
	}
	if status != byte(v1.STATUS_OK) {
		return 0, fmt.Errorf("settlement response status %d", status)
	}
	count, err := reader.readUint32()
	if err != nil {
		return 0, err
	}
	for range count {
		messageIDLen, err := reader.readUint32()
		if err != nil {
			return 0, err
		}
		if cap(*scratch) < int(messageIDLen) {
			*scratch = make([]byte, int(messageIDLen))
		} else {
			*scratch = (*scratch)[:messageIDLen]
		}
		if _, err := io.ReadFull(reader.r, *scratch); err != nil {
			return 0, err
		}
		status, err := reader.readByte()
		if err != nil {
			return 0, err
		}
		if status != byte(v1.STATUS_OK) {
			return 0, fmt.Errorf("settlement result status %d", status)
		}
	}
	return int(count), nil
}

func writeBenchmarkFrame(writer io.Writer, frame []byte) error {
	for len(frame) > 0 {
		n, err := writer.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}
