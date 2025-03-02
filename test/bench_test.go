package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"testing"

	"github.com/ValerySidorin/fujin/server"
	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
)

const (
	PERF_ADDR = "localhost:4848"
)

func runBenchServerWithKafka(ctx context.Context) *server.Server {
	return RunDefaultServerWithKafka(ctx)
}

func benchProduceWithKafka(b *testing.B, pub, payload string, batchNum int) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	b.StopTimer()
	runBenchServerWithKafka(ctx)
	c := createClientConn(ctx, PERF_ADDR)
	p := doDefaultConnectProducer(c)

	cmd := []byte{
		byte(request.OP_CODE_PRODUCE),
		0, 0, 0, 0,
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(pub)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(pub)...)

	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(payload)...)

	buf := make([]byte, 0)
	for range batchNum {
		buf = append(buf, cmd...)
	}

	b.SetBytes(int64(len(buf)))
	bw := bufio.NewWriterSize(p, defaultSendBufSize)

	ch := make(chan struct{})

	b.StartTimer()
	for b.Loop() {
		bw.Write(buf)
	}
	go drainStream(b, p, ch, b.N*6)
	bw.Flush()
	<-ch
	b.StopTimer()
	disconnect(p)
	_ = c.CloseWithError(0x0, "")
}

func Benchmark_Produce_1BPayload_1NBatch_Kafka(b *testing.B) {
	benchProduceWithKafka(b, "pub", "b", 1)
}
