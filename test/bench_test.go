package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
)

const (
	PERF_ADDR = "localhost:4848"
)

func Benchmark_Produce_1BPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", "b")
}

func Benchmark_Produce_1BPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", "b")
}

func benchProduce(b *testing.B, typ, pub, payload string) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	b.StopTimer()
	switch typ {
	case "kafka3":
		RunDefaultServerWithKafka3Brokers(ctx)
	case "nats":
		RunDefaultServerWithNats(ctx)
	default:
		panic("invalid typ")
	}
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

	b.SetBytes(int64(len(cmd)))
	bw := bufio.NewWriterSize(p, defaultSendBufSize)

	bytes := make(chan int)

	go drainStream(b, p, bytes)

	b.StartTimer()
	for b.Loop() {
		bw.Write(cmd)
	}
	bw.Write([]byte{byte(request.OP_CODE_DISCONNECT)})

	bw.Flush()
	res := <-bytes
	b.StopTimer()
	p.Close()
	_ = c.CloseWithError(0x0, "")
	expected := b.N*6 + 1
	if res != expected {
		panic(fmt.Errorf("Invalid number of bytes read: bytes: %d, expected: %d", res, expected))
	}
}
