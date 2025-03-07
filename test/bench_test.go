package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
)

const (
	PERF_ADDR = "localhost:4848"
)

// Kafka benchmarks
func Benchmark_Produce_1BPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Kafka_3Brokers(b *testing.B) {
	benchProduce(b, "kafka3", "pub", sizedString(32*1024))
}

// Nats benchmarks
func Benchmark_Produce_1BPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Nats(b *testing.B) {
	benchProduce(b, "nats", "pub", sizedString(32*1024))
}

// RabbitMQ benchmarks
func Benchmark_Produce_1BPayload_RabbitMQ(b *testing.B) {
	benchProduce(b, "rabbitmq", "pub", sizedString(1))
}

func Benchmark_Produce_32KBPayload_RabbitMQ(b *testing.B) {
	benchProduce(b, "rabbitmq", "pub", sizedString(32*1024))
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
	case "rabbitmq":
		RunDefaultServerWithAMQP091(ctx)
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

var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedBytes(sz int) []byte {
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

func sizedString(sz int) string {
	return string(sizedBytes(sz))
}
