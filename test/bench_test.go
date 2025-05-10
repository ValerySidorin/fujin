package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/public/server"
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

// ArtemisMQ benchmarks
func Benchmark_Produce_1BPayload_ArtemisMQ(b *testing.B) {
	benchProduce(b, "artemismq", "pub", sizedString(1))
}

func Benchmark_Produce_32KBPayload_ArtemisMQ(b *testing.B) {
	benchProduce(b, "artemismq", "pub", sizedString(32*1024))
}

// Redis Pub/Sub benchmarks
func Benchmark_Produce_1BPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_RedisPubSub(b *testing.B) {
	benchProduce(b, "resp_pubsub", "pub", sizedString(32*1024))
}

// Redis Streams benchmarks
func Benchmark_Produce_1BPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_RedisStreams(b *testing.B) {
	benchProduce(b, "resp_streams", "pub", sizedString(32*1024))
}

// MQTT benchmarks
func Benchmark_Produce_1BPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_MQTT(b *testing.B) {
	benchProduce(b, "mqtt", "pub", sizedString(32*1024))
}

// NSQ benchmarks
func Benchmark_Produce_1BPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_NSQ(b *testing.B) {
	benchProduce(b, "nsq", "pub", sizedString(32*1024))
}

func benchProduce(b *testing.B, typ, pub, payload string) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	var s *server.Server

	b.StopTimer()
	switch typ {
	case "kafka3":
		s = RunDefaultServerWithKafka3Brokers(ctx)
	case "nats":
		s = RunDefaultServerWithNats(ctx)
	case "rabbitmq":
		s = RunDefaultServerWithAMQP091(ctx)
	case "artemismq":
		s = RunDefaultServerWithAMQP10(ctx)
	case "resp_pubsub":
		s = RunDefaultServerWithRedisPubSub(ctx)
	case "resp_streams":
		s = RunDefaultServerWithRedisStreams(ctx)
	case "mqtt":
		s = RunDefaultServerWithMQTT(ctx)
	case "nsq":
		s = RunDefaultServerWithNSQ(ctx)
	default:
		panic("invalid typ")
	}

	defer func() {
		cancel()
		<-s.Done()
	}()

	c := createClientConn(ctx, PERF_ADDR)
	p := doDefaultConnectProducer(c)

	cmd := []byte{
		byte(request.OP_CODE_WRITE),
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
