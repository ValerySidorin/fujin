package fujin_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/fujin"
)

var benchmarkPayloadSizes = []int{1, 128, 1024, 32 * 1024, 1024 * 1024}
var benchmarkConcurrency = []int{1, 16, 128}

func BenchmarkNativeProduce(b *testing.B) {
	server := startNativeTestServer(b)
	conn, err := fujin.Dial(context.Background(), server.addr(), &tls.Config{InsecureSkipVerify: true}, nil)
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
