package nativeproto

import (
	"bytes"
	"fmt"
	"testing"

	client "github.com/fujin-io/fujin/sdk/go/client"
)

var helloFrameSink []byte

func BenchmarkHelloFrameConstruct(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		helloFrameSink = Hello("fujin-go", "v1.2.3", Version)
	}
}

func BenchmarkHelloFrameReuse(b *testing.B) {
	frame := Hello("fujin-go", "v1.2.3", Version)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if len(frame) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkHelloResponse(b *testing.B) {
	response := []byte{RespHello, 0, HelloFormat, byte(Version)}
	response = AppendString(response, "v0.5.0")
	source := bytes.NewReader(response)
	reader := NewReader(source)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		source.Reset(response)
		reader.r.Reset(source)
		if _, err := reader.HelloResponse(); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkProduceFrame(b *testing.B) {
	for _, size := range []int{1, 128, 1024, 32 * 1024, 1024 * 1024} {
		payload := make([]byte, size)
		b.Run(fmt.Sprintf("produce/payload=%dB", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				frame := Produce(OpProduce, 1, "pub", payload, nil)
				if len(frame) == 0 {
					b.Fatal("empty frame")
				}
			}
		})
	}
	payload := make([]byte, 128)
	headers := []client.Header{
		{Key: []byte("content-type"), Value: []byte("application/octet-stream")},
		{Key: []byte("tenant"), Value: []byte("performance")},
		{Key: []byte("trace-id"), Value: []byte("0123456789abcdef")},
		{Key: []byte("source"), Value: []byte("benchmark")},
	}
	b.Run("hproduce/payload=128B/headers=4", func(b *testing.B) {
		b.SetBytes(128)
		b.ReportAllocs()
		for b.Loop() {
			frame := Produce(OpHProduce, 1, "pub", payload, headers)
			if len(frame) == 0 {
				b.Fatal("empty frame")
			}
		}
	})
}
