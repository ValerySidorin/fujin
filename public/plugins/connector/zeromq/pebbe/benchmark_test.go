//go:build zeromq_pebbe && cgo

package pebbe

import (
	"fmt"
	"testing"
)

func BenchmarkFujinV1Framing(b *testing.B) {
	for _, size := range []int{128, 32 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			route := routeConfig{name: "route", CommonSettings: CommonSettings{MaxMessageBytes: 4 << 20}, RouteSettings: RouteSettings{Pattern: PatternPush, Framing: FramingFujinV1}}
			payload := make([]byte, size)
			headers := [][]byte{[]byte("kind"), []byte("benchmark"), []byte("trace"), []byte("0123456789")}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				frames, err := encodeMessage(route, payload, headers, true)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := decodeMessage(route, frames); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
