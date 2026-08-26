//go:build !race

package nativeproto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHelloResponseDoesNotAllocate(t *testing.T) {
	response := []byte{RespHello, 0, HelloFormat, byte(Version)}
	response = AppendString(response, "v0.5.0")
	source := bytes.NewReader(response)
	reader := NewReader(source)

	allocations := testing.AllocsPerRun(1000, func() {
		source.Reset(response)
		reader.r.Reset(source)
		if _, err := reader.HelloResponse(); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations)
}
