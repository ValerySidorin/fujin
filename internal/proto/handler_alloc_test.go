//go:build !race

package proto

import (
	"testing"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleHelloDoesNotAllocateAfterPoolWarmup(t *testing.T) {
	harness := newUnnegotiatedProtocolTestHarness()
	frame := buildHelloFrame("fujin-go", "v-client", v1.Version)
	require.NoError(t, harness.feed(frame))
	resetHelloBenchmarkHarness(harness)

	allocations := testing.AllocsPerRun(1000, func() {
		if err := harness.feed(frame); err != nil {
			panic(err)
		}
		resetHelloBenchmarkHarness(harness)
	})
	assert.Zero(t, allocations)
}
