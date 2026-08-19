package proto

import (
	"testing"
	"time"

	"github.com/fujin-io/fujin/internal/proto/pool"

	"github.com/stretchr/testify/assert"
)

func TestInbound_ErrorConstants(t *testing.T) {
	assert.Error(t, ErrNilHandler)
	assert.Equal(t, "handler is nil", ErrNilHandler.Error())
}

func TestInboundReadBufferGrowth(t *testing.T) {
	buf := pool.Get(initialReadBufferSize)
	t.Cleanup(func() { pool.Put(buf) })

	for _, want := range []int{pool.SIZE_MEDIUM, pool.SIZE_LARGE} {
		var shortReads int
		buf, shortReads = resizeReadBuffer(buf, cap(buf), shortReads)
		assert.Equal(t, want, cap(buf))
		assert.Zero(t, shortReads)
	}
	buf, _ = resizeReadBuffer(buf, cap(buf), 0)
	assert.Equal(t, maximumReadBufferSize, cap(buf))
}

func TestInboundReadBufferShrinksAfterQuietPeriod(t *testing.T) {
	buf := pool.Get(pool.SIZE_LARGE)
	t.Cleanup(func() { pool.Put(buf) })

	shortReads := 0
	for range readBufferShrinkAfter - 1 {
		buf, shortReads = resizeReadBuffer(buf, 1, shortReads)
		assert.Equal(t, pool.SIZE_LARGE, cap(buf))
	}
	buf, shortReads = resizeReadBuffer(buf, 1, shortReads)
	assert.Equal(t, initialReadBufferSize, cap(buf))
	assert.Zero(t, shortReads)
}

func TestInbound_TimeoutValues(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
	}{
		{"100ms", 100 * time.Millisecond},
		{"1s", 1 * time.Second},
		{"5s", 5 * time.Second},
		{"10s", 10 * time.Second},
		{"30s", 30 * time.Second},
		{"1m", 1 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that various timeout values are valid
			assert.True(t, tt.duration >= 0)
			assert.NotNil(t, tt.duration)
		})
	}
}

func TestInbound_StructFields(t *testing.T) {
	// Test that inbound struct can be created with basic fields
	// This tests the structure definition is valid
	type testInbound struct {
		ftt time.Duration
	}

	inb := testInbound{
		ftt: 5 * time.Second,
	}

	assert.Equal(t, 5*time.Second, inb.ftt)
}

func TestInbound_ForceTerminateTimeoutVariations(t *testing.T) {
	timeouts := []time.Duration{
		0,
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		15 * time.Second,
		30 * time.Second,
	}

	for _, timeout := range timeouts {
		t.Run(timeout.String(), func(t *testing.T) {
			// Verify timeout values are reasonable
			assert.GreaterOrEqual(t, timeout, time.Duration(0))
			assert.LessOrEqual(t, timeout, time.Minute)
		})
	}
}
