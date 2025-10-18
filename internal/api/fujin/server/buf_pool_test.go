package server_test

import (
	"testing"

	"github.com/ValerySidorin/fujin/internal/api/fujin/server"
	"github.com/stretchr/testify/assert"
)

func TestBufPool(t *testing.T) {
	bufs := server.GetBufs()
	assert.Equal(t, 0, cap(bufs))

	bufs = append(bufs, []byte{})
	bufs = append(bufs, []byte{})
	assert.Equal(t, 2, cap(bufs))

	server.PutBufs(bufs)

	bufs2 := server.GetBufs()
	assert.Equal(t, 2, cap(bufs2))
}

func TestGetBufs_InitialCall(t *testing.T) {
	bufs := server.GetBufs()
	assert.NotNil(t, bufs)
	assert.Equal(t, 0, len(bufs), "Initial bufs should have length 0")
}

func TestGetBufs_ResetsLength(t *testing.T) {
	// Get bufs, fill it, and put it back
	bufs := server.GetBufs()
	bufs = append(bufs, []byte{1, 2, 3})
	bufs = append(bufs, []byte{4, 5, 6})
	bufs = append(bufs, []byte{7, 8, 9})
	assert.Equal(t, 3, len(bufs))

	server.PutBufs(bufs)

	// Get again - length should be reset to 0
	bufs2 := server.GetBufs()
	assert.Equal(t, 0, len(bufs2), "Length should be reset to 0")
	assert.GreaterOrEqual(t, cap(bufs2), 3, "Capacity should be preserved")
}

func TestPutBufs_PreservesCapacity(t *testing.T) {
	bufs := server.GetBufs()

	// Add multiple buffers to increase capacity
	for i := 0; i < 10; i++ {
		bufs = append(bufs, []byte{byte(i)})
	}

	originalCap := cap(bufs)
	assert.GreaterOrEqual(t, originalCap, 10)

	server.PutBufs(bufs)

	bufs2 := server.GetBufs()
	assert.Equal(t, originalCap, cap(bufs2), "Capacity should be preserved after Put/Get cycle")
}

func TestBufPool_MultipleOperations(t *testing.T) {
	// Perform multiple Get/Put cycles
	for i := 0; i < 5; i++ {
		bufs := server.GetBufs()
		assert.Equal(t, 0, len(bufs))

		// Add some buffers
		for j := 0; j < i+1; j++ {
			bufs = append(bufs, []byte{byte(j)})
		}

		assert.Equal(t, i+1, len(bufs))
		server.PutBufs(bufs)
	}
}

func TestBufPool_WithDataPreservation(t *testing.T) {
	bufs := server.GetBufs()

	// Add buffers with specific data
	buf1 := []byte{1, 2, 3}
	buf2 := []byte{4, 5, 6}
	buf3 := []byte{7, 8, 9}

	bufs = append(bufs, buf1)
	bufs = append(bufs, buf2)
	bufs = append(bufs, buf3)

	server.PutBufs(bufs)

	// Get back and verify data is still there (at the slice level)
	bufs2 := server.GetBufs()
	assert.Equal(t, 0, len(bufs2), "Length should be 0")

	// The underlying array should still be there with the capacity
	// We can reuse it
	bufs2 = append(bufs2, []byte{10, 11})
	assert.Equal(t, 1, len(bufs2))
}

func TestBufPool_EmptySlice(t *testing.T) {
	bufs := server.GetBufs()
	// Don't add anything, just put it back
	server.PutBufs(bufs)

	bufs2 := server.GetBufs()
	assert.NotNil(t, bufs2)
	assert.Equal(t, 0, len(bufs2))
}

func TestBufPool_LargeBuffer(t *testing.T) {
	bufs := server.GetBufs()

	// Create a large buffer list
	for i := 0; i < 1000; i++ {
		bufs = append(bufs, make([]byte, 100))
	}

	assert.Equal(t, 1000, len(bufs))
	originalCap := cap(bufs)

	server.PutBufs(bufs)

	bufs2 := server.GetBufs()
	assert.Equal(t, 0, len(bufs2))
	assert.Equal(t, originalCap, cap(bufs2))
}

func TestBufPool_InterleavedOperations(t *testing.T) {
	// Get multiple buffers without putting them back immediately
	bufs1 := server.GetBufs()
	bufs2 := server.GetBufs()
	bufs3 := server.GetBufs()

	// Modify each
	bufs1 = append(bufs1, []byte{1})
	bufs2 = append(bufs2, []byte{2}, []byte{3})
	bufs3 = append(bufs3, []byte{4}, []byte{5}, []byte{6})

	assert.Equal(t, 1, len(bufs1))
	assert.Equal(t, 2, len(bufs2))
	assert.Equal(t, 3, len(bufs3))

	// Put them back
	server.PutBufs(bufs1)
	server.PutBufs(bufs2)
	server.PutBufs(bufs3)

	// Get new ones and verify
	bufs4 := server.GetBufs()
	assert.Equal(t, 0, len(bufs4))
	assert.GreaterOrEqual(t, cap(bufs4), 0)
}

func TestBufPool_NilBuffersInSlice(t *testing.T) {
	bufs := server.GetBufs()

	// Append nil byte slices
	bufs = append(bufs, nil)
	bufs = append(bufs, nil)
	bufs = append(bufs, []byte{1, 2, 3})

	assert.Equal(t, 3, len(bufs))
	assert.Nil(t, bufs[0])
	assert.Nil(t, bufs[1])
	assert.NotNil(t, bufs[2])

	server.PutBufs(bufs)

	bufs2 := server.GetBufs()
	assert.Equal(t, 0, len(bufs2))
}

func TestBufPool_ConsecutivePuts(t *testing.T) {
	bufs1 := server.GetBufs()
	bufs1 = append(bufs1, []byte{1})

	bufs2 := server.GetBufs()
	bufs2 = append(bufs2, []byte{2}, []byte{3})

	// Put both back
	server.PutBufs(bufs1)
	server.PutBufs(bufs2)

	// Get them back - order is not guaranteed with sync.Pool
	retrieved1 := server.GetBufs()
	retrieved2 := server.GetBufs()

	assert.Equal(t, 0, len(retrieved1))
	assert.Equal(t, 0, len(retrieved2))
}
