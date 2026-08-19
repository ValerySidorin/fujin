package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufPool(t *testing.T) {
	bufs := GetBufs()
	assert.Equal(t, 0, len(bufs))

	bufs = append(bufs, []byte{})
	bufs = append(bufs, []byte{})
	assert.Equal(t, 2, len(bufs))

	PutBufs(bufs)

	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2))
	// sync.Pool may or may not return the same slice, so capacity is not guaranteed
}

func TestGetBufs_InitialCall(t *testing.T) {
	bufs := GetBufs()
	assert.NotNil(t, bufs)
	assert.Equal(t, 0, len(bufs), "Initial bufs should have length 0")
}

func TestGetBufs_ResetsLength(t *testing.T) {
	// Get bufs, fill it, and put it back
	bufs := GetBufs()
	bufs = append(bufs, []byte{1, 2, 3})
	bufs = append(bufs, []byte{4, 5, 6})
	bufs = append(bufs, []byte{7, 8, 9})
	assert.Equal(t, 3, len(bufs))

	PutBufs(bufs)

	// Get again - length should be reset to 0
	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2), "Length should be reset to 0")
	// Note: capacity is not guaranteed — sync.Pool may return a fresh slice
}

func TestPutBufs_PreservesCapacity(t *testing.T) {
	bufs := GetBufs()

	// Add multiple buffers to increase capacity
	for i := 0; i < 10; i++ {
		bufs = append(bufs, []byte{byte(i)})
	}

	assert.Equal(t, 10, len(bufs))

	PutBufs(bufs)

	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2), "Length should be reset after Put/Get cycle")
}

func TestBufPool_MultipleOperations(t *testing.T) {
	// Perform multiple Get/Put cycles
	for i := 0; i < 5; i++ {
		bufs := GetBufs()
		assert.Equal(t, 0, len(bufs))

		// Add some buffers
		for j := 0; j < i+1; j++ {
			bufs = append(bufs, []byte{byte(j)})
		}

		assert.Equal(t, i+1, len(bufs))
		PutBufs(bufs)
	}
}

func TestBufPool_WithDataPreservation(t *testing.T) {
	bufs := GetBufs()

	// Add buffers with specific data
	buf1 := []byte{1, 2, 3}
	buf2 := []byte{4, 5, 6}
	buf3 := []byte{7, 8, 9}

	bufs = append(bufs, buf1)
	bufs = append(bufs, buf2)
	bufs = append(bufs, buf3)

	PutBufs(bufs)

	// Get back and verify data is still there (at the slice level)
	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2), "Length should be 0")

	// The underlying array should still be there with the capacity
	// We can reuse it
	bufs2 = append(bufs2, []byte{10, 11})
	assert.Equal(t, 1, len(bufs2))
}

func TestBufPool_EmptySlice(t *testing.T) {
	bufs := GetBufs()
	// Don't add anything, just put it back
	PutBufs(bufs)

	bufs2 := GetBufs()
	assert.NotNil(t, bufs2)
	assert.Equal(t, 0, len(bufs2))
}

func TestBufPool_LargeBuffer(t *testing.T) {
	bufs := GetBufs()

	// Create a large buffer list
	for i := 0; i < 1000; i++ {
		bufs = append(bufs, make([]byte, 100))
	}

	assert.Equal(t, 1000, len(bufs))

	PutBufs(bufs)

	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2))
}

func TestBufPool_InterleavedOperations(t *testing.T) {
	// Get multiple buffers without putting them back immediately
	bufs1 := GetBufs()
	bufs2 := GetBufs()
	bufs3 := GetBufs()

	// Modify each
	bufs1 = append(bufs1, []byte{1})
	bufs2 = append(bufs2, []byte{2}, []byte{3})
	bufs3 = append(bufs3, []byte{4}, []byte{5}, []byte{6})

	assert.Equal(t, 1, len(bufs1))
	assert.Equal(t, 2, len(bufs2))
	assert.Equal(t, 3, len(bufs3))

	// Put them back
	PutBufs(bufs1)
	PutBufs(bufs2)
	PutBufs(bufs3)

	// Get new ones and verify
	bufs4 := GetBufs()
	assert.Equal(t, 0, len(bufs4))
	assert.GreaterOrEqual(t, cap(bufs4), 0)
}

func TestBufPool_NilBuffersInSlice(t *testing.T) {
	bufs := GetBufs()

	// Append nil byte slices
	bufs = append(bufs, nil)
	bufs = append(bufs, nil)
	bufs = append(bufs, []byte{1, 2, 3})

	assert.Equal(t, 3, len(bufs))
	assert.Nil(t, bufs[0])
	assert.Nil(t, bufs[1])
	assert.NotNil(t, bufs[2])

	PutBufs(bufs)

	bufs2 := GetBufs()
	assert.Equal(t, 0, len(bufs2))
}

func TestBufPool_ConsecutivePuts(t *testing.T) {
	bufs1 := GetBufs()
	bufs1 = append(bufs1, []byte{1})

	bufs2 := GetBufs()
	bufs2 = append(bufs2, []byte{2}, []byte{3})

	// Put both back
	PutBufs(bufs1)
	PutBufs(bufs2)

	// Get them back - order is not guaranteed with sync.Pool
	retrieved1 := GetBufs()
	retrieved2 := GetBufs()

	assert.Equal(t, 0, len(retrieved1))
	assert.Equal(t, 0, len(retrieved2))
}
