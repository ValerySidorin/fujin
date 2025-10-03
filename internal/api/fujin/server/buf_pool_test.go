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
