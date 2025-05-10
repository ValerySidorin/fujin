package fujin_test

import (
	"testing"

	"github.com/ValerySidorin/fujin/internal/server/fujin"
	"github.com/stretchr/testify/assert"
)

func TestBufPool(t *testing.T) {
	bufs := fujin.GetBufs()
	assert.Equal(t, 0, cap(bufs))

	bufs = append(bufs, []byte{})
	bufs = append(bufs, []byte{})
	assert.Equal(t, 2, cap(bufs))

	fujin.PutBufs(bufs)

	bufs2 := fujin.GetBufs()
	assert.Equal(t, 2, cap(bufs2))
}
