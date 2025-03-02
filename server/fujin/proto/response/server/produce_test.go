package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProduceResponse(t *testing.T) {
	buf := make([]byte, 0, 7)
	msgCmd := ProduceResponse(buf, []byte{23, 43, 222, 1}, true)
	assert.EqualValues(t, []byte{5, 23, 43, 222, 1, 0}, msgCmd)
}
