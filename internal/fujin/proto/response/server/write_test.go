package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProduceResponseSuccess(t *testing.T) {
	buf := make([]byte, 0, 6)
	msgCmd := WriteResponseSuccess(buf, []byte{23, 43, 222, 1})
	assert.EqualValues(t, []byte{2, 23, 43, 222, 1, 0}, msgCmd)
}
