package server

import (
	"testing"

	"github.com/ValerySidorin/fujin/internal/api/fujin/proto/response"
	"github.com/stretchr/testify/assert"
)

func TestProduceResponseSuccess(t *testing.T) {
	buf := make([]byte, 0, 6)
	msgCmd := ProduceResponseSuccess(buf, []byte{23, 43, 222, 1})
	assert.EqualValues(t, []byte{byte(response.RESP_CODE_PRODUCE), 23, 43, 222, 1, 0}, msgCmd)
}
