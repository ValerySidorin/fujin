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

func TestProduceResponseSuccess_EmptyCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 2)
	msgCmd := ProduceResponseSuccess(buf, []byte{})
	expected := []byte{byte(response.RESP_CODE_PRODUCE), response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
	assert.Equal(t, 2, len(msgCmd))
}

func TestProduceResponseSuccess_NilBuffer(t *testing.T) {
	var buf []byte
	msgCmd := ProduceResponseSuccess(buf, []byte{1, 2, 3})
	expected := []byte{byte(response.RESP_CODE_PRODUCE), 1, 2, 3, response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_PrefilledBuffer(t *testing.T) {
	buf := []byte{10, 20, 30}
	msgCmd := ProduceResponseSuccess(buf, []byte{5, 6})
	expected := []byte{10, 20, 30, byte(response.RESP_CODE_PRODUCE), 5, 6, response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_SingleByteCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 3)
	msgCmd := ProduceResponseSuccess(buf, []byte{255})
	expected := []byte{byte(response.RESP_CODE_PRODUCE), 255, response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_LargeCorrelationID(t *testing.T) {
	cID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := make([]byte, 0, 12)
	msgCmd := ProduceResponseSuccess(buf, cID)

	// Check length: 1 (response code) + 10 (cID) + 1 (error code) = 12
	assert.Equal(t, 12, len(msgCmd))

	// Check structure
	assert.Equal(t, byte(response.RESP_CODE_PRODUCE), msgCmd[0])
	assert.EqualValues(t, cID, msgCmd[1:11])
	assert.Equal(t, byte(response.ERR_CODE_NO), msgCmd[11])
}

func TestProduceResponseSuccess_ResponseStructure(t *testing.T) {
	buf := make([]byte, 0)
	cID := []byte{100, 200}
	msgCmd := ProduceResponseSuccess(buf, cID)

	// Verify the structure: [RESP_CODE_PRODUCE, cID bytes..., ERR_CODE_NO]
	assert.Equal(t, 4, len(msgCmd), "Response should have 4 bytes")
	assert.Equal(t, byte(response.RESP_CODE_PRODUCE), msgCmd[0], "First byte should be RESP_CODE_PRODUCE")
	assert.Equal(t, byte(100), msgCmd[1], "Second byte should be first cID byte")
	assert.Equal(t, byte(200), msgCmd[2], "Third byte should be second cID byte")
	assert.Equal(t, byte(response.ERR_CODE_NO), msgCmd[3], "Last byte should be ERR_CODE_NO")
}

func TestProduceResponseSuccess_OriginalBufferUnchanged(t *testing.T) {
	// Create a buffer with insufficient capacity to test that original is not modified
	original := make([]byte, 2)
	original[0] = 99
	original[1] = 88

	msgCmd := ProduceResponseSuccess(original, []byte{1, 2, 3})

	// Original buffer should still have its original values
	assert.Equal(t, byte(99), original[0])
	assert.Equal(t, byte(88), original[1])
	assert.Equal(t, 2, len(original))

	// New buffer should be different
	assert.NotEqual(t, len(original), len(msgCmd))
}

func TestProduceResponseSuccess_ZeroByteCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 3)
	msgCmd := ProduceResponseSuccess(buf, []byte{0})
	expected := []byte{byte(response.RESP_CODE_PRODUCE), 0, response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_AllZerosCorrelationID(t *testing.T) {
	buf := make([]byte, 0)
	cID := []byte{0, 0, 0, 0}
	msgCmd := ProduceResponseSuccess(buf, cID)
	expected := []byte{byte(response.RESP_CODE_PRODUCE), 0, 0, 0, 0, response.ERR_CODE_NO}
	assert.EqualValues(t, expected, msgCmd)
}
