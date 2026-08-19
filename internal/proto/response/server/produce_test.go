package server

import (
	"testing"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/stretchr/testify/assert"
)

func TestProduceResponseSuccess(t *testing.T) {
	buf := make([]byte, 0, 6)
	msgCmd := ProduceResponseSuccess(buf, []byte{23, 43, 222, 1})
	assert.EqualValues(t, []byte{byte(v1.RESP_CODE_PRODUCE), 23, 43, 222, 1, 0}, msgCmd)
}

func TestProduceResponseSuccess_EmptyCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 2)
	msgCmd := ProduceResponseSuccess(buf, []byte{})
	expected := []byte{byte(v1.RESP_CODE_PRODUCE), byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
	assert.Equal(t, 2, len(msgCmd))
}

func TestProduceResponseSuccess_NilBuffer(t *testing.T) {
	var buf []byte
	msgCmd := ProduceResponseSuccess(buf, []byte{1, 2, 3})
	expected := []byte{byte(v1.RESP_CODE_PRODUCE), 1, 2, 3, byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_PrefilledBuffer(t *testing.T) {
	buf := []byte{10, 20, 30}
	msgCmd := ProduceResponseSuccess(buf, []byte{5, 6})
	expected := []byte{10, 20, 30, byte(v1.RESP_CODE_PRODUCE), 5, 6, byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_SingleByteCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 3)
	msgCmd := ProduceResponseSuccess(buf, []byte{255})
	expected := []byte{byte(v1.RESP_CODE_PRODUCE), 255, byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_LargeCorrelationID(t *testing.T) {
	cID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := make([]byte, 0, 12)
	msgCmd := ProduceResponseSuccess(buf, cID)

	assert.Equal(t, 12, len(msgCmd))

	assert.Equal(t, byte(v1.RESP_CODE_PRODUCE), msgCmd[0])
	assert.EqualValues(t, cID, msgCmd[1:11])
	assert.Equal(t, byte(v1.STATUS_OK), msgCmd[11])
}

func TestProduceResponseSuccess_ResponseStructure(t *testing.T) {
	buf := make([]byte, 0)
	cID := []byte{100, 200}
	msgCmd := ProduceResponseSuccess(buf, cID)

	assert.Equal(t, 4, len(msgCmd), "Response should have 4 bytes")
	assert.Equal(t, byte(v1.RESP_CODE_PRODUCE), msgCmd[0], "First byte should be RESP_CODE_PRODUCE")
	assert.Equal(t, byte(100), msgCmd[1], "Second byte should be first cID byte")
	assert.Equal(t, byte(200), msgCmd[2], "Third byte should be second cID byte")
	assert.Equal(t, byte(v1.STATUS_OK), msgCmd[3], "Last byte should be STATUS_OK")
}

func TestProduceResponseSuccess_OriginalBufferUnchanged(t *testing.T) {
	original := make([]byte, 2)
	original[0] = 99
	original[1] = 88

	msgCmd := ProduceResponseSuccess(original, []byte{1, 2, 3})

	assert.Equal(t, byte(99), original[0])
	assert.Equal(t, byte(88), original[1])
	assert.Equal(t, 2, len(original))

	assert.NotEqual(t, len(original), len(msgCmd))
}

func TestProduceResponseSuccess_ZeroByteCorrelationID(t *testing.T) {
	buf := make([]byte, 0, 3)
	msgCmd := ProduceResponseSuccess(buf, []byte{0})
	expected := []byte{byte(v1.RESP_CODE_PRODUCE), 0, byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
}

func TestProduceResponseSuccess_AllZerosCorrelationID(t *testing.T) {
	buf := make([]byte, 0)
	cID := []byte{0, 0, 0, 0}
	msgCmd := ProduceResponseSuccess(buf, cID)
	expected := []byte{byte(v1.RESP_CODE_PRODUCE), 0, 0, 0, 0, byte(v1.STATUS_OK)}
	assert.EqualValues(t, expected, msgCmd)
}
