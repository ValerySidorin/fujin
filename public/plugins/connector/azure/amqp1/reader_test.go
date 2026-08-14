package amqp1

import (
	"math"
	"testing"

	"github.com/Azure/go-amqp"
)

func TestDeliveryIdRoundTrip(t *testing.T) {
	msg := &amqp.Message{}
	SetDeliveryId(msg, 42)
	if got := GetDeliveryId(msg); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestDeliveryIdZero(t *testing.T) {
	msg := &amqp.Message{}
	SetDeliveryId(msg, 0)
	if got := GetDeliveryId(msg); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestDeliveryIdMaxUint32(t *testing.T) {
	msg := &amqp.Message{}
	SetDeliveryId(msg, math.MaxUint32)
	if got := GetDeliveryId(msg); got != math.MaxUint32 {
		t.Fatalf("expected %d, got %d", uint32(math.MaxUint32), got)
	}
}

func TestDeliveryIdOverwrite(t *testing.T) {
	msg := &amqp.Message{}
	SetDeliveryId(msg, 100)
	SetDeliveryId(msg, 200)
	if got := GetDeliveryId(msg); got != 200 {
		t.Fatalf("expected 200, got %d", got)
	}
}

func TestDeliveryIdDoesNotCorruptMessage(t *testing.T) {
	msg := &amqp.Message{
		Data:                  [][]byte{[]byte("hello")},
		ApplicationProperties: map[string]any{"key": "value"},
	}

	SetDeliveryId(msg, 12345)

	if string(msg.Data[0]) != "hello" {
		t.Fatalf("Data corrupted: got %q", msg.Data[0])
	}
	if msg.ApplicationProperties["key"] != "value" {
		t.Fatalf("ApplicationProperties corrupted")
	}
	if got := GetDeliveryId(msg); got != 12345 {
		t.Fatalf("expected 12345, got %d", got)
	}
}

func TestDeliveryIdIndependentPerMessage(t *testing.T) {
	msg1 := &amqp.Message{}
	msg2 := &amqp.Message{}

	SetDeliveryId(msg1, 1)
	SetDeliveryId(msg2, 2)

	if got := GetDeliveryId(msg1); got != 1 {
		t.Fatalf("msg1: expected 1, got %d", got)
	}
	if got := GetDeliveryId(msg2); got != 2 {
		t.Fatalf("msg2: expected 2, got %d", got)
	}
}
