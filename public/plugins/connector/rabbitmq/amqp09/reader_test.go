package amqp09

import (
	"encoding/binary"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAutoCommit(t *testing.T) {
	r := &Reader{autoCommit: true}
	if !r.AutoCommit() {
		t.Fatal("expected autoCommit=true")
	}

	r2 := &Reader{autoCommit: false}
	if r2.AutoCommit() {
		t.Fatal("expected autoCommit=false")
	}
}

func TestMsgIDArgsLen(t *testing.T) {
	r := &Reader{}
	if r.MsgIDArgsLen() != 8 {
		t.Fatalf("expected 8, got %d", r.MsgIDArgsLen())
	}
}

func TestEncodeMsgID(t *testing.T) {
	r := &Reader{}
	buf := r.EncodeMsgID(nil, "topic", uint64(42))
	if len(buf) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(buf))
	}
	got := binary.BigEndian.Uint64(buf)
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestEncodeMsgIDAppends(t *testing.T) {
	r := &Reader{}
	prefix := []byte{0xFF}
	buf := r.EncodeMsgID(prefix, "topic", uint64(1))
	if len(buf) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(buf))
	}
	if buf[0] != 0xFF {
		t.Fatal("prefix byte corrupted")
	}
	got := binary.BigEndian.Uint64(buf[1:])
	if got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
}

func TestCloseNilChannelAndConn(t *testing.T) {
	r := &Reader{}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubscribeReaderBusyUnlocks(t *testing.T) {
	// Simulate a reader that already has a channel open.
	// The bug was: Lock taken, "reader busy" returned without Unlock → deadlock.
	// After fix: Unlock before return, so a second call must not deadlock.
	r := &Reader{}
	// Fake a non-nil channel to trigger "reader busy"
	r.channel = &amqp.Channel{}

	err := r.Subscribe(nil, func() error { return nil }, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	// If the lock was not released, this would deadlock.
	err = r.Subscribe(nil, func() error { return nil }, nil)
	if err == nil {
		t.Fatal("expected error on second call")
	}
}

func TestSubscribeWithHeadersReaderBusyUnlocks(t *testing.T) {
	r := &Reader{}
	r.channel = &amqp.Channel{}

	err := r.SubscribeWithHeaders(nil, func() error { return nil }, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	// Must not deadlock.
	err = r.SubscribeWithHeaders(nil, func() error { return nil }, nil)
	if err == nil {
		t.Fatal("expected error on second call")
	}
}
