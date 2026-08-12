package jetstream

import (
	"encoding/binary"
	"testing"
)

func TestEncodeMsgID(t *testing.T) {
	r := &Reader{}

	var seq uint64 = 42
	topic := "test.subject"

	buf := r.EncodeMsgID(nil, topic, seq)

	if len(buf) != 8+len(topic) {
		t.Fatalf("expected length %d, got %d", 8+len(topic), len(buf))
	}

	gotSeq := binary.BigEndian.Uint64(buf[:8])
	if gotSeq != seq {
		t.Fatalf("expected seq %d, got %d", seq, gotSeq)
	}

	gotTopic := string(buf[8:])
	if gotTopic != topic {
		t.Fatalf("expected topic %q, got %q", topic, gotTopic)
	}
}

func TestEncodeMsgID_LargeSequence(t *testing.T) {
	r := &Reader{}

	var seq uint64 = 1<<63 - 1
	topic := "orders.created"

	buf := r.EncodeMsgID(nil, topic, seq)

	gotSeq := binary.BigEndian.Uint64(buf[:8])
	if gotSeq != seq {
		t.Fatalf("expected seq %d, got %d", seq, gotSeq)
	}

	gotTopic := string(buf[8:])
	if gotTopic != topic {
		t.Fatalf("expected topic %q, got %q", topic, gotTopic)
	}
}

func TestEncodeMsgID_AppendToBuf(t *testing.T) {
	r := &Reader{}

	prefix := []byte{0xAA, 0xBB}
	var seq uint64 = 100
	topic := "foo"

	buf := r.EncodeMsgID(prefix, topic, seq)

	if buf[0] != 0xAA || buf[1] != 0xBB {
		t.Fatal("prefix was not preserved")
	}

	gotSeq := binary.BigEndian.Uint64(buf[2:10])
	if gotSeq != seq {
		t.Fatalf("expected seq %d, got %d", seq, gotSeq)
	}
}

func TestMsgIDArgsLen(t *testing.T) {
	r := &Reader{}
	if r.MsgIDArgsLen() != 8 {
		t.Fatalf("expected 8, got %d", r.MsgIDArgsLen())
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222", Stream: "ORDERS"},
				Routes: map[string]RouteSettings{
					"c1": {Subject: "orders.>"},
				},
			},
		},
		{
			name: "missing url",
			config: Config{
				Common: CommonSettings{Stream: "ORDERS"},
				Routes: map[string]RouteSettings{
					"c1": {Subject: "orders.>"},
				},
			},
			wantErr: true,
		},
		{
			name: "missing stream",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222"},
				Routes: map[string]RouteSettings{
					"c1": {Subject: "orders.>"},
				},
			},
			wantErr: true,
		},
		{
			name: "no routes",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222", Stream: "ORDERS"},
				Routes: map[string]RouteSettings{},
			},
			wantErr: true,
		},
		{
			name: "missing subject",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222", Stream: "ORDERS"},
				Routes: map[string]RouteSettings{
					"c1": {},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid ack_wait",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222", Stream: "ORDERS"},
				Routes: map[string]RouteSettings{
					"c1": {Subject: "orders.>", AckWait: "not-a-duration"},
				},
			},
			wantErr: true,
		},
		{
			name: "valid ack_wait",
			config: Config{
				Common: CommonSettings{URL: "nats://localhost:4222", Stream: "ORDERS"},
				Routes: map[string]RouteSettings{
					"c1": {Subject: "orders.>", AckWait: "10s"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAckWaitDuration(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		conf := ConnectorConfig{}
		if d := conf.ackWaitDuration(); d != 30*1e9 {
			t.Fatalf("expected 30s, got %v", d)
		}
	})

	t.Run("custom", func(t *testing.T) {
		conf := ConnectorConfig{
			RouteSettings: RouteSettings{AckWait: "5s"},
		}
		if d := conf.ackWaitDuration(); d != 5*1e9 {
			t.Fatalf("expected 5s, got %v", d)
		}
	})
}
