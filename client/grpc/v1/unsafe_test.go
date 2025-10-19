package v1

import (
	"testing"
)

func TestStringToBytes(t *testing.T) {
	tests := []struct {
		name string
		str  string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"unicode", "日本語"},
		{"long", "this is a longer string with multiple words and characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringToBytes(tt.str)
			if string(result) != tt.str {
				t.Errorf("stringToBytes(%q) = %q, want %q", tt.str, string(result), tt.str)
			}
		})
	}
}

func TestBytesToString(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
		want  string
	}{
		{"empty", []byte{}, ""},
		{"nil", nil, ""},
		{"simple", []byte("hello"), "hello"},
		{"unicode", []byte("日本語"), "日本語"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bytesToString(tt.bytes)
			if result != tt.want {
				t.Errorf("bytesToString(%q) = %q, want %q", tt.bytes, result, tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	original := "test string with unicode: 日本語"

	// String -> Bytes -> String
	bytes := stringToBytes(original)
	result := bytesToString(bytes)

	if result != original {
		t.Errorf("round trip failed: got %q, want %q", result, original)
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	str := "content-type: application/json; charset=utf-8"

	b.Run("unsafe", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = stringToBytes(str)
		}
	})

	b.Run("standard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = []byte(str)
		}
	})
}

func BenchmarkBytesToString(b *testing.B) {
	bytes := []byte("content-type: application/json; charset=utf-8")

	b.Run("unsafe", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = bytesToString(bytes)
		}
	})

	b.Run("standard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = string(bytes)
		}
	})
}

func BenchmarkHeaderConversion(b *testing.B) {
	headers := map[string]string{
		"content-type": "application/json",
		"timestamp":    "2025-10-19T12:00:00Z",
		"source":       "grpc-client",
		"trace-id":     "abc123def456",
		"user-agent":   "fujin/1.0",
	}

	b.Run("unsafe_conversion", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			protoHeaders := make([]struct{ Key, Value []byte }, 0, len(headers))
			for k, v := range headers {
				protoHeaders = append(protoHeaders, struct{ Key, Value []byte }{
					Key:   stringToBytes(k),
					Value: stringToBytes(v),
				})
			}
			_ = protoHeaders
		}
	})

	b.Run("standard_conversion", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			protoHeaders := make([]struct{ Key, Value []byte }, 0, len(headers))
			for k, v := range headers {
				protoHeaders = append(protoHeaders, struct{ Key, Value []byte }{
					Key:   []byte(k),
					Value: []byte(v),
				})
			}
			_ = protoHeaders
		}
	})
}
