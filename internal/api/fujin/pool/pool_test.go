package pool

import (
	"testing"
)

func TestGet_SingleByte(t *testing.T) {
	b := Get(SIZE_BYTE)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_BYTE {
		t.Errorf("Expected capacity %d, got %d", SIZE_BYTE, cap(b))
	}
}

func TestGet_4Byte(t *testing.T) {
	b := Get(SIZE_4_BYTE)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_4_BYTE {
		t.Errorf("Expected capacity %d, got %d", SIZE_4_BYTE, cap(b))
	}
}

func TestGet_Tiny(t *testing.T) {
	b := Get(SIZE_TINY)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_TINY {
		t.Errorf("Expected capacity %d, got %d", SIZE_TINY, cap(b))
	}
}

func TestGet_Small(t *testing.T) {
	b := Get(SIZE_SMALL)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_SMALL {
		t.Errorf("Expected capacity %d, got %d", SIZE_SMALL, cap(b))
	}
}

func TestGet_Medium(t *testing.T) {
	b := Get(SIZE_MEDIUM)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_MEDIUM {
		t.Errorf("Expected capacity %d, got %d", SIZE_MEDIUM, cap(b))
	}
}

func TestGet_Large(t *testing.T) {
	b := Get(SIZE_LARGE)
	if len(b) != 0 {
		t.Errorf("Expected length 0, got %d", len(b))
	}
	if cap(b) != SIZE_LARGE {
		t.Errorf("Expected capacity %d, got %d", SIZE_LARGE, cap(b))
	}
}

func TestGet_SizeBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		size        int
		expectedCap int
	}{
		{"zero size", 0, SIZE_BYTE},
		{"one byte", 1, SIZE_BYTE},
		{"two bytes", 2, SIZE_4_BYTE},
		{"four bytes", 4, SIZE_4_BYTE},
		{"five bytes", 5, SIZE_TINY},
		{"boundary tiny", SIZE_TINY, SIZE_TINY},
		{"boundary tiny plus one", SIZE_TINY + 1, SIZE_SMALL},
		{"boundary small", SIZE_SMALL, SIZE_SMALL},
		{"boundary small plus one", SIZE_SMALL + 1, SIZE_MEDIUM},
		{"boundary medium", SIZE_MEDIUM, SIZE_MEDIUM},
		{"boundary medium plus one", SIZE_MEDIUM + 1, SIZE_LARGE},
		{"boundary large", SIZE_LARGE, SIZE_LARGE},
		{"larger than large", SIZE_LARGE + 1, SIZE_LARGE},
		{"very large", 1000000, SIZE_LARGE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Get(tt.size)
			if cap(b) != tt.expectedCap {
				t.Errorf("Get(%d): expected capacity %d, got %d", tt.size, tt.expectedCap, cap(b))
			}
			if len(b) != 0 {
				t.Errorf("Get(%d): expected length 0, got %d", tt.size, len(b))
			}
		})
	}
}

func TestPut_AllSizes(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
	}{
		{"single byte", SIZE_BYTE},
		{"4 byte", SIZE_4_BYTE},
		{"tiny", SIZE_TINY},
		{"small", SIZE_SMALL},
		{"medium", SIZE_MEDIUM},
		{"large", SIZE_LARGE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Get(tt.capacity)
			if cap(b) != tt.capacity {
				t.Fatalf("Get(%d) returned buffer with capacity %d", tt.capacity, cap(b))
			}
			// Put should not panic
			Put(b)
		})
	}
}

func TestPut_NonMatchingCapacity(t *testing.T) {
	// Create a slice with a non-standard capacity
	b := make([]byte, 0, 100)
	// Put should not panic even with non-matching capacity (default case)
	Put(b)
}

func TestGetPut_RoundTrip(t *testing.T) {
	sizes := []int{SIZE_BYTE, SIZE_4_BYTE, SIZE_TINY, SIZE_SMALL, SIZE_MEDIUM, SIZE_LARGE}

	for _, size := range sizes {
		t.Run("size_"+string(rune(size)), func(t *testing.T) {
			// Get a buffer
			b1 := Get(size)
			originalCap := cap(b1)

			// Modify it
			b1 = append(b1, byte(42))

			// Put it back
			Put(b1)

			// Get again
			b2 := Get(size)

			// Should have same capacity
			if cap(b2) != originalCap {
				t.Errorf("Expected capacity %d after round trip, got %d", originalCap, cap(b2))
			}

			// Should be reset to length 0
			if len(b2) != 0 {
				t.Errorf("Expected length 0 after round trip, got %d", len(b2))
			}
		})
	}
}

func TestGetPut_MultipleOperations(t *testing.T) {
	// Perform multiple Get/Put operations to test pool reuse
	for i := 0; i < 100; i++ {
		b := Get(SIZE_SMALL)
		b = append(b, byte(i))
		if len(b) != 1 {
			t.Errorf("Expected length 1, got %d", len(b))
		}
		if b[0] != byte(i) {
			t.Errorf("Expected byte value %d, got %d", byte(i), b[0])
		}
		Put(b)
	}
}

func TestGet_AllPoolsUsed(t *testing.T) {
	// Test that all pool types are actually used
	sizes := []int{SIZE_BYTE, SIZE_4_BYTE, SIZE_TINY, SIZE_SMALL, SIZE_MEDIUM, SIZE_LARGE}
	buffers := make([][]byte, len(sizes))

	for i, size := range sizes {
		buffers[i] = Get(size)
		if cap(buffers[i]) != size {
			t.Errorf("Get(%d): expected capacity %d, got %d", size, size, cap(buffers[i]))
		}
	}

	// Put them all back
	for _, buf := range buffers {
		Put(buf)
	}

	// Get them again to verify pool reuse
	for _, size := range sizes {
		b := Get(size)
		if cap(b) != size {
			t.Errorf("Second Get(%d): expected capacity %d, got %d", size, size, cap(b))
		}
		Put(b)
	}
}

func TestPut_WithData(t *testing.T) {
	b := Get(SIZE_SMALL)
	// Fill with some data
	for i := 0; i < 100; i++ {
		b = append(b, byte(i))
	}

	if len(b) != 100 {
		t.Fatalf("Expected length 100, got %d", len(b))
	}

	Put(b)

	// Get a new buffer - it should be reset to length 0
	b2 := Get(SIZE_SMALL)
	if len(b2) != 0 {
		t.Errorf("Expected new buffer to have length 0, got %d", len(b2))
	}
}

func BenchmarkGet_Byte(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_BYTE)
		Put(buf)
	}
}

func BenchmarkGet_4Byte(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_4_BYTE)
		Put(buf)
	}
}

func BenchmarkGet_Tiny(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_TINY)
		Put(buf)
	}
}

func BenchmarkGet_Small(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_SMALL)
		Put(buf)
	}
}

func BenchmarkGet_Medium(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_MEDIUM)
		Put(buf)
	}
}

func BenchmarkGet_Large(b *testing.B) {
	for b.Loop() {
		buf := Get(SIZE_LARGE)
		Put(buf)
	}
}

func BenchmarkGetPut_Mixed(b *testing.B) {
	sizes := []int{SIZE_BYTE, SIZE_4_BYTE, SIZE_TINY, SIZE_SMALL, SIZE_MEDIUM, SIZE_LARGE}
	for b.Loop() {
		for _, size := range sizes {
			buf := Get(size)
			Put(buf)
		}
	}
}
