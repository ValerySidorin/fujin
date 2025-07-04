package pool_test

import (
	"testing"

	"github.com/ValerySidorin/fujin/internal/common/pool"
)

func TestBytePool_GetPut0(t *testing.T) {
	pool := pool.NewBytePool()

	val, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error on Get(): %v", err)
	}
	if val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}

	err = pool.Put(0)
	if err != nil {
		t.Fatalf("unexpected error on Put(0): %v", err)
	}

	val, err = pool.Get()
	if err != nil {
		t.Fatalf("unexpected error on Put(0): %v", err)
	}
}

func TestBytePool_GetPut(t *testing.T) {
	pool := pool.NewBytePool()

	seen := make(map[byte]bool)
	for range 256 {
		val, err := pool.Get()
		if err != nil {
			t.Fatalf("unexpected error on Get(): %v", err)
		}
		if seen[val] {
			t.Fatalf("duplicate value returned: %d", val)
		}
		seen[val] = true
	}

	if _, err := pool.Get(); err == nil {
		t.Fatal("expected error when getting from empty pool")
	}

	if err := pool.Put(42); err != nil {
		t.Fatalf("unexpected error on Put(42): %v", err)
	}

	if err := pool.Put(42); err == nil {
		t.Fatal("expected error on double Put(42)")
	}

	val, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error on Get(): %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestBytePool_InvalidPut(t *testing.T) {
	pool := pool.NewBytePool()

	err := pool.Put(255)
	if err == nil {
		t.Fatal("expected error on putting existing value")
	}

	err = pool.Put(250)
	if err == nil {
		t.Fatal("expected error on putting existing value")
	}

	_, _ = pool.Get()
	err = pool.Put(0)
	if err != nil {
		t.Fatalf("unexpected error on Put(0): %v", err)
	}
}

func BenchmarkBytePool_GetPut(b *testing.B) {
	pool := pool.NewBytePool()

	for b.Loop() {
		val, err := pool.Get()
		if err == nil {
			_ = pool.Put(val)
		}
	}
}
