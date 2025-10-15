package correlator

import (
	"sync"
	"testing"
	"time"
)

func TestCorrelatorBasic(t *testing.T) {
	corr := New[string]()

	ch := make(chan string, 1)
	id := corr.Next(ch)

	if id != 1 {
		t.Errorf("Expected ID 1, got %d", id)
	}

	corr.Send(id, "test")

	select {
	case val := <-ch:
		if val != "test" {
			t.Errorf("Expected 'test', got '%s'", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for value")
	}
}

func TestCorrelatorMultiple(t *testing.T) {
	corr := New[int]()

	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)

	id1 := corr.Next(ch1)
	id2 := corr.Next(ch2)

	if id1 != 1 || id2 != 2 {
		t.Errorf("Expected IDs 1,2, got %d,%d", id1, id2)
	}

	corr.Send(id2, 42)
	corr.Send(id1, 24)

	select {
	case val := <-ch1:
		if val != 24 {
			t.Errorf("Expected 24, got %d", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for ch1")
	}

	select {
	case val := <-ch2:
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for ch2")
	}
}

func TestCorrelatorDelete(t *testing.T) {
	corr := New[string]()

	ch := make(chan string, 1)
	id := corr.Next(ch)

	corr.Delete(id)
	corr.Send(id, "test")

	select {
	case <-ch:
		t.Error("Expected no value after delete")
	case <-time.After(100 * time.Millisecond):
		// Expected - no value should be received
	}
}

func TestCorrelatorConcurrent(t *testing.T) {
	corr := New[int]()

	const numGoroutines = 100
	const numOperations = 10

	var wg sync.WaitGroup

	// Test concurrent Next operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				ch := make(chan int, 1)
				id := corr.Next(ch)
				corr.Send(id, int(id))

				select {
				case val := <-ch:
					if val != int(id) {
						t.Errorf("Expected %d, got %d", id, val)
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("Timeout waiting for value")
				}
			}
		}()
	}

	wg.Wait()
}

func TestCorrelatorClose(t *testing.T) {
	corr := New[string]()

	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)

	id1 := corr.Next(ch1)
	id2 := corr.Next(ch2)

	corr.Close()

	// After close, channels should be closed
	select {
	case <-ch1:
		// Channel should be closed
	case <-time.After(100 * time.Millisecond):
		t.Error("ch1 should be closed")
	}

	select {
	case <-ch2:
		// Channel should be closed
	case <-time.After(100 * time.Millisecond):
		t.Error("ch2 should be closed")
	}

	// Send after close should not panic
	corr.Send(id1, "test")
	corr.Send(id2, "test")
}
