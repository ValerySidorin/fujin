package client

import (
	"testing"
	"time"
)

func TestCorrelator_Next(t *testing.T) {
	manager := newCorrelator[error]()
	ch := make(chan error)

	id := manager.next(ch)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if _, exists := manager.m[id]; !exists {
		t.Errorf("Expected channel to be stored with id %d, but it was not found", id)
	}
}

func TestCorrelator_Concurrency(t *testing.T) {
	manager := newCorrelator[error]()
	ch := make(chan error)
	const goroutines = 100

	done := make(chan struct{})
	for range goroutines {
		go func() {
			manager.next(ch)
			done <- struct{}{}
		}()
	}

	for range goroutines {
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out")
		}
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if len(manager.m) != goroutines {
		t.Errorf("Expected %d channels, but got %d", goroutines, len(manager.m))
	}
}
func TestCorrelator_Uint32Max(t *testing.T) {
	manager := newCorrelator[error]()
	ch := make(chan error)

	manager.n = ^uint32(0) - 1

	id1 := manager.next(ch)
	id2 := manager.next(ch)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if id1 != ^uint32(0) {
		t.Errorf("Expected id1 to be %d, but got %d", ^uint32(0), id1)
	}

	if id2 != 0 {
		t.Errorf("Expected id2 to wrap around to 0, but got %d", id2)
	}

	if _, exists := manager.m[id1]; !exists {
		t.Errorf("Expected channel to be stored with id %d, but it was not found", id1)
	}

	if _, exists := manager.m[id2]; !exists {
		t.Errorf("Expected channel to be stored with id %d, but it was not found", id2)
	}
}

func TestCorrelator_Delete(t *testing.T) {
	manager := newCorrelator[error]()
	ch := make(chan error)

	id := manager.next(ch)
	manager.delete(id)

	manager.mu.RLock()
	defer manager.mu.RUnlock()

	if _, exists := manager.m[id]; exists {
		t.Errorf("Expected channel with id %d to be deleted, but it still exists", id)
	}
}

func TestCorrelator_DeleteNonExistent(t *testing.T) {
	manager := newCorrelator[error]()

	manager.delete(12345)

	if len(manager.m) != 0 {
		t.Errorf("Expected map to remain empty, but it has %d entries", len(manager.m))
	}
}

func TestCorrelator_EmptyMap(t *testing.T) {
	manager := newCorrelator[error]()

	if len(manager.m) != 0 {
		t.Errorf("Expected map to be empty, but it has %d entries", len(manager.m))
	}
}

func TestCorrelator_ConcurrentSend(t *testing.T) {
	manager := newCorrelator[error]()
	ch := make(chan error, 100)
	id := manager.next(ch)

	const goroutines = 100
	var err error

	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			manager.send(id, err)
			done <- struct{}{}
		}()
	}

	for range goroutines {
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out")
		}
	}

	close(ch)
	count := 0
	for range ch {
		count++
	}

	if count != goroutines {
		t.Errorf("Expected %d errors to be sent to the channel, but got %d", goroutines, count)
	}
}
