package pool

import (
	"sync"
	"sync/atomic"
	"testing"
)

type mockCloser struct {
	closed int32
}

func (m *mockCloser) Close() {
	atomic.StoreInt32(&m.closed, 1)
}

func TestNewPool(t *testing.T) {
	newFunc := func() (any, error) {
		return &mockCloser{}, nil
	}
	p := NewPool(newFunc)
	if p == nil {
		t.Fatal("Expected non-nil pool")
	}
}

func TestGetFromEmptyPool(t *testing.T) {
	newFunc := func() (any, error) {
		return &mockCloser{}, nil
	}
	p := NewPool(newFunc)
	obj, err := p.Get()
	if err != nil {
		t.Fatalf("Did not expect error, got %v", err)
	}
	if obj == nil {
		t.Fatal("Expected non-nil object")
	}
}

func TestPutAndGetFromPool(t *testing.T) {
	newFunc := func() (any, error) {
		return &mockCloser{}, nil
	}
	p := NewPool(newFunc)

	original := &mockCloser{}
	p.Put(original)
	obj, err := p.Get()
	if err != nil {
		t.Fatalf("Did not expect error, got %v", err)
	}
	if obj != original {
		t.Fatal("Expected to get the same object")
	}
}

func TestClosePool(t *testing.T) {
	newFunc := func() (any, error) {
		return &mockCloser{}, nil
	}
	p := NewPool(newFunc)

	closer := &mockCloser{}
	p.Put(closer)

	p.Close()
	if atomic.LoadInt32(&closer.closed) != 1 {
		t.Fatal("Expected the object to be closed")
	}
}

type mockErrCloser struct {
	closed int32
}

func (m *mockErrCloser) Close() error {
	atomic.StoreInt32(&m.closed, 1)
	return nil
}

func TestErrCloser(t *testing.T) {
	newFunc := func() (any, error) {
		return &mockErrCloser{}, nil
	}
	p := NewPool(newFunc)

	errCloser := &mockErrCloser{}
	p.Put(errCloser)

	p.Close()
	if atomic.LoadInt32(&errCloser.closed) != 1 {
		t.Fatal("Expected the object to be closed with error")
	}
}

func TestPutOverflowClosesItem(t *testing.T) {
	p := NewPoolWithSize(func() (any, error) {
		return &mockCloser{}, nil
	}, 1)

	first := &mockCloser{}
	overflow := &mockCloser{}

	p.Put(first)
	p.Put(overflow)

	if atomic.LoadInt32(&overflow.closed) != 1 {
		t.Fatal("Expected overflow item to be closed")
	}
	if atomic.LoadInt32(&first.closed) != 0 {
		t.Fatal("Expected first item to remain open")
	}
}

func TestConcurrentGetPut(t *testing.T) {
	var created atomic.Int64
	p := NewPool(func() (any, error) {
		created.Add(1)
		return &mockCloser{}, nil
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			obj, err := p.Get()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			p.Put(obj)
		}()
	}
	wg.Wait()

	// Should have reused some objects
	if created.Load() >= 100 {
		t.Fatal("Expected some object reuse")
	}
}
