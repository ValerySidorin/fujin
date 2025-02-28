package pool

import (
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
