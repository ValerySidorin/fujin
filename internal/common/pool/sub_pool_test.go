package pool

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestNewSubPool(t *testing.T) {
	extPool := NewPool(func() (any, error) {
		return &mockCloser{}, nil
	})

	sp := NewSubPool(extPool)
	if sp == nil {
		t.Fatal("Expected non-nil subpool")
	}
}

func TestGetFromSubPool(t *testing.T) {
	extPool := NewPool(func() (any, error) {
		return &mockCloser{}, nil
	})

	sp := NewSubPool(extPool)

	obj, err := sp.Get()
	if err != nil {
		t.Fatalf("Did not expect error, got %v", err)
	}
	if obj == nil {
		t.Fatal("Expected non-nil object from subpool")
	}
}

func TestPutAndGetFromSubPool(t *testing.T) {
	extPool := NewPool(func() (any, error) {
		return &mockCloser{}, nil
	})

	sp := NewSubPool(extPool)

	original := &mockCloser{}
	sp.Put(original)
	obj, err := sp.Get()
	if err != nil {
		t.Fatalf("Did not expect error, got %v", err)
	}
	if obj != original {
		t.Fatal("Expected to get the same object from subpool")
	}
}

func TestSubPoolClose(t *testing.T) {
	extPool := NewPool(func() (any, error) {
		return &mockCloser{}, nil
	})

	sp := NewSubPool(extPool)

	closer := &mockCloser{}
	sp.Put(closer)

	sp.Close(func(c any) error {
		if cl, ok := c.(*mockCloser); ok {
			cl.Close()
			return nil
		}
		return errors.New("not a mockCloser")
	})

	if atomic.LoadInt32(&closer.closed) != 1 {
		t.Fatal("Expected the object to be closed")
	}
}

func TestSubPoolErrFlush(t *testing.T) {
	extPool := NewPool(func() (any, error) {
		return &mockErrCloser{}, nil
	})

	sp := NewSubPool(extPool)

	errCloser := &mockErrCloser{}
	sp.Put(errCloser)

	sp.Close(func(c any) error {
		return errors.New("flush error")
	})

	if atomic.LoadInt32(&errCloser.closed) != 1 {
		t.Fatal("Expected the object to be closed with error")
	}
}
