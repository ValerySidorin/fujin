package pool

import (
	"errors"
	"sync"
)

var ErrBytePoolExhausted = errors.New("no available byte values")

type BytePool struct {
	mu   sync.Mutex
	bits [4]uint64
}

func NewBytePool() *BytePool {
	return &BytePool{
		bits: [4]uint64{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)},
	}
}

// Get returns free value (0-255)
func (bp *BytePool) Get() (byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for i := range 4 {
		if bp.bits[i] != 0 {
			for j := range 64 {
				mask := uint64(1) << j
				if bp.bits[i]&mask != 0 {
					bp.bits[i] &^= mask
					return byte(i*64 + j), nil
				}
			}
		}
	}
	return 0, ErrBytePoolExhausted
}

// Put returns value to pool
func (bp *BytePool) Put(val byte) error {
	if val > 254 {
		return errors.New("invalid byte value")
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()

	i := val / 64
	j := val % 64
	mask := uint64(1) << j

	if bp.bits[i]&mask != 0 {
		return errors.New("value already present in pool")
	}

	bp.bits[i] |= mask
	return nil
}
