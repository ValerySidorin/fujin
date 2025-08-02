package pool

import (
	"errors"
	"sync"
)

type BytePool struct {
	mu   sync.Mutex
	bits [4]uint64 // 256 бит: 4 * 64
}

func NewBytePool() *BytePool {
	// Все биты = 1: все значения свободны
	return &BytePool{
		bits: [4]uint64{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)},
	}
}

// Get возвращает свободное значение (0–255)
func (bp *BytePool) Get() (byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for i := range 4 {
		if bp.bits[i] != 0 {
			// ищем первый установленный бит
			for j := range 64 {
				mask := uint64(1) << j
				if bp.bits[i]&mask != 0 {
					bp.bits[i] &^= mask // сбросить бит
					return byte(i*64 + j), nil
				}
			}
		}
	}
	return 0, errors.New("no available byte values")
}

// Put возвращает значение обратно в пул
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
