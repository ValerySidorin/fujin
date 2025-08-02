package client

import (
	"sync"
)

type correlator[T any] struct {
	n uint32
	m map[uint32]chan T

	mu sync.RWMutex
}

func newCorrelator[T any]() *correlator[T] {
	return &correlator[T]{
		m: make(map[uint32]chan T),
	}
}

func (m *correlator[T]) next(c chan T) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.n++
	m.m[m.n] = c
	return m.n
}

func (m *correlator[T]) send(id uint32, val T) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.m[id]
	if ok {
		c <- val
	}
}

func (m *correlator[T]) delete(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, id)
}
