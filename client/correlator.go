package client

import (
	"sync"
)

type correlator struct {
	n uint32
	m map[uint32]chan error

	mu sync.RWMutex
}

func newCorrelator() *correlator {
	return &correlator{
		m: make(map[uint32]chan error),
	}
}

func (m *correlator) next(c chan error) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.n++
	m.m[m.n] = c
	return m.n
}

func (m *correlator) send(id uint32, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.m[id]
	if ok {
		c <- err
	}
}

func (m *correlator) delete(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, id)
}
