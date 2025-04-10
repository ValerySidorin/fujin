package client

import (
	"sync"
)

type defaultCorrelationManager struct {
	n uint32
	m map[uint32]chan error

	mu sync.RWMutex
}

func newDefaultCorrelationManager() *defaultCorrelationManager {
	return &defaultCorrelationManager{
		m: make(map[uint32]chan error),
	}
}

func (m *defaultCorrelationManager) next(c chan error) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.n++
	m.m[m.n] = c
	return m.n
}

func (m *defaultCorrelationManager) send(id uint32, err error) (chan error, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.m[id]
	if ok {
		c <- err
	}
	return c, ok
}

func (m *defaultCorrelationManager) delete(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, id)
}
