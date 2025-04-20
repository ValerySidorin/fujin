package client

import (
	"sync"
)

type model struct {
	msgMetaLen     byte
	numMsgsInBatch uint32
	err            error
}

type correlator struct {
	n uint32
	m map[uint32]chan model

	mu sync.RWMutex
}

func newCorrelator() *correlator {
	return &correlator{
		m: make(map[uint32]chan model),
	}
}

func (m *correlator) next(c chan model) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.n++
	m.m[m.n] = c
	return m.n
}

func (m *correlator) send(id uint32, model model) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.m[id]
	if ok {
		c <- model
	}
}

func (m *correlator) delete(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, id)
}
