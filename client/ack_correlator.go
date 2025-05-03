package client

import "sync"

type ackCorrelator struct {
	n uint32
	m map[uint32]chan AckResponse

	mu sync.RWMutex
}

func newAckCorrelator() *ackCorrelator {
	return &ackCorrelator{
		m: make(map[uint32]chan AckResponse),
	}
}

func (m *ackCorrelator) next(c chan AckResponse) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.n++
	m.m[m.n] = c
	return m.n
}

func (m *ackCorrelator) send(id uint32, resp AckResponse) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.m[id]
	if ok {
		c <- resp
	}
}

func (m *ackCorrelator) delete(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, id)
}
