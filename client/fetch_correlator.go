package client

import "sync"

type fetchCorrelator struct {
	n uint32
	m map[uint32][]Msg
	e map[uint32]chan error

	mu sync.RWMutex
}

func newFetchCorrelator() *fetchCorrelator {
	return &fetchCorrelator{
		m: make(map[uint32][]Msg),
		e: make(map[uint32]chan error),
	}
}

func (c *fetchCorrelator) next(eCh chan error) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.n++
	c.e[c.n] = eCh
	return c.n
}

func (c *fetchCorrelator) get(id uint32) (msgs []Msg, eCh chan error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.m[id], c.e[id]
}

func (c *fetchCorrelator) setMsgs(id uint32, msgs []Msg) {
	c.mu.Lock()
	c.m[id] = msgs
	c.mu.Unlock()
}

func (c *fetchCorrelator) getMsgs(id uint32) []Msg {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.m[id]
}

func (c *fetchCorrelator) delete(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.m, id)
	delete(c.e, id)
}
