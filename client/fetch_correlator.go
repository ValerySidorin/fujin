package client

import "sync"

type fetchCorrelator struct {
	n  uint32
	m  map[uint32][]Msg
	e  map[uint32]chan error
	ac map[uint32]bool

	mu sync.RWMutex
}

func newFetchCorrelator() *fetchCorrelator {
	return &fetchCorrelator{
		m:  make(map[uint32][]Msg),
		e:  make(map[uint32]chan error),
		ac: map[uint32]bool{},
	}
}

func (c *fetchCorrelator) next(eCh chan error, autoCommit bool) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.n++
	c.e[c.n] = eCh
	c.ac[c.n] = autoCommit
	return c.n
}

func (c *fetchCorrelator) get(id uint32) (msgs []Msg, autoCommit bool, eCh chan error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.m[id], c.ac[id], c.e[id]
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
	delete(c.ac, id)
}
