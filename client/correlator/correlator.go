package correlator

import (
	"sync"
)

// Correlator manages correlation between requests and responses
type Correlator[T any] struct {
	n  uint32
	m  map[uint32]chan T
	mu sync.RWMutex
}

// New creates a new correlator
func New[T any]() *Correlator[T] {
	return &Correlator[T]{
		m: make(map[uint32]chan T),
	}
}

// Next registers a new correlation and returns its ID
func (c *Correlator[T]) Next(ch chan T) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	c.m[c.n] = ch
	return c.n
}

// Send sends a value to the correlator with the given ID
func (c *Correlator[T]) Send(id uint32, val T) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ch, ok := c.m[id]; ok {
		ch <- val
	}
}

// Delete removes a correlation by ID
func (c *Correlator[T]) Delete(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
}

// Close closes all pending channels and clears the correlator
func (c *Correlator[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.m {
		close(ch)
	}
	c.m = make(map[uint32]chan T)
}
