package client

import (
	"sync"
)

type subscriptions struct {
	mu sync.RWMutex
	m  map[byte]*Subscription
}

func newSubscriptions() *subscriptions {
	return &subscriptions{
		m: make(map[byte]*Subscription),
	}
}

func (sm *subscriptions) add(s *Subscription) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.m[s.id] = s
}

func (sm *subscriptions) get(id byte) (*Subscription, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	s, ok := sm.m[id]
	return s, ok
}

func (sm *subscriptions) delete(id byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.m, id)
}

func (sm *subscriptions) close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, sub := range sm.m {
		sub.Close()
	}
}
