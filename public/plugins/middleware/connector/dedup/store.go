package dedup

import (
	"sync"
	"time"
)

// dedupStore is a thread-safe in-memory store for deduplication keys with TTL.
type dedupStore struct {
	ttl  time.Duration
	mu   sync.Mutex
	m    map[[32]byte]time.Time
	stop chan struct{}
}

func newDedupStore(ttl, cleanupInterval time.Duration) *dedupStore {
	s := &dedupStore{
		ttl:  ttl,
		m:    make(map[[32]byte]time.Time),
		stop: make(chan struct{}),
	}
	go s.cleanup(cleanupInterval)
	return s
}

// Add returns true if the key is new (not a duplicate), false if it already exists and hasn't expired.
func (s *dedupStore) Add(key [32]byte) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if expireAt, exists := s.m[key]; exists && now.Before(expireAt) {
		return false // duplicate
	}
	s.m[key] = now.Add(s.ttl)
	return true
}

func (s *dedupStore) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			now := time.Now()
			s.mu.Lock()
			for key, expireAt := range s.m {
				if now.After(expireAt) {
					delete(s.m, key)
				}
			}
			s.mu.Unlock()
		}
	}
}

// Stop stops the background cleanup goroutine.
func (s *dedupStore) Stop() {
	close(s.stop)
}
