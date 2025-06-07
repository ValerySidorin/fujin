package fujin

import (
	"fmt"
	"sync"

	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
)

type fetcherCache struct {
	cman *connectors.Manager
	m    map[string]map[bool]reader.Reader
	mu   sync.RWMutex
}

func newFetcherCache() *fetcherCache {
	return &fetcherCache{
		m: make(map[string]map[bool]reader.Reader),
	}
}

func (fm *fetcherCache) get(topic string, autoCommit bool) (reader.Reader, error) {
	fm.mu.RLock()
	r := fm.m[topic][autoCommit]
	if r != nil {
		fm.mu.RUnlock()
		return r, nil
	}

	var err error
	fm.mu.Lock()
	r, err = fm.cman.GetReader(topic, autoCommit)
	if err != nil {
		fm.mu.Unlock()
		return nil, fmt.Errorf("get reader: %w", err)
	}

	acm, ok := fm.m[topic]
	if !ok {
		acm = make(map[bool]reader.Reader)
		fm.m[topic] = acm
	}

	return nil, false
}

func (fm *fetcherMap) put()
