package pool

import "sync"

type SubPool struct {
	extP *Pool
	p    []any
	mu   sync.RWMutex
}

func NewSubPool(extP *Pool) *SubPool {
	return &SubPool{
		extP: extP,
	}
}

func (sp *SubPool) Get() (any, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if len(sp.p) <= 0 {
		return sp.extP.Get()
	}

	c := sp.p[len(sp.p)-1]
	sp.p = sp.p[:len(sp.p)-1]
	return c, nil
}

func (sp *SubPool) Put(c any) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.p = append(sp.p, c)
}

func (sp *SubPool) Close(flush func(c any) error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, c := range sp.p {
		if err := flush(c); err != nil {
			if closer, ok := c.(Closer); ok {
				closer.Close()
				continue
			}
			if errCloser, ok := c.(ErrCloser); ok {
				errCloser.Close()
				continue
			}
		}
		sp.extP.Put(c)
	}
}
