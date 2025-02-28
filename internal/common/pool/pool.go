package pool

import (
	"sync"
)

type Closer interface {
	Close()
}

type ErrCloser interface {
	Close() error
}

type Pool struct {
	f  func() (any, error)
	p  []any
	mu sync.RWMutex
}

func NewPool(f func() (any, error)) *Pool {
	return &Pool{
		f: f,
	}
}

func (p *Pool) Get() (any, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.p) <= 0 {
		return p.f()
	}

	c := p.p[len(p.p)-1]
	p.p = p.p[:len(p.p)-1]
	return c, nil
}

func (p *Pool) Put(c any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.p = append(p.p, c)
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.p {
		if closer, ok := c.(Closer); ok {
			closer.Close()
			continue
		}
		if errCloser, ok := c.(ErrCloser); ok {
			errCloser.Close()
			continue
		}

	}
}
