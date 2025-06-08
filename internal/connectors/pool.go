package connectors

import (
	"fmt"
	"sync"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
)

type Pool struct {
	gman    *Manager
	readers map[string]map[bool]reader.Reader
	rMu     sync.Mutex
}

func NewPool(gman *Manager) *Pool {
	return &Pool{
		gman:    gman,
		readers: map[string]map[bool]reader.Reader{},
	}
}

func (p *Pool) GetReader(name string, autoCommit bool) (reader.Reader, error) {
	p.rMu.Lock()
	defer p.rMu.Unlock()

	r := p.readers[name][autoCommit]
	if r != nil {
		return r, nil
	}

	var err error
	r, err = p.gman.GetReader(name, autoCommit)
	if err != nil {
		return nil, fmt.Errorf("get reader: %w", err)
	}
	rm, ok := p.readers[name]
	if !ok {
		rm = make(map[bool]reader.Reader)
		p.readers[name] = rm
	}

	rm[autoCommit] = r
	return r, nil
}

func (p *Pool) Close() {
	for _, rm := range p.readers {
		for _, r := range rm {
			r.Close()
		}
	}
}
