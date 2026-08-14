package connector

import "sync"

// ReadyOnce guarantees the adapter readiness callback is invoked at most once.
type ReadyOnce struct {
	once sync.Once
	err  error
}

func (r *ReadyOnce) Signal(ready func() error) error {
	r.once.Do(func() {
		if ready != nil {
			r.err = ready()
		}
	})
	return r.err
}
