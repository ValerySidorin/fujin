package pool

const defaultMaxSize = 64

type Closer interface {
	Close()
}

type ErrCloser interface {
	Close() error
}

type Pool struct {
	f  func() (any, error)
	ch chan any
}

func NewPool(f func() (any, error)) *Pool {
	return NewPoolWithSize(f, defaultMaxSize)
}

func NewPoolWithSize(f func() (any, error), maxSize int) *Pool {
	return &Pool{
		f:  f,
		ch: make(chan any, maxSize),
	}
}

func (p *Pool) Get() (any, error) {
	select {
	case c := <-p.ch:
		return c, nil
	default:
		return p.f()
	}
}

func (p *Pool) Put(c any) {
	select {
	case p.ch <- c:
	default:
		closeItem(c)
	}
}

func (p *Pool) Close() {
	for {
		select {
		case c := <-p.ch:
			closeItem(c)
		default:
			return
		}
	}
}

func closeItem(c any) {
	if closer, ok := c.(Closer); ok {
		closer.Close()
		return
	}
	if errCloser, ok := c.(ErrCloser); ok {
		errCloser.Close()
	}
}
