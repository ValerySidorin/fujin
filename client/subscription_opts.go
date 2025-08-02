package client

type SubscriptionOption func(s *Subscription)

func WithAsync(flag bool) func(*Subscription) {
	return func(s *Subscription) {
		s.conf.Async = flag
	}
}

func WithMsgBufSize(size int) func(*Subscription) {
	return func(s *Subscription) {
		s.conf.MsgBufSize = size
	}
}

func WithPoolConfig(pool PoolConfig) func(*Subscription) {
	return func(s *Subscription) {
		s.conf.Pool = pool
	}
}
