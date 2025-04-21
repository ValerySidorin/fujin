package client

import "time"

type PoolConfig struct {
	Size           int
	PreAlloc       bool
	ReleaseTimeout time.Duration
}

type SubscriberConfig struct {
	Topic      string
	AutoCommit bool
	Async      bool
	Pool       PoolConfig
}

func (c *SubscriberConfig) ValidateAndSetDefaults() error {
	if c.Topic == "" {
		return ErrEmptyTopic
	}

	if c.Async == true {
		if c.Pool.Size == 0 {
			c.Pool.Size = 1000
		}
		if c.Pool.ReleaseTimeout == 0 {
			c.Pool.ReleaseTimeout = 5 * time.Second
		}
	}

	return nil
}
