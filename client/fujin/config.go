package fujin

import "time"

type PoolConfig struct {
	Size           int
	PreAlloc       bool
	ReleaseTimeout time.Duration
}

type ReaderConfig struct {
	Topic      string
	AutoCommit bool
	Async      bool
}

func (c *ReaderConfig) Validate() error {
	if c.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}
