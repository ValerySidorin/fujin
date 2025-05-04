package nsq

import "time"

type PoolConfig struct {
	Size           int           `yaml:"size"`
	PreAlloc       bool          `yaml:"pre_alloc"`
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

type WriterConfig struct {
	Address string     `yaml:"address"`
	Topic   string     `yaml:"topic"`
	Pool    PoolConfig `yaml:"pool"`
}

type ReaderConfig struct {
	Addresses        []string `yaml:"addresses"`
	LookupdAddresses []string `yaml:"lookupd_addresses"`
	Topic            string   `yaml:"topic"`
	Channel          string   `yaml:"channel"`
	MaxInFlight      int      `yaml:"max_in_flight"`
}
