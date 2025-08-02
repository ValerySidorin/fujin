package kafka

import (
	"strings"
	"time"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
)

type Balancer string

const (
	BalancerUnknown           Balancer = ""
	BalancerSticky            Balancer = "sticky"
	BalancerCooperativeSticky Balancer = "cooperative_sticky"
	BalancerRange             Balancer = "range"
	BalancerRoundRobin        Balancer = "round_robin"
)

type IsolationLevel string

const (
	IsolationLevelDefault        = ""
	IsolationLevelReadUncommited = "read_uncommited"
	IsolationLevelReadCommited   = "read_commited"
)

type ReaderConfig struct {
	Brokers                []string       `yaml:"brokers"`
	Topic                  string         `yaml:"topic"`
	Group                  string         `yaml:"group"`
	AllowAutoTopicCreation bool           `yaml:"allow_auto_topic_creation"`
	MaxPollRecords         int            `yaml:"max_poll_records"`
	FetchIsolationLevel    IsolationLevel `yaml:"fetch_isolation_level"`
	AutoCommitInterval     time.Duration  `yaml:"auto_commit_interval"`
	AutoCommitMarks        bool           `yaml:"auto_commit_marks"`
	Balancers              []Balancer     `yaml:"balancers"`
	BlockRebalanceOnPoll   bool           `yaml:"block_rebalance_on_poll"`
	PingTimeout            time.Duration  `yaml:"ping_timeout"`
}

type WriterConfig struct {
	Brokers                []string      `yaml:"brokers"`
	Topic                  string        `yaml:"topic"`
	Linger                 time.Duration `yaml:"linger"`
	AllowAutoTopicCreation bool          `yaml:"allow_auto_topic_creation"`
	MaxBufferedRecords     int           `yaml:"max_buffered_records"`
	DisableIdempotentWrite bool          `yaml:"disable_idempotent_write"`
	PingTimeout            time.Duration `yaml:"ping_timeout"`
}

func (c *ReaderConfig) Validate() error {
	if len(c.Brokers) <= 0 {
		return cerr.ValidationErr("brokers not defined")
	}
	if c.Topic == "" {
		return cerr.ValidationErr("topic not defined")
	}

	return nil
}

func (c *WriterConfig) Validate() error {
	if len(c.Brokers) <= 0 {
		return cerr.ValidationErr("brokers not defined")
	}
	if c.Topic == "" {
		return cerr.ValidationErr("topic not defined")
	}

	return nil
}

func (c *WriterConfig) Endpoint() string {
	return strings.Join(c.Brokers, ",")
}
