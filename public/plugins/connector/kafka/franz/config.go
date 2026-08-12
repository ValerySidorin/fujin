package franz

import (
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/util"
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

type CommonSettings struct {
	Brokers     []string          `yaml:"brokers"`
	PingTimeout time.Duration     `yaml:"ping_timeout"`
	TLS         pconfig.TLSConfig `yaml:"tls"`
}

type RouteSettings struct {
	// reader settings
	ConsumeTopics        []string       `yaml:"consume_topics"`
	Group                string         `yaml:"group"`
	MaxPollRecords       int            `yaml:"max_poll_records"`
	FetchIsolationLevel  IsolationLevel `yaml:"fetch_isolation_level"`
	AutoCommitInterval   time.Duration  `yaml:"auto_commit_interval"`
	AutoCommitMarks      bool           `yaml:"auto_commit_marks"`
	Balancers            []Balancer     `yaml:"balancers"`
	BlockRebalanceOnPoll bool           `yaml:"block_rebalance_on_poll"`
	// writer settings
	AllowAutoTopicCreation bool          `yaml:"allow_auto_topic_creation"`
	ProduceTopic           string        `yaml:"produce_topic"`
	Linger                 time.Duration `yaml:"linger"`
	MaxBufferedRecords     int           `yaml:"max_buffered_records"`
	DisableIdempotentWrite bool          `yaml:"disable_idempotent_write"`
	TransactionalID        string        `yaml:"transactional_id"` // Transactional ID for Kafka transactions
}

type Config struct {
	Common CommonSettings           `yaml:"common"`
	Routes map[string]RouteSettings `yaml:"routes"`
}

type ConnectorConfig struct {
	CommonSettings
	RouteSettings
}

func NewConnectorConfig(common CommonSettings, route RouteSettings) ConnectorConfig {
	return ConnectorConfig{
		CommonSettings: common,
		RouteSettings:  route,
	}
}

func (c *Config) Validate() error {
	if len(c.Common.Brokers) <= 0 {
		return util.ValidationErr("brokers not defined")
	}

	for _, c := range c.Routes {
		if len(c.ConsumeTopics) <= 0 && c.ProduceTopic == "" {
			return util.ValidationErr("consume topic or produce topic must be defined")
		}
	}

	return nil
}
