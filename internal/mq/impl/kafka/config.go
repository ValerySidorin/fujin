package kafka

import "time"

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
	DisableAutoCommit      bool           `yaml:"disable_auto_commit"`
	MaxPollRecords         int            `yaml:"max_poll_records"`
	FetchIsolationLevel    IsolationLevel `yaml:"fetch_isolation_level"`
}

type WriterConfig struct {
	Brokers                []string      `yaml:"brokers"`
	Topic                  string        `yaml:"topic"`
	Linger                 time.Duration `yaml:"linger"`
	AllowAutoTopicCreation bool          `yaml:"allow_auto_topic_creation"`

	Endpoint string `yaml:"-"` // Used to compare writers that can be shared in tx
}
