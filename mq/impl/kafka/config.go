package kafka

import (
	"errors"
	"fmt"
	"time"
)

type IsolationLevel string

const (
	IsolationLevelDefault        = ""
	IsolationLevelReadUncommited = "read_uncommited"
	IsolationLevelReadCommited   = "read_commited"
)

var ErrValidateKafkaConf = errors.New("validate kafka config")

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

func (c *ReaderConfig) Validate() error {
	if len(c.Brokers) <= 0 {
		return validationErr("brokers not defined")
	}
	if c.Topic == "" {
		return validationErr("topic not defined")
	}

	return nil
}

func (c *WriterConfig) Validate() error {
	if len(c.Brokers) <= 0 {
		return validationErr("brokers not defined")
	}
	if c.Topic == "" {
		return validationErr("topic not defined")
	}

	return nil
}

func validationErr(text string) error {
	return fmt.Errorf(text+": %w", ErrValidateKafkaConf)
}
