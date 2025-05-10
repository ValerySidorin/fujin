package streams

import (
	"time"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/ValerySidorin/fujin/public/connectors/impl/resp/config"
)

type ParseMsgProtocol string

const (
	ParseMsgProtocolJSON ParseMsgProtocol = "json"
)

type StreamConf struct {
	StartID       string `yaml:"start_id"`
	GroupCreateID string `yaml:"group_create_id"`
}

type GroupConf struct {
	Name     string `yaml:"name"`
	Consumer string `yaml:"consumer"`
}

type ReaderConfig struct {
	config.ReaderConfig `yaml:",inline"`
	Streams             map[string]StreamConf `yaml:"streams"`
	Block               time.Duration         `yaml:"block"`
	Count               int64                 `yaml:"count"`
	Group               GroupConf             `yaml:"group"`

	ParseMsgProtocol ParseMsgProtocol `yaml:"parse_msg_protocol"`
}

type WriterConfig struct {
	config.WriterConfig `yaml:",inline"`
	Stream              string           `yaml:"stream"`
	ParseMsgProtocol    ParseMsgProtocol `yaml:"parse_msg_protocol"`
}

func (c ReaderConfig) Validate() error {
	if err := c.ReaderConfig.Validate(); err != nil {
		return err
	}

	if len(c.Streams) <= 0 {
		return cerr.ValidationErr("one or more streams required")
	}

	return nil
}

func (c WriterConfig) Validate() error {
	if err := c.WriterConfig.Validate(); err != nil {
		return err
	}

	if c.Stream == "" {
		return cerr.ValidationErr("stream is required")
	}

	return nil
}
