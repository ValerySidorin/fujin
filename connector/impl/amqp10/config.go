package amqp10

import (
	"time"

	"github.com/Azure/go-amqp"
	"github.com/ValerySidorin/fujin/connector/cerr"
)

type ConnConfig struct {
	Addr         string         `yaml:"addr"`
	ContainerID  string         `yaml:"container_id"`
	HostName     string         `yaml:"host_name"`
	IdleTimeout  time.Duration  `yaml:"idle_timeout"`
	MaxFrameSize uint32         `yaml:"max_frame_size"`
	MaxSessions  uint16         `yaml:"max_sessions"` // Useless for now
	Properties   map[string]any `yaml:"properties"`
	WriteTimeout time.Duration  `yaml:"write_timeout"`
	// TODO: Add SASL mechanisms and TLS
}

type SessionConfig struct {
	MaxLinks uint32 `yaml:"max_links"` // Useless for now
}

type SenderConfig struct {
	Target                      string                   `yaml:"target"`
	Capabilities                []string                 `yaml:"capabilities"`
	Durability                  amqp.Durability          `yaml:"durability"`
	DynamicAddress              bool                     `yaml:"dynamic_address"`
	DesiredCapabilities         []string                 `yaml:"desired_capabilities"`
	ExpiryPolicy                amqp.ExpiryPolicy        `yaml:"expiry_policy"`
	ExpiryTimeout               uint32                   `yaml:"expiry_timeout"`
	Name                        string                   `yaml:"name"`
	Properties                  map[string]any           `yaml:"properties"`
	RequestedReceiverSettleMode *amqp.ReceiverSettleMode `yaml:"requested_receiver_settle_mode"`
	SettlementMode              *amqp.SenderSettleMode   `yaml:"settlement_mode"`
	SourceAddress               string                   `yaml:"source_address"`
	TargetCapabilities          []string                 `yaml:"target_capabilities"`
	TargetDurability            amqp.Durability          `yaml:"target_durability"`
	TargetExpiryPolicy          amqp.ExpiryPolicy        `yaml:"target_expiry_policy"`
	TargetExpiryTimeout         uint32                   `yaml:"target_expiry_timeout"`
}

type ReceiverConfig struct {
	Source                    string                   `yaml:"source"`
	Credit                    int32                    `yaml:"credit"`
	Durability                amqp.Durability          `yaml:"durability"`
	DynamicAddress            bool                     `yaml:"dynamic_address"`
	ExpiryPolicy              amqp.ExpiryPolicy        `yaml:"expiry_policy"`
	ExpiryTimeout             uint32                   `yaml:"expiry_timeout"`
	Filters                   []amqp.LinkFilter        `yaml:"filters"`
	Name                      string                   `yaml:"name"`
	Properties                map[string]any           `yaml:"properties"`
	RequestedSenderSettleMode *amqp.SenderSettleMode   `yaml:"requested_sender_settle_mode"`
	SettlementMode            *amqp.ReceiverSettleMode `yaml:"settlement_mode"`
}

type SendConfig struct {
	Settled bool `yaml:"settled"`
}

type WriterConfig struct {
	Conn    ConnConfig    `yaml:"conn"`
	Session SessionConfig `yaml:"session"`
	Sender  SenderConfig  `yaml:"sender"`
	Send    SendConfig    `yaml:"send"`
}

type ReaderConfig struct {
	Conn     ConnConfig     `yaml:"conn"`
	Session  SessionConfig  `yaml:"session"`
	Receiver ReceiverConfig `yaml:"receiver"`
}

func (c *WriterConfig) Validate() error {
	if c.Conn.Addr == "" {
		return cerr.ValidationErr("addr is not defined")
	}
	if c.Sender.Target == "" {
		return cerr.ValidationErr("target is not defined")
	}
	return nil
}

func (c *ReaderConfig) Validate() error {
	if c.Conn.Addr == "" {
		return cerr.ValidationErr("addr is not defined")
	}
	if c.Receiver.Source == "" {
		return cerr.ValidationErr("source is not defined")
	}
	return nil
}
