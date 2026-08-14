package amqp1

import (
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/fujin-io/fujin/public/util"
)

type ConnConfig struct {
	Addr         string         `yaml:"addr"`
	ContainerID  string         `yaml:"container_id"`
	HostName     string         `yaml:"host_name"`
	IdleTimeout  time.Duration  `yaml:"idle_timeout"`
	MaxFrameSize uint32         `yaml:"max_frame_size"`
	MaxSessions  uint16         `yaml:"max_sessions"`
	Properties   map[string]any `yaml:"properties"`
	WriteTimeout time.Duration  `yaml:"write_timeout"`
}

type SessionConfig struct {
	MaxLinks uint32 `yaml:"max_links"`
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
	Source                    string                 `yaml:"source"`
	Credit                    int32                  `yaml:"credit"`
	Durability                amqp.Durability        `yaml:"durability"`
	DynamicAddress            bool                   `yaml:"dynamic_address"`
	ExpiryPolicy              amqp.ExpiryPolicy      `yaml:"expiry_policy"`
	ExpiryTimeout             uint32                 `yaml:"expiry_timeout"`
	Filters                   []amqp.LinkFilter      `yaml:"filters"`
	Name                      string                 `yaml:"name"`
	Properties                map[string]any         `yaml:"properties"`
	RequestedSenderSettleMode *amqp.SenderSettleMode `yaml:"requested_sender_settle_mode"`
}

type SendConfig struct {
	Settled bool `yaml:"settled"`
}

// CommonSettings contains settings shared across all routes.
type CommonSettings struct {
	// Common connection settings can be added here if needed
	// For now, connection settings are per-route.
}

// RouteSettings contains settings specific to a route.
// A route can be either a reader or a writer.
type RouteSettings struct {
	// Connection settings
	Conn ConnConfig `yaml:"conn"`

	// Session settings
	Session SessionConfig `yaml:"session"`

	// Writer-specific settings (optional, only for writers)
	Sender *SenderConfig `yaml:"sender,omitempty"`
	Send   *SendConfig   `yaml:"send,omitempty"`

	// Reader-specific settings (optional, only for readers)
	Receiver *ReceiverConfig `yaml:"receiver,omitempty"`
}

// Config is the top-level configuration structure.
type Config struct {
	Common CommonSettings           `yaml:"common"`
	Routes map[string]RouteSettings `yaml:"routes"`
}

// ConnectorConfig combines common and route-specific settings.
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
	if len(c.Routes) == 0 {
		return util.ValidationErr("at least one route must be defined")
	}

	for name, route := range c.Routes {
		if route.Conn.Addr == "" {
			return util.ValidationErr(fmt.Sprintf("route %q: addr is not defined", name))
		}

		// Validate writer config.
		if route.Sender != nil {
			if route.Sender.Target == "" {
				return util.ValidationErr(fmt.Sprintf("route %q: sender.target is not defined", name))
			}
		}

		// Validate reader config.
		if route.Receiver != nil {
			if route.Receiver.Source == "" {
				return util.ValidationErr(fmt.Sprintf("route %q: receiver.source is not defined", name))
			}
		}

		// Route must have either sender (writer) or receiver (reader).
		if route.Sender == nil && route.Receiver == nil {
			return util.ValidationErr(fmt.Sprintf("route %q: must have either sender (writer) or receiver (reader) configured", name))
		}
	}

	return nil
}

func (c *ConnectorConfig) Endpoint() string {
	return c.Conn.Addr
}
