package amqp09

import (
	"fmt"
	"time"

	"github.com/fujin-io/fujin/public/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnConfig struct {
	URL string `yaml:"url"`

	// Connection
	Vhost      string        `yaml:"vhost"`
	ChannelMax uint16        `yaml:"channel_max"`
	FrameSize  int           `yaml:"frame_size"`
	Heartbeat  time.Duration `yaml:"heartbeat"`
	// TODO: Add tls and properties
}

type ExchangeConfig struct {
	Name       string     `yaml:"name"`
	Kind       string     `yaml:"kind"`
	Durable    bool       `yaml:"durable"`
	AutoDelete bool       `yaml:"auto_delete"`
	Internal   bool       `yaml:"internal"`
	NoWait     bool       `yaml:"no_wait"`
	Args       amqp.Table `yaml:"args"`
}

type QueueConfig struct {
	Name       string     `yaml:"name"`
	Durable    bool       `yaml:"durable"`
	AutoDelete bool       `yaml:"auto_delete"`
	Exclusive  bool       `yaml:"exclusive"`
	NoWait     bool       `yaml:"no_wait"`
	Args       amqp.Table `yaml:"args"`
}

type QueueBindConfig struct {
	RoutingKey string     `yaml:"routing_key"`
	NoWait     bool       `yaml:"no_wait"`
	Args       amqp.Table `yaml:"args"`
}

type ConsumeConfig struct {
	Consumer  string     `yaml:"consumer"`
	Exclusive bool       `yaml:"exclusive"`
	NoLocal   bool       `yaml:"no_local"`
	NoWait    bool       `yaml:"no_wait"`
	Args      amqp.Table `yaml:"args"`
}

type AckConfig struct {
	Multiple bool `yaml:"multiple"`
}

type NackConfig struct {
	Multiple bool `yaml:"multiple"`
	Requeue  bool `yaml:"requeue"`
}

type PublishConfig struct {
	Mandatory bool `yaml:"mandatory"`
	Immediate bool `yaml:"immediate"`

	ContentType     string `yaml:"content_type"`
	ContentEncoding string `yaml:"content_encoding"`
	DeliveryMode    uint8  `yaml:"delivery_mode"`
	Priority        uint8  `yaml:"priority"`
	ReplyTo         string `yaml:"reply_to"`
	AppId           string `yaml:"app_id"`
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

	// Exchange settings (required for both reader and writer)
	Exchange ExchangeConfig `yaml:"exchange"`

	// Queue settings (required for both reader and writer)
	Queue QueueConfig `yaml:"queue"`

	// Queue bind settings (required for both reader and writer)
	QueueBind QueueBindConfig `yaml:"queue_bind"`

	// Writer-specific settings (optional, only for writers)
	Publish *PublishConfig `yaml:"publish,omitempty"`

	// Reader-specific settings (optional, only for readers)
	Consume *ConsumeConfig `yaml:"consume,omitempty"`
	Ack     *AckConfig     `yaml:"ack,omitempty"`
	Nack    *NackConfig    `yaml:"nack,omitempty"`
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
		if route.Conn.URL == "" {
			return util.ValidationErr(fmt.Sprintf("route %q: url is not defined", name))
		}

		if route.Exchange.Name == "" {
			return util.ValidationErr(fmt.Sprintf("route %q: exchange.name is not defined", name))
		}

		if route.Exchange.Kind == "" {
			return util.ValidationErr(fmt.Sprintf("route %q: exchange.kind is not defined", name))
		}

		if route.Queue.Name == "" {
			return util.ValidationErr(fmt.Sprintf("route %q: queue.name is not defined", name))
		}

		// Route must have either publish (writer) or consume (reader).
		if route.Publish == nil && route.Consume == nil {
			return util.ValidationErr(fmt.Sprintf("route %q: must have either publish (writer) or consume (reader) configured", name))
		}
	}

	return nil
}

func (c *ConnectorConfig) Endpoint() string {
	return c.Conn.URL
}
