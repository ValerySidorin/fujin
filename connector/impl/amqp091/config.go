package amqp091

import (
	"time"

	"github.com/ValerySidorin/fujin/internal/connector/cerr"
	"github.com/rabbitmq/amqp091-go"
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
	Name       string        `yaml:"name"`
	Kind       string        `yaml:"kind"`
	Durable    bool          `yaml:"durable"`
	AutoDelete bool          `yaml:"auto_delete"`
	Internal   bool          `yaml:"internal"`
	NoWait     bool          `yaml:"no_wait"`
	Args       amqp091.Table `yaml:"args"`
}

type QueueConfig struct {
	Name       string        `yaml:"name"`
	Durable    bool          `yaml:"durable"`
	AutoDelete bool          `yaml:"auto_delete"`
	Exclusive  bool          `yaml:"exclusive"`
	NoWait     bool          `yaml:"no_wait"`
	Args       amqp091.Table `yaml:"args"`
}

type QueueBindConfig struct {
	RoutingKey string        `yaml:"routing_key"`
	NoWait     bool          `yaml:"no_wait"`
	Args       amqp091.Table `yaml:"args"`
}

type ConsumeConfig struct {
	Consumer  string        `yaml:"consumer"`
	Exclusive bool          `yaml:"exclusive"`
	NoLocal   bool          `yaml:"no_local"`
	NoWait    bool          `yaml:"no_wait"`
	Args      amqp091.Table `yaml:"args"`
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

type ReaderConfig struct {
	Conn      ConnConfig      `yaml:"conn"`
	Exchange  ExchangeConfig  `yaml:"exchange"`
	Queue     QueueConfig     `yaml:"queue"`
	QueueBind QueueBindConfig `yaml:"queue_bind"`
	Consume   ConsumeConfig   `yaml:"consume"`

	Ack  AckConfig  `yaml:"ack"`
	Nack NackConfig `yaml:"nack"`
}

type WriterConfig struct {
	Conn      ConnConfig      `yaml:"conn"`
	Exchange  ExchangeConfig  `yaml:"exchange"`
	Queue     QueueConfig     `yaml:"queue"`
	QueueBind QueueBindConfig `yaml:"queue_bind"`
	Publish   PublishConfig   `yaml:"publish"`
}

func (c *ReaderConfig) Validate() error {
	if c.Conn.URL == "" {
		return cerr.ValidationErr("url not defined")
	}
	if c.Exchange.Name == "" {
		return cerr.ValidationErr("exchange name not defined")
	}
	if c.Exchange.Kind == "" {
		return cerr.ValidationErr("exchange kind not defined")
	}
	if c.Queue.Name == "" {
		return cerr.ValidationErr("queue name not defined")
	}

	return nil
}

func (c *WriterConfig) Validate() error {
	if c.Conn.URL == "" {
		return cerr.ValidationErr("url not defined")
	}
	if c.Exchange.Name == "" {
		return cerr.ValidationErr("exchange name not defined")
	}
	if c.Exchange.Kind == "" {
		return cerr.ValidationErr("exchange kind not defined")
	}
	if c.Queue.Name == "" {
		return cerr.ValidationErr("queue name not defined")
	}

	return nil
}
