package config

type WriterSettings interface {
	Endpoint() string
}

type Config struct {
	Protocol string `yaml:"protocol"`
	Settings any    `yaml:"settings,omitempty"`
	// Kafka        any               `yaml:"kafka,omitempty"`
	// NatsCore     any               `yaml:"nats_core,omitempty"`
	// AMQP091      any               `yaml:"amqp091,omitempty"`
	// AMQP10       any               `yaml:"amqp10,omitempty"`
	// RedisPubSub  any               `yaml:"redis_pubsub,omitempty"`
	// RedisStreams any               `yaml:"redis_streams,omitempty"`
	// MQTT         any               `yaml:"mqtt,omitempty"`
	// NSQ          any               `yaml:"nsq,omitempty"`
}
