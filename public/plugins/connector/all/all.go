// Package all imports all available connector plugins for side-effect registration.
// Import this package to enable all connectors:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/connector/all"
package all

import (
	// Azure AMQP1.0 connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/azure/amqp1"
	// RabbitMQ AMQP0.9.1 connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/rabbitmq/amqp09"
	// Kafka franz-go connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/kafka/franz"
	// MQTT Paho connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/mqtt/paho"
	// NATS Core connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/nats/core"
	// NATS JetStream connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/nats/jetstream"
	// NSQ connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/nsq"
	// Redis Rueidis PubSub connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/pubsub"
	// Redis Rueidis Streams connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/streams"
)
