package protocol

type Protocol string

const (
	Kafka         Protocol = "kafka"
	NatsStreaming Protocol = "nats_streaming"
	AMQP091       Protocol = "amqp091"
	AMQP10        Protocol = "amqp10"
	RedisPubSub   Protocol = "redis_pubsub"
	RedisStreams  Protocol = "redis_streams"
)
