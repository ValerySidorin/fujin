package protocol

type Protocol string

const (
	Kafka   Protocol = "kafka"
	Nats    Protocol = "nats"
	AMQP091 Protocol = "amqp091"
)
