package imports

import (
	_ "github.com/ValerySidorin/fujin/connector/impl/amqp091"
	_ "github.com/ValerySidorin/fujin/connector/impl/amqp10"
	_ "github.com/ValerySidorin/fujin/connector/impl/kafka"
	_ "github.com/ValerySidorin/fujin/connector/impl/mqtt"
	_ "github.com/ValerySidorin/fujin/connector/impl/nats/core"
	_ "github.com/ValerySidorin/fujin/connector/impl/nsq"
	_ "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	_ "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
)
