package all

import (
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/amqp091"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/amqp10"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/kafka"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/mqtt"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/nats/core"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/nsq"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/resp/pubsub"
	_ "github.com/ValerySidorin/fujin/public/connectors/impl/resp/streams"
)
