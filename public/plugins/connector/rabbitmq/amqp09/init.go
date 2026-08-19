package amqp09

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("rabbitmq_amqp09", descriptor()); err != nil {
		panic(fmt.Sprintf("register rabbitmq_amqp09 connector: %v", err))
	}
}
