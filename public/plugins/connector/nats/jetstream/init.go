package jetstream

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nats_jetstream", descriptor()); err != nil {
		panic(fmt.Sprintf("register nats_jetstream connector: %v", err))
	}
}
