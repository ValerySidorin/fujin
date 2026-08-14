package core

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nats_core", descriptor()); err != nil {
		panic(fmt.Sprintf("register nats_core connector: %v", err))
	}
}
