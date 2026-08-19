package pubsub

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("redis_rueidis_pubsub", descriptor()); err != nil {
		panic(fmt.Sprintf("register redis_rueidis_pubsub connector: %v", err))
	}
}
