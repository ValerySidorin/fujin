package streams

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("redis_rueidis_streams", descriptor()); err != nil {
		panic(fmt.Sprintf("register redis_rueidis_streams connector: %v", err))
	}
}
