package nsq

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nsq", descriptor()); err != nil {
		panic(fmt.Sprintf("register nsq connector: %v", err))
	}
}
