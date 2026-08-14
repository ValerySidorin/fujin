package paho

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("mqtt_paho", descriptor()); err != nil {
		panic(fmt.Sprintf("register mqtt_paho connector: %v", err))
	}
}
