package faker

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("faker", descriptor()); err != nil {
		panic(fmt.Sprintf("failed to register faker connector: %v", err))
	}
}
