//go:build zeromq_pebbe && cgo

package pebbe

import "github.com/fujin-io/fujin/public/plugins/connector"

func init() {
	if err := connector.Register(connectorName, descriptor()); err != nil {
		panic(err)
	}
}
