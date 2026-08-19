//go:build fujin

// Package all imports all transport plugins for side-effect registration.
// Import this package to enable all transports (tcp, quic, unix).
// Use build tags to exclude transports: -tags quic,unix (excludes tcp)
//
//	import _ "github.com/fujin-io/fujin/public/plugins/transport/all"
package all

import (
	_ "github.com/fujin-io/fujin/public/plugins/transport/quic"
	_ "github.com/fujin-io/fujin/public/plugins/transport/tcp"
	_ "github.com/fujin-io/fujin/public/plugins/transport/unix"
	_ "github.com/fujin-io/fujin/public/plugins/transport/websocket"
)
