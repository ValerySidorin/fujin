// Package all imports all available configurator plugins for side-effect registration.
// Import this package to enable all configurators:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/configurator/all"
package all

import (
	// File configurator plugin
	_ "github.com/fujin-io/fujin/public/plugins/configurator/yaml"
	// Env var configurator plugin
	_ "github.com/fujin-io/fujin/public/plugins/configurator/env"
)
