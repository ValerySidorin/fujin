package core

import (
	"fmt"
	"strings"
)

// allowedSettings contains settings that can be overridden at runtime
// NATS Core has minimal configuration - URL and Subject are connection identifiers
// and should not be changed at runtime
var allowedSettings = map[string]bool{
	// Currently no runtime-overridable settings for NATS Core
}

// convertConfigValue converts and validates a configuration value for NATS Core
func convertConfigValue(settingPath string, value string) (any, error) {
	// Normalize the path by removing the "routes.<name>." prefix when present.
	normalizedPath := normalizePath(settingPath)

	// Check if this setting is allowed
	if !allowedSettings[normalizedPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.Contains(settingPath, ".tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	// No settings currently allowed
	return nil, fmt.Errorf("setting '%s' is not supported for runtime override", settingPath)
}

// normalizePath removes the "routes.<name>." prefix from a setting path.
func normalizePath(fullPath string) string {
	if strings.HasPrefix(fullPath, "routes.") {
		parts := strings.SplitN(fullPath, ".", 3)
		if len(parts) >= 3 {
			return parts[2]
		}
	}
	return fullPath
}
