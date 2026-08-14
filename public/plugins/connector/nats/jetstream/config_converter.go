package jetstream

import (
	"fmt"
	"strings"
)

// allowedSettings contains settings that can be overridden at runtime.
var allowedSettings = map[string]bool{
	// Currently no runtime-overridable settings for NATS JetStream
}

// convertConfigValue converts and validates a configuration value for NATS JetStream.
func convertConfigValue(settingPath string, value string) (any, error) {
	normalizedPath := normalizePath(settingPath)

	if !allowedSettings[normalizedPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.Contains(settingPath, ".tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

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
