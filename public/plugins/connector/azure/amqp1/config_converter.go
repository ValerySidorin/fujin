package amqp1

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// convertConfigValue converts and validates a configuration value for AMQP10
// This function handles both reader and writer settings
func convertConfigValue(settingPath string, value string) (any, error) {
	// Normalize the path by removing the "routes.<name>." prefix when present.
	settingName := normalizePath(settingPath)

	switch settingName {
	case "conn.idle_timeout", "conn.write_timeout":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for '%s': %w (expected format: '10ms', '5s', '1h', etc.)", settingName, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s must be non-negative, got: %v", settingName, d)
		}
		return d, nil

	case "conn.max_frame_size":
		// Uint32 value
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid uint32 format for 'conn.max_frame_size': %w", err)
		}
		if i == 0 {
			return nil, fmt.Errorf("conn.max_frame_size must be positive, got: %d", i)
		}
		return uint32(i), nil

	case "conn.max_sessions":
		// Uint16 value
		i, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid uint16 format for 'conn.max_sessions': %w", err)
		}
		if i == 0 {
			return nil, fmt.Errorf("conn.max_sessions must be positive, got: %d", i)
		}
		return uint16(i), nil

	case "session.max_links":
		// Uint32 value
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid uint32 format for 'session.max_links': %w", err)
		}
		if i == 0 {
			return nil, fmt.Errorf("session.max_links must be positive, got: %d", i)
		}
		return uint32(i), nil

	case "send.settled":
		// Boolean value (writer only)
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'send.settled': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	case "receiver.credit":
		// Int32 value (reader only)
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid int32 format for 'receiver.credit': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("receiver.credit must be positive, got: %d", i)
		}
		return int32(i), nil

	case "sender.target":
		// String value (writer only)
		if value == "" {
			return nil, fmt.Errorf("sender.target cannot be empty")
		}
		return value, nil

	case "receiver.source":
		// String value (reader only)
		if value == "" {
			return nil, fmt.Errorf("receiver.source cannot be empty")
		}
		return value, nil

	default:
		// This should not happen if the setting is in the whitelist
		// If it does, it indicates a programming error or unsupported setting
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingName)
	}
}

// normalizePath removes the "routes.<name>." prefix from a setting path.
func normalizePath(path string) string {
	if strings.HasPrefix(path, "routes.") {
		parts := strings.SplitN(path, ".", 3)
		if len(parts) >= 3 {
			return parts[2]
		}
	}
	return path
}
