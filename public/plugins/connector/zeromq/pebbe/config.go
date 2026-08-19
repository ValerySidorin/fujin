//go:build zeromq_pebbe && cgo

package pebbe

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	PatternPub  = "pub"
	PatternSub  = "sub"
	PatternPush = "push"
	PatternPull = "pull"

	ModeConnect = "connect"
	ModeBind    = "bind"

	FramingFujinV1 = "fujin_v1"
	FramingRaw     = "raw"

	SecurityNull  = "null"
	SecurityCurve = "curve"
)

type CommonSettings struct {
	IOThreads               int           `yaml:"io_threads"`
	SendHWM                 int           `yaml:"send_hwm"`
	ReceiveHWM              int           `yaml:"receive_hwm"`
	SendTimeout             time.Duration `yaml:"send_timeout"`
	ReadyTimeout            time.Duration `yaml:"ready_timeout"`
	ReceivePollInterval     time.Duration `yaml:"receive_poll_interval"`
	ReconnectInterval       time.Duration `yaml:"reconnect_interval"`
	ReconnectIntervalMax    time.Duration `yaml:"reconnect_interval_max"`
	Linger                  time.Duration `yaml:"linger"`
	MaxMessageBytes         int           `yaml:"max_message_bytes"`
	SubscriberQueueCapacity int           `yaml:"subscriber_queue_capacity"`
}

type SecuritySettings struct {
	Mechanism               string   `yaml:"mechanism"`
	PublicKey               string   `yaml:"public_key"`
	SecretKeyPath           string   `yaml:"secret_key_path"`
	ServerPublicKey         string   `yaml:"server_public_key"`
	AllowedClientPublicKeys []string `yaml:"allowed_client_public_keys"`
}

type RouteSettings struct {
	Pattern       string           `yaml:"pattern"`
	Endpoint      string           `yaml:"endpoint"`
	Mode          string           `yaml:"mode"`
	Framing       string           `yaml:"framing"`
	Topic         string           `yaml:"topic"`
	Subscriptions []string         `yaml:"subscriptions"`
	Security      SecuritySettings `yaml:"security"`
}

type Config struct {
	Common CommonSettings           `yaml:"common"`
	Routes map[string]RouteSettings `yaml:"routes"`
}

type routeConfig struct {
	name string
	CommonSettings
	RouteSettings
	zapDomain string
	secretKey string
}

func (c *Config) normalizeAndValidate() error {
	applyCommonDefaults(&c.Common)
	if c.Common.IOThreads <= 0 {
		return fmt.Errorf("io_threads must be positive")
	}
	if c.Common.SendHWM <= 0 || c.Common.ReceiveHWM <= 0 {
		return fmt.Errorf("send_hwm and receive_hwm must be positive")
	}
	if c.Common.SendTimeout <= 0 || c.Common.ReadyTimeout <= 0 || c.Common.ReceivePollInterval <= 0 {
		return fmt.Errorf("send_timeout, ready_timeout, and receive_poll_interval must be positive")
	}
	if c.Common.ReconnectInterval <= 0 || c.Common.ReconnectIntervalMax <= 0 || c.Common.ReconnectIntervalMax < c.Common.ReconnectInterval {
		return fmt.Errorf("reconnect intervals must be positive and reconnect_interval_max must not be smaller than reconnect_interval")
	}
	if c.Common.Linger < 0 {
		return fmt.Errorf("linger must not be negative")
	}
	if c.Common.MaxMessageBytes <= 0 || c.Common.SubscriberQueueCapacity <= 0 {
		return fmt.Errorf("max_message_bytes and subscriber_queue_capacity must be positive")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route must be configured")
	}
	bindEndpoints := make(map[string]string)
	for name, route := range c.Routes {
		normalizeRoute(&route)
		if err := validateRoute(name, route); err != nil {
			return err
		}
		if route.Mode == ModeBind {
			if prior := bindEndpoints[route.Endpoint]; prior != "" {
				return fmt.Errorf("routes %q and %q bind the same endpoint %q", prior, name, route.Endpoint)
			}
			bindEndpoints[route.Endpoint] = name
		}
		c.Routes[name] = route
	}
	return nil
}

func applyCommonDefaults(c *CommonSettings) {
	if c.IOThreads == 0 {
		c.IOThreads = 1
	}
	if c.SendHWM == 0 {
		c.SendHWM = 1000
	}
	if c.ReceiveHWM == 0 {
		c.ReceiveHWM = 1000
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = 5 * time.Second
	}
	if c.ReadyTimeout == 0 {
		c.ReadyTimeout = 10 * time.Second
	}
	if c.ReceivePollInterval == 0 {
		c.ReceivePollInterval = 100 * time.Millisecond
	}
	if c.ReconnectInterval == 0 {
		c.ReconnectInterval = 100 * time.Millisecond
	}
	if c.ReconnectIntervalMax == 0 {
		c.ReconnectIntervalMax = 5 * time.Second
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = 4 << 20
	}
	if c.SubscriberQueueCapacity == 0 {
		c.SubscriberQueueCapacity = 256
	}
}

func normalizeRoute(route *RouteSettings) {
	route.Pattern = strings.ToLower(strings.TrimSpace(route.Pattern))
	route.Mode = strings.ToLower(strings.TrimSpace(route.Mode))
	if route.Mode == "" {
		route.Mode = ModeConnect
	}
	route.Framing = strings.ToLower(strings.TrimSpace(route.Framing))
	if route.Framing == "" {
		route.Framing = FramingFujinV1
	}
	route.Security.Mechanism = strings.ToLower(strings.TrimSpace(route.Security.Mechanism))
	if route.Security.Mechanism == "" {
		route.Security.Mechanism = SecurityNull
	}
}

func validateRoute(name string, route RouteSettings) error {
	prefix := fmt.Sprintf("route %q", name)
	switch route.Pattern {
	case PatternPub, PatternSub, PatternPush, PatternPull:
	default:
		return fmt.Errorf("%s: unsupported pattern %q", prefix, route.Pattern)
	}
	if !strings.HasPrefix(route.Endpoint, "tcp://") && !strings.HasPrefix(route.Endpoint, "ipc://") {
		return fmt.Errorf("%s: endpoint must use tcp:// or ipc://", prefix)
	}
	if route.Mode != ModeBind && route.Mode != ModeConnect {
		return fmt.Errorf("%s: mode must be bind or connect", prefix)
	}
	if route.Framing != FramingRaw && route.Framing != FramingFujinV1 {
		return fmt.Errorf("%s: framing must be raw or fujin_v1", prefix)
	}
	if route.Pattern == PatternPub {
		if route.Topic == "" || !utf8.ValidString(route.Topic) {
			return fmt.Errorf("%s: PUB topic must be non-empty UTF-8", prefix)
		}
	} else if route.Topic != "" {
		return fmt.Errorf("%s: topic is valid only for PUB routes", prefix)
	}
	if route.Pattern != PatternSub && len(route.Subscriptions) > 0 {
		return fmt.Errorf("%s: subscriptions are valid only for SUB routes", prefix)
	}
	for _, subscription := range route.Subscriptions {
		if !utf8.ValidString(subscription) {
			return fmt.Errorf("%s: subscription must be UTF-8", prefix)
		}
	}
	if err := validateSecurity(prefix, route.Mode, route.Security); err != nil {
		return err
	}
	return nil
}

func validateSecurity(prefix, mode string, security SecuritySettings) error {
	switch security.Mechanism {
	case SecurityNull:
		if security.PublicKey != "" || security.SecretKeyPath != "" || security.ServerPublicKey != "" || len(security.AllowedClientPublicKeys) > 0 {
			return fmt.Errorf("%s: CURVE fields require mechanism curve", prefix)
		}
		return nil
	case SecurityCurve:
	default:
		return fmt.Errorf("%s: security mechanism must be null or curve", prefix)
	}
	if !validZ85Key(security.PublicKey) {
		return fmt.Errorf("%s: public_key must be a valid 40-character Z85 key", prefix)
	}
	if strings.TrimSpace(security.SecretKeyPath) == "" {
		return fmt.Errorf("%s: secret_key_path is required for CURVE", prefix)
	}
	if mode == ModeConnect {
		if !validZ85Key(security.ServerPublicKey) {
			return fmt.Errorf("%s: server_public_key must be a valid 40-character Z85 key", prefix)
		}
		if len(security.AllowedClientPublicKeys) > 0 {
			return fmt.Errorf("%s: allowed_client_public_keys are valid only for bind mode", prefix)
		}
		return nil
	}
	if security.ServerPublicKey != "" {
		return fmt.Errorf("%s: server_public_key is valid only for connect mode", prefix)
	}
	if len(security.AllowedClientPublicKeys) == 0 {
		return fmt.Errorf("%s: CURVE bind mode requires allowed_client_public_keys", prefix)
	}
	seen := make(map[string]struct{}, len(security.AllowedClientPublicKeys))
	for _, key := range security.AllowedClientPublicKeys {
		if !validZ85Key(key) {
			return fmt.Errorf("%s: allowed client key must be valid Z85", prefix)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s: duplicate allowed client key", prefix)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validZ85Key(key string) bool {
	if len(key) != 40 {
		return false
	}
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
	for _, r := range key {
		if !strings.ContainsRune(alphabet, r) {
			return false
		}
	}
	return true
}
