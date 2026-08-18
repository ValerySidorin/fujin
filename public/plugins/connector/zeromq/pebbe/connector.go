//go:build zeromq_pebbe && cgo

package pebbe

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

const connectorName = "zeromq_pebbe"

type compiledConnector struct {
	config   Config
	profiles map[string]connector.RouteProfile
}

func descriptor() connector.Descriptor {
	return connector.Descriptor{Converter: convertConfigValue, Compile: compileConnector}
}

func convertConfigValue(path, _ string) (any, error) {
	return nil, fmt.Errorf("setting %q cannot be overridden at runtime", path)
}

func compileConnector(raw any) (connector.Compiled, error) {
	var config Config
	if parsed, ok := raw.(Config); ok {
		config = parsed
	} else if err := util.ConvertConfig(raw, &config); err != nil {
		return nil, fmt.Errorf("%s connector: convert config: %w", connectorName, err)
	}
	if err := config.normalizeAndValidate(); err != nil {
		return nil, fmt.Errorf("%s connector: invalid config: %w", connectorName, err)
	}
	profiles := make(map[string]connector.RouteProfile, len(config.Routes))
	for name, route := range config.Routes {
		profile := connector.RouteProfile{Headers: route.Framing == FramingFujinV1}
		switch route.Pattern {
		case PatternPub, PatternPush:
			profile.Produce = true
			profile.ProduceGuarantee = connector.AcceptanceLocal
		case PatternSub, PatternPull:
			profile.Subscribe = true
		}
		profiles[name] = profile
	}
	return &compiledConnector{config: config, profiles: profiles}, nil
}

func (c *compiledConnector) Routes() map[string]connector.RouteProfile {
	profiles := make(map[string]connector.RouteProfile, len(c.profiles))
	for name, profile := range c.profiles {
		profiles[name] = profile
	}
	return profiles
}

func (c *compiledConnector) OpenRuntime(l *slog.Logger) (connector.Runtime, error) {
	return openRuntime(c.config, l)
}

func (*compiledConnector) OpenRuntimeEagerly() bool { return true }

func (c *compiledConnector) ExclusiveRuntimeKeys() []string {
	keys := make([]string, 0)
	for _, route := range c.config.Routes {
		if route.Mode == ModeBind {
			keys = append(keys, route.Endpoint)
		}
	}
	sort.Strings(keys)
	return keys
}
