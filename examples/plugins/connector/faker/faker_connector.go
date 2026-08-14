package faker

import (
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func descriptor() connector.Descriptor {
	return connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
		profiles := map[string]connector.RouteProfile{
			"default": {
				Produce:          true,
				Headers:          true,
				Transactions:     true,
				Subscribe:        true,
				ProduceGuarantee: connector.AcceptanceLocal,
			},
		}
		factories := map[string]connector.RouteFactory{
			"default": {
				Reader: func(autoSettle bool, l *slog.Logger) (connector.ReadCloser, error) {
					return NewReader(autoSettle, l)
				},
				Writer: func(l *slog.Logger) (connector.WriteCloser, error) { return NewWriter(l) },
			},
		}
		return connector.CompileStatic(profiles, factories)
	}}
}
