package server

import (
	"log/slog"
	"os"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/server/config"
)

var registerTestConnector sync.Once

func testConfig(cc connectorconfig.ConnectorsConfig) config.Config {
	registerTestConnector.Do(func() {
		if err := connector.Register("server_test", connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
			return connector.CompileStatic(
				map[string]connector.RouteProfile{"route": {}},
				map[string]connector.RouteFactory{"route": {}},
			)
		}}); err != nil {
			panic(err)
		}
	})
	return config.Config{Connectors: cc}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
