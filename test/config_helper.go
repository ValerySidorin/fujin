package test

import (
	connector_config "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

func ptr(b bool) *bool { return &b }

// DefaultQUICTransportConfig returns a transport.Config for QUIC with test TLS.
// Go test binaries run with the package directory as their working directory.
func DefaultQUICTransportConfig() transport.Config {
	return transport.Config{
		Type:    "quic",
		Enabled: ptr(true),
		Settings: map[string]any{
			"addr":                 ":4848",
			"max_incoming_streams": 1024,
			"tls": map[string]any{
				"enabled":              true,
				"server_cert_pem_path": "../examples/assets/certs/fujin.io.pem",
				"server_key_pem_path":  "../examples/assets/certs/fujin.io-key.pem",
			},
		},
	}
}

// DefaultTCPTransportConfig returns a transport.Config for TCP.
func DefaultTCPTransportConfig() transport.Config {
	return transport.Config{
		Type:    "tcp",
		Enabled: ptr(true),
		Settings: map[string]any{
			"addr": ":4850",
			"tls":  map[string]any{"enabled": false},
		},
	}
}

// DefaultUnixTransportConfig returns a transport.Config for Unix socket.
func DefaultUnixTransportConfig() transport.Config {
	return transport.Config{
		Type:    "unix",
		Enabled: ptr(true),
		Settings: map[string]any{
			"path": PERF_UNIX_PATH,
		},
	}
}

// MakeConfigWithQUIC creates a server Config with QUIC transport.
func MakeConfigWithQUIC(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	return serverconfig.Config{
		Transports: []transport.Config{DefaultQUICTransportConfig()},
		Connectors: connectors,
	}
}

// MakeConfigWithQUICAndGRPC creates a server Config with QUIC and GRPC.
func MakeConfigWithQUICAndGRPC(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	return serverconfig.Config{
		Transports: []transport.Config{DefaultQUICTransportConfig()},
		GRPC:       DefaultGRPCServerTestConfig,
		Connectors: connectors,
	}
}

// MakeConfigWithGRPC creates a server Config with only the gRPC adapter.
func MakeConfigWithGRPC(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	grpcConfig := DefaultGRPCServerTestConfig
	grpcConfig.TLS = nil
	return serverconfig.Config{
		GRPC:       grpcConfig,
		Connectors: connectors,
	}
}

// MakeConfigWithGRPCAndOptionalTCP preserves the combined benchmark topology
// when native transports are compiled in while supporting grpc-only builds.
func MakeConfigWithGRPCAndOptionalTCP(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	config := MakeConfigWithGRPC(connectors)
	if _, _, ok := transport.Get("tcp"); ok {
		config.Transports = append(config.Transports, DefaultTCPTransportConfig())
	}
	return config
}

// MakeConfigWithTCP creates a server Config with TCP transport.
func MakeConfigWithTCP(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	return serverconfig.Config{
		Transports: []transport.Config{DefaultTCPTransportConfig()},
		Connectors: connectors,
	}
}

// MakeConfigWithUnix creates a server Config with Unix transport.
func MakeConfigWithUnix(connectors connector_config.ConnectorsConfig) serverconfig.Config {
	return serverconfig.Config{
		Transports: []transport.Config{DefaultUnixTransportConfig()},
		Connectors: connectors,
	}
}
