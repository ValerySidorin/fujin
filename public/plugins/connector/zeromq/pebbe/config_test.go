//go:build zeromq_pebbe && cgo

package pebbe

import (
	"strings"
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileConnectorPublishesExactProfilesWithoutOpeningSecrets(t *testing.T) {
	key := strings.Repeat("0", 40)
	compiled, err := compileConnector(Config{Routes: map[string]RouteSettings{
		"pub":     {Pattern: PatternPub, Endpoint: "tcp://127.0.0.1:5555", Topic: "events."},
		"sub_raw": {Pattern: PatternSub, Endpoint: "ipc:///tmp/fujin-zmq-test.sock", Framing: FramingRaw},
		"curve_pull": {
			Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:5556", Mode: ModeBind,
			Security: SecuritySettings{Mechanism: SecurityCurve, PublicKey: key, SecretKeyPath: "/does/not/exist", AllowedClientPublicKeys: []string{key}},
		},
	}})
	require.NoError(t, err)
	profiles := compiled.Routes()
	assert.Equal(t, connector.RouteProfile{Produce: true, Headers: true, ProduceGuarantee: connector.AcceptanceLocal}, profiles["pub"])
	assert.Equal(t, connector.RouteProfile{Subscribe: true}, profiles["sub_raw"])
	assert.Equal(t, connector.RouteProfile{Subscribe: true, Headers: true}, profiles["curve_pull"])
	assert.True(t, compiled.(connector.EagerRuntimeCompiled).OpenRuntimeEagerly())
	assert.Equal(t, []string{"tcp://127.0.0.1:5556"}, compiled.(connector.ExclusiveRuntimeCompiled).ExclusiveRuntimeKeys())
}

func TestCompileConnectorRejectsInvalidRouteContracts(t *testing.T) {
	tests := []struct {
		name     string
		route    RouteSettings
		contains string
	}{
		{name: "unsupported endpoint", route: RouteSettings{Pattern: PatternPull, Endpoint: "udp://127.0.0.1:1"}, contains: "endpoint"},
		{name: "pub topic", route: RouteSettings{Pattern: PatternPub, Endpoint: "tcp://127.0.0.1:1"}, contains: "topic"},
		{name: "push subscriptions", route: RouteSettings{Pattern: PatternPush, Endpoint: "tcp://127.0.0.1:1", Subscriptions: []string{"x"}}, contains: "subscriptions"},
		{name: "curve client server key", route: RouteSettings{Pattern: PatternPush, Endpoint: "tcp://127.0.0.1:1", Security: SecuritySettings{Mechanism: SecurityCurve, PublicKey: strings.Repeat("0", 40), SecretKeyPath: "secret"}}, contains: "server_public_key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := compileConnector(Config{Routes: map[string]RouteSettings{"route": test.route}})
			require.ErrorContains(t, err, test.contains)
		})
	}
}

func TestCompileConnectorRejectsDuplicateBindEndpoint(t *testing.T) {
	_, err := compileConnector(Config{Routes: map[string]RouteSettings{
		"one": {Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:5555", Mode: ModeBind},
		"two": {Pattern: PatternSub, Endpoint: "tcp://127.0.0.1:5555", Mode: ModeBind},
	}})
	require.ErrorContains(t, err, "bind the same endpoint")
}
