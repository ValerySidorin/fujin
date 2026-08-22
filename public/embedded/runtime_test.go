package embedded_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/embedded"
	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	_ "github.com/fujin-io/fujin/public/plugins/transport/tcp"
)

var registerEmbeddedTestConnector sync.Once

func registerConnector(t *testing.T) {
	t.Helper()
	registerEmbeddedTestConnector.Do(func() {
		if err := connector.Register("embedded_test", connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
			return connector.CompileStatic(
				map[string]connector.RouteProfile{"route": {}},
				map[string]connector.RouteFactory{"route": {}},
			)
		}}); err != nil {
			panic(err)
		}
	})
}

func TestRuntimeReportsEphemeralEndpointAndStops(t *testing.T) {
	registerConnector(t)
	runtime, err := embedded.Start(testConfig("v1"), embedded.Options{
		BuildVersion: "embedded-test",
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoints := runtime.Endpoints()
	if len(endpoints) != 1 {
		t.Fatalf("endpoints: got %v", endpoints)
	}
	endpoint := endpoints[0]
	if endpoint.Interface != "native" || endpoint.Transport != "tcp" || endpoint.Network != "tcp" {
		t.Fatalf("unexpected endpoint: %+v", endpoint)
	}
	if endpoint.Address == "127.0.0.1:0" {
		t.Fatal("runtime did not expose the actual ephemeral port")
	}
	connection, err := net.DialTimeout("tcp", endpoint.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()

	status := runtime.Status()
	if status.SchemaVersion != 1 || status.State != "ready" || status.Connectors.BuildVersion != "embedded-test" {
		t.Fatalf("unexpected status: %+v", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if state := runtime.Status().State; state != "stopped" {
		t.Fatalf("state after close: got %q", state)
	}
}

func TestRuntimeWaitsForHealthEndpoint(t *testing.T) {
	registerConnector(t)
	runtime, err := embedded.Start([]byte(`
grpc:
  enabled: false
health:
  enabled: true
  addr: 127.0.0.1:0
connectors:
  main:
    type: embedded_test
`), embedded.Options{ReadyTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	endpoints := runtime.Endpoints()
	if len(endpoints) != 1 || endpoints[0].Interface != "health" || endpoints[0].Address == "" {
		t.Fatalf("health endpoint: %+v", endpoints)
	}
	response, err := http.Get("http://" + endpoints[0].Address + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ready status: got %d", response.StatusCode)
	}
}

func TestRuntimeRejectsConfigurationWithoutListeners(t *testing.T) {
	registerConnector(t)
	_, err := embedded.Start([]byte(`
grpc:
  enabled: false
connectors:
  main:
    type: embedded_test
`), embedded.Options{ReadyTimeout: time.Second})
	if !errors.Is(err, embedded.ErrNoListeners) {
		t.Fatalf("error: got %v, want ErrNoListeners", err)
	}
}

func TestRuntimeAppliesMonotonicConnectorSnapshots(t *testing.T) {
	registerConnector(t)
	runtime, err := embedded.Start(testConfig("v1"), embedded.Options{ReadyTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())

	accepted := runtime.ApplyConnectorSnapshot(context.Background(), 1, testConnectors("v2"))
	if accepted.State != configurator.ApplyAccepted || !accepted.Changed || accepted.Err != nil {
		t.Fatalf("accepted snapshot: %+v", accepted)
	}
	duplicate := runtime.ApplyConnectorSnapshot(context.Background(), 1, testConnectors("v2"))
	if duplicate.State != configurator.ApplyAccepted || duplicate.Changed || duplicate.Err != nil {
		t.Fatalf("duplicate snapshot: %+v", duplicate)
	}
	conflict := runtime.ApplyConnectorSnapshot(context.Background(), 1, testConnectors("conflict"))
	if conflict.State != configurator.ApplyRejected || conflict.Err == nil {
		t.Fatalf("conflicting snapshot: %+v", conflict)
	}
	stale := runtime.ApplyConnectorSnapshot(context.Background(), 0, testConnectors("v1"))
	if stale.State != configurator.ApplyStale || stale.Err != nil {
		t.Fatalf("stale snapshot: %+v", stale)
	}
}

func testConfig(version string) []byte {
	return []byte(fmt.Sprintf(`
fujin:
  transports:
    - type: tcp
      settings:
        addr: 127.0.0.1:0
grpc:
  enabled: false
connectors:
  main:
    type: embedded_test
    settings:
      version: %s
`, version))
}

func testConnectors(version string) []byte {
	return []byte(fmt.Sprintf(`
main:
  type: embedded_test
  settings:
    version: %s
`, version))
}
