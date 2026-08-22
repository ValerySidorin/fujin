package cabi_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/embedded"
	"github.com/fujin-io/fujin/public/embedded/cabi"
	"github.com/fujin-io/fujin/public/plugins/connector"
	_ "github.com/fujin-io/fujin/public/plugins/transport/tcp"
)

var registerCABIConnector sync.Once

func registerConnector(t *testing.T) {
	t.Helper()
	registerCABIConnector.Do(func() {
		if err := connector.Register("cabi_test", connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
			return connector.CompileStatic(
				map[string]connector.RouteProfile{"route": {}},
				map[string]connector.RouteFactory{"route": {}},
			)
		}}); err != nil {
			panic(err)
		}
	})
}

func TestBridgeOwnsHandleUntilSuccessfulStop(t *testing.T) {
	registerConnector(t)
	bridge := cabi.NewBridge()
	handle, result, err := bridge.Start(config("v1"), 5*time.Second)
	if err != nil || result != cabi.ResultOK || handle == 0 {
		t.Fatalf("start: handle=%d result=%d err=%v", handle, result, err)
	}

	statusJSON, result, err := bridge.StatusJSON(handle)
	if err != nil || result != cabi.ResultOK {
		t.Fatalf("status: result=%d err=%v", result, err)
	}
	var status embedded.Status
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		t.Fatal(err)
	}
	if status.SchemaVersion != 1 || status.State != "ready" || len(status.Endpoints) != 1 {
		t.Fatalf("status: %+v", status)
	}

	state, changed, result, err := bridge.ApplyConnectorSnapshot(handle, 1, connectors("v2"))
	if err != nil || result != cabi.ResultOK || state != cabi.SnapshotAccepted || !changed {
		t.Fatalf("apply: state=%d changed=%t result=%d err=%v", state, changed, result, err)
	}
	state, changed, result, err = bridge.ApplyConnectorSnapshot(handle, 0, connectors("v1"))
	if err != nil || result != cabi.ResultOK || state != cabi.SnapshotStale || changed {
		t.Fatalf("stale: state=%d changed=%t result=%d err=%v", state, changed, result, err)
	}

	result, err = bridge.Stop(handle, 5*time.Second)
	if err != nil || result != cabi.ResultOK {
		t.Fatalf("stop: result=%d err=%v", result, err)
	}
	if _, result, err = bridge.StatusJSON(handle); result != cabi.ResultInvalidHandle || err == nil {
		t.Fatalf("released handle remained valid: result=%d err=%v", result, err)
	}
	if result, err = bridge.Stop(handle, time.Second); result != cabi.ResultInvalidHandle || err == nil {
		t.Fatalf("double stop: result=%d err=%v", result, err)
	}
}

func TestBridgeRejectsZeroAndUnknownHandles(t *testing.T) {
	bridge := cabi.NewBridge()
	for _, handle := range []uint64{0, 42} {
		if _, result, err := bridge.StatusJSON(handle); result != cabi.ResultInvalidHandle || err == nil {
			t.Fatalf("status handle %d: result=%d err=%v", handle, result, err)
		}
	}
}

func TestBridgeClassifiesInvalidConfiguration(t *testing.T) {
	bridge := cabi.NewBridge()
	if handle, result, err := bridge.Start([]byte("["), time.Second); handle != 0 || result != cabi.ResultInvalidConfig || err == nil {
		t.Fatalf("invalid config: handle=%d result=%d err=%v", handle, result, err)
	}
}

func TestBridgeSerializesConcurrentStop(t *testing.T) {
	registerConnector(t)
	bridge := cabi.NewBridge()
	handle, result, err := bridge.Start(config("v1"), 5*time.Second)
	if err != nil || result != cabi.ResultOK {
		t.Fatalf("start: result=%d err=%v", result, err)
	}

	results := make(chan cabi.Result, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, _ := bridge.Stop(handle, 5*time.Second)
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	counts := map[cabi.Result]int{}
	for result := range results {
		counts[result]++
	}
	if counts[cabi.ResultOK] != 1 || counts[cabi.ResultInvalidHandle] != 1 {
		t.Fatalf("concurrent stop results: %v", counts)
	}
}

func config(version string) []byte {
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
    type: cabi_test
    settings:
      version: %s
`, version))
}

func connectors(version string) []byte {
	return []byte(fmt.Sprintf(`
main:
  type: cabi_test
  settings:
    version: %s
`, version))
}
