package server

import (
	"context"
	"sync"
	"testing"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
)

func TestReloadConnectors(t *testing.T) {
	initial := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "server_test", Settings: map[string]any{"version": "initial"}},
	}

	conf := testConfig(initial)
	s, err := NewServer(conf, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	initialGeneration := s.catalog.Current()
	got, ok := initialGeneration.Config("conn1")
	if !ok || got.Settings.(map[string]any)["version"] != "initial" {
		t.Fatalf("initial config mismatch: %v", got)
	}

	// Reload with new config
	updated := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "server_test", Settings: map[string]any{"version": "updated"}},
		"conn2": {Type: "server_test", Settings: map[string]any{"version": "new"}},
	}
	if err := s.ReloadConnectors(updated); err != nil {
		t.Fatal(err)
	}

	current := s.catalog.Current()
	if current == initialGeneration {
		t.Fatal("reload did not publish a new generation")
	}
	conn1, ok := current.Config("conn1")
	if !ok || conn1.Settings.(map[string]any)["version"] != "updated" {
		t.Fatalf("conn1 config mismatch: %v", conn1)
	}
	conn2, ok := current.Config("conn2")
	if !ok || conn2.Settings.(map[string]any)["version"] != "new" {
		t.Fatalf("conn2 config mismatch: %v", conn2)
	}
}

func TestReloadConnectors_Concurrent(t *testing.T) {
	initial := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "server_test", Settings: map[string]any{"version": "v0"}},
	}

	conf := testConfig(initial)
	s, err := NewServer(conf, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Simulate concurrent reloads and reads
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			_ = s.ReloadConnectors(connectorconfig.ConnectorsConfig{
				"conn1": {Type: "server_test", Settings: map[string]any{"version": n}},
			})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = s.catalog.Current().Config("conn1")
		}()
	}
	wg.Wait()
}

func TestReadyForConnectionsWaitsForGRPC(t *testing.T) {
	grpc := newLifecycleGRPCServer()
	s := &Server{grpcServer: grpc}
	result := make(chan bool, 1)
	go func() {
		result <- s.ReadyForConnections(time.Second)
	}()

	select {
	case <-result:
		t.Fatal("server reported ready before gRPC was ready")
	case <-time.After(10 * time.Millisecond):
	}

	close(grpc.ready)
	select {
	case ready := <-result:
		if !ready {
			t.Fatal("server did not report ready after gRPC became ready")
		}
	case <-time.After(time.Second):
		t.Fatal("server readiness did not observe gRPC")
	}
}

func TestDoneWaitsForGRPC(t *testing.T) {
	grpc := newLifecycleGRPCServer()
	s := &Server{grpcServer: grpc}
	done := s.Done()

	select {
	case <-done:
		t.Fatal("server reported done before gRPC stopped")
	case <-time.After(10 * time.Millisecond):
	}

	close(grpc.done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server completion did not observe gRPC")
	}
}

type lifecycleGRPCServer struct {
	ready chan struct{}
	done  chan struct{}
}

func newLifecycleGRPCServer() *lifecycleGRPCServer {
	return &lifecycleGRPCServer{
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (s *lifecycleGRPCServer) ListenAndServe(context.Context) error { return nil }
func (s *lifecycleGRPCServer) Stop()                                {}

func (s *lifecycleGRPCServer) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-s.ready:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *lifecycleGRPCServer) Done() <-chan struct{} { return s.done }
