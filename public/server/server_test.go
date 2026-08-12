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
		"conn1": {Type: "noop"},
	}

	conf := testConfig(initial)
	s, err := NewServer(conf, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Verify initial config
	got := *s.connectorConfig.Load()
	if len(got) != 1 || got["conn1"].Type != "noop" {
		t.Fatalf("initial config mismatch: %v", got)
	}

	// Reload with new config
	updated := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "kafka"},
		"conn2": {Type: "nats"},
	}
	s.ReloadConnectors(updated)

	got = *s.connectorConfig.Load()
	if len(got) != 2 {
		t.Fatalf("expected 2 connectors after reload, got %d", len(got))
	}
	if got["conn1"].Type != "kafka" {
		t.Fatalf("conn1 type should be kafka, got %s", got["conn1"].Type)
	}
	if got["conn2"].Type != "nats" {
		t.Fatalf("conn2 type should be nats, got %s", got["conn2"].Type)
	}
}

func TestReloadConnectors_Concurrent(t *testing.T) {
	initial := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "v0"},
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
			s.ReloadConnectors(connectorconfig.ConnectorsConfig{
				"conn1": {Type: "kafka"},
			})
		}(i)
		go func() {
			defer wg.Done()
			cfg := *s.connectorConfig.Load()
			_ = cfg["conn1"]
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
