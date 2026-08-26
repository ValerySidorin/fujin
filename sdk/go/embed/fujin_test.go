package fujin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestEncodeStartRequest(t *testing.T) {
	request, err := encodeStartRequest(Options{
		Config: &RuntimeConfig{
			Fujin: FujinConfig{Transports: []TransportConfig{{
				Type:     "tcp",
				Settings: map[string]any{"addr": "127.0.0.1:0"},
			}}},
		},
		WorkerThreads: 2,
		RuntimeThread: "fujin-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(request, &parsed); err != nil {
		t.Fatal(err)
	}
	runtime := parsed["runtime"].(map[string]any)
	if runtime["worker_threads"] != float64(2) || runtime["thread_name"] != "fujin-test" {
		t.Fatalf("unexpected runtime request: %#v", runtime)
	}
	config := parsed["config"].(map[string]any)
	if _, configured := config["grpc"]; configured {
		t.Fatalf("nil gRPC config must preserve the generated library default: %#v", config)
	}
	if parsed["graceful_upgrade"] != false {
		t.Fatalf("graceful upgrade must default false: %#v", parsed)
	}
}

func TestEncodeStartRequestRejectsNegativeWorkers(t *testing.T) {
	_, err := encodeStartRequest(Options{WorkerThreads: -1})
	if err == nil {
		t.Fatal("expected negative worker count to fail")
	}
}

func TestStartHonorsPreCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := new(Library).Start(ctx, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context.Canceled", err)
	}
}

func TestOpenRejectsMissingLibrary(t *testing.T) {
	_, err := Open("/definitely/missing/libfujin.so")
	if err == nil {
		t.Fatal("expected missing library to fail")
	}
}

func TestLibraryLifecycle(t *testing.T) {
	path := os.Getenv("FUJIN_LIBRARY_PATH")
	if path == "" {
		t.Skip("FUJIN_LIBRARY_PATH is not set")
	}
	library, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if library.BuildVersion() == "" {
		t.Fatal("empty build version")
	}
	config := RuntimeConfig{
		Fujin: FujinConfig{Transports: []TransportConfig{{
			Type:     "tcp",
			Settings: map[string]any{"addr": "127.0.0.1:0"},
		}}},
		GRPC:       &GRPCConfig{Enabled: false},
		Connectors: map[string]ConnectorConfig{},
	}
	application, err := library.Start(context.Background(), Options{
		Config:        &config,
		WorkerThreads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-application.Done():
		t.Fatal("application terminated before shutdown")
	default:
	}
	if err := library.Close(); err == nil {
		t.Fatal("library close must reject an active application")
	}
	endpoints, err := application.Endpoints()
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 1 || endpoints[0].Transport == nil || *endpoints[0].Transport != "tcp" {
		t.Fatalf("unexpected endpoints: %#v", endpoints)
	}
	if endpoints[0].Address == "127.0.0.1:0" {
		t.Fatalf("endpoint did not expose bound address: %#v", endpoints[0])
	}
	status, err := application.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.ActiveRevision != 0 {
		t.Fatalf("unexpected initial revision: %d", status.ActiveRevision)
	}
	watches, err := application.WatchesConnectors()
	if err != nil {
		t.Fatal(err)
	}
	if watches {
		t.Fatal("static config unexpectedly owns a connector watcher")
	}
	applied, err := application.ReloadConnectors(ConnectorSnapshot{
		Revision:   1,
		Connectors: map[string]ConnectorConfig{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied.State != "accepted" || applied.Revision != 1 {
		t.Fatalf("unexpected apply result: %#v", applied)
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := application.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	select {
	case <-application.Done():
	default:
		t.Fatal("Done was not closed after shutdown")
	}
	if err := application.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := application.Status(); !errors.Is(err, ErrClosed) {
		t.Fatalf("status after close = %v, want ErrClosed", err)
	}
	if err := library.Close(); err != nil {
		t.Fatal(err)
	}
}
