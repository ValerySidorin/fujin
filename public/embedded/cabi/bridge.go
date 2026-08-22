// Package cabi implements the stable state and ownership semantics behind the
// Fujin C ABI. The generated c-shared entrypoint only translates C buffers.
package cabi

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin/public/embedded"
	"github.com/fujin-io/fujin/public/plugins/configurator"
)

const ABIVersion uint32 = 1

// BuildVersion is set by generated library entrypoints through linker flags.
var BuildVersion string

// Result is a stable ABI result code. Existing values will never be renumbered.
type Result uint32

const (
	ResultOK Result = iota
	ResultInvalidArgument
	ResultInvalidHandle
	ResultBufferTooSmall
	ResultInvalidConfig
	ResultStartFailed
	ResultTimeout
	ResultSnapshotRejected
	ResultInternal
	ResultPanic
)

// SnapshotState is a stable connector-snapshot outcome code.
type SnapshotState uint32

const (
	SnapshotAccepted SnapshotState = iota
	SnapshotRejected
	SnapshotStale
	SnapshotSuperseded
)

// Bridge owns process-local numeric handles. Handles are never Go pointers and
// are never reused during the process lifetime.
type Bridge struct {
	next atomic.Uint64
	mu   sync.RWMutex
	runs map[uint64]*entry
}

type entry struct {
	mu      sync.Mutex
	runtime *embedded.Runtime
}

// NewBridge creates an isolated handle registry, primarily for tests.
func NewBridge() *Bridge { return &Bridge{runs: make(map[uint64]*entry)} }

// Default is the registry used by exported C functions.
var Default = NewBridge()

// Start copies configuration into a new embedded runtime and returns a handle.
func (b *Bridge) Start(config []byte, readyTimeout time.Duration) (uint64, Result, error) {
	if b == nil || len(config) == 0 || readyTimeout < 0 {
		return 0, ResultInvalidArgument, errors.New("config is empty or timeout is invalid")
	}
	runtime, err := embedded.Start(config, embedded.Options{
		BuildVersion: BuildVersion,
		ReadyTimeout: readyTimeout,
	})
	if err != nil {
		switch {
		case errors.Is(err, embedded.ErrInvalidConfig):
			return 0, ResultInvalidConfig, err
		case errors.Is(err, embedded.ErrStartupTimeout):
			return 0, ResultTimeout, err
		default:
			return 0, ResultStartFailed, err
		}
	}
	handle := b.next.Add(1)
	if handle == 0 {
		_ = runtime.Close(context.Background())
		return 0, ResultInternal, errors.New("handle space exhausted")
	}
	b.mu.Lock()
	b.runs[handle] = &entry{runtime: runtime}
	b.mu.Unlock()
	return handle, ResultOK, nil
}

// StatusJSON returns the versioned detached runtime status document.
func (b *Bridge) StatusJSON(handle uint64) ([]byte, Result, error) {
	entry, ok := b.lock(handle)
	if !ok {
		return nil, ResultInvalidHandle, errors.New("invalid Fujin handle")
	}
	defer entry.mu.Unlock()
	encoded, err := json.Marshal(entry.runtime.Status())
	if err != nil {
		return nil, ResultInternal, err
	}
	return encoded, ResultOK, nil
}

// ApplyConnectorSnapshot applies a complete connector snapshot.
func (b *Bridge) ApplyConnectorSnapshot(handle, revision uint64, config []byte) (SnapshotState, bool, Result, error) {
	if len(config) == 0 {
		return SnapshotRejected, false, ResultInvalidArgument, errors.New("connector snapshot is empty")
	}
	entry, ok := b.lock(handle)
	if !ok {
		return SnapshotRejected, false, ResultInvalidHandle, errors.New("invalid Fujin handle")
	}
	defer entry.mu.Unlock()
	applied := entry.runtime.ApplyConnectorSnapshot(context.Background(), revision, config)
	state := snapshotState(applied.State)
	if applied.Err != nil {
		return state, applied.Changed, ResultSnapshotRejected, applied.Err
	}
	return state, applied.Changed, ResultOK, nil
}

// Stop requests shutdown. A timeout preserves the handle so the caller may retry.
func (b *Bridge) Stop(handle uint64, timeout time.Duration) (Result, error) {
	if timeout < 0 {
		return ResultInvalidArgument, errors.New("timeout is invalid")
	}
	entry, ok := b.lock(handle)
	if !ok {
		return ResultInvalidHandle, errors.New("invalid Fujin handle")
	}
	defer entry.mu.Unlock()
	ctx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	if err := entry.runtime.Close(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ResultTimeout, err
		}
		return ResultInternal, err
	}
	b.mu.Lock()
	delete(b.runs, handle)
	b.mu.Unlock()
	return ResultOK, nil
}

func (b *Bridge) lock(handle uint64) (*entry, bool) {
	if b == nil || handle == 0 {
		return nil, false
	}
	b.mu.RLock()
	entry, ok := b.runs[handle]
	b.mu.RUnlock()
	if !ok {
		return nil, false
	}
	entry.mu.Lock()
	b.mu.RLock()
	current, registered := b.runs[handle]
	b.mu.RUnlock()
	if !registered || current != entry {
		entry.mu.Unlock()
		return nil, false
	}
	return entry, true
}

func snapshotState(state configurator.ApplyState) SnapshotState {
	switch state {
	case configurator.ApplyAccepted:
		return SnapshotAccepted
	case configurator.ApplyStale:
		return SnapshotStale
	case configurator.ApplySuperseded:
		return SnapshotSuperseded
	default:
		return SnapshotRejected
	}
}
