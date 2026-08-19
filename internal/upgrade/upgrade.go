//go:build unix

// Package upgrade implements graceful binary upgrade for Fujin.
//
// The upgrade mechanism uses a Unix domain control socket for coordination
// between old and new processes. File descriptors for listener sockets are
// passed via SCM_RIGHTS, allowing the new process to inherit them without
// dropping connections.
//
// # Upgrade Sequence
//
//  1. New process starts with FUJIN_UPGRADE=1
//  2. Connects to the old process's control socket
//  3. Sends request_fds command
//  4. Receives listener FDs via SCM_RIGHTS + metadata
//  5. Starts serving on inherited FDs
//  6. Sends ready command
//  7. Old process stops accepting, drains connections, exits
package upgrade

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/fujin-io/fujin/public/plugins/transport"
)

// UpgradableServer is the interface that the server must implement
// for the upgrade mechanism to collect listener FDs and signal drain.
type UpgradableServer interface {
	ListenerFDs() ([]transport.ListenerFD, error)
}

// Upgrader manages the control socket for graceful binary upgrades.
type Upgrader struct {
	sockPath string
	server   UpgradableServer
	drainFn  func() // called to initiate drain on the old process
	l        *slog.Logger
}

// NewUpgrader creates a new Upgrader that listens for upgrade requests.
func NewUpgrader(sockPath string, server UpgradableServer, drainFn func(), l *slog.Logger) *Upgrader {
	return &Upgrader{
		sockPath: sockPath,
		server:   server,
		drainFn:  drainFn,
		l:        l,
	}
}

// ListenForUpgrade starts listening on the control socket for upgrade requests.
// This should be called by the old process. It blocks until an upgrade is handled
// or the context is cancelled.
func (u *Upgrader) ListenForUpgrade(ctx context.Context) error {
	// Ensure parent directory exists
	if dir := filepath.Dir(u.sockPath); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create upgrade socket dir %s: %w", dir, err)
		}
	}

	// Clean up stale socket
	os.Remove(u.sockPath)

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: u.sockPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen upgrade socket %s: %w", u.sockPath, err)
	}
	defer func() {
		ln.Close()
		os.Remove(u.sockPath)
	}()

	u.l.Info("upgrade socket listening", "path", u.sockPath)

	// Close listener when context is cancelled
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			u.l.Error("accept upgrade connection", "err", err)
			continue
		}

		if err := u.handleUpgradeRequest(conn); err != nil {
			u.l.Error("handle upgrade request", "err", err)
			conn.Close()
			continue
		}
		// Upgrade handled successfully — drain and exit
		return nil
	}
}

func (u *Upgrader) handleUpgradeRequest(conn *net.UnixConn) error {
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// 1. Receive request_fds
	msg, err := recvMessage(conn)
	if err != nil {
		return fmt.Errorf("receive request: %w", err)
	}
	if msg.Cmd != CmdRequestFDs {
		return fmt.Errorf("unexpected command: %q, expected %q", msg.Cmd, CmdRequestFDs)
	}

	u.l.Info("upgrade request received, collecting listener FDs")

	// 2. Collect listener FDs
	listenerFDs, err := u.server.ListenerFDs()
	if err != nil {
		return fmt.Errorf("collect listener fds: %w", err)
	}

	files := make([]*os.File, len(listenerFDs))
	metas := make([]FDMeta, len(listenerFDs))
	for i, lfd := range listenerFDs {
		files[i] = lfd.FD
		metas[i] = FDMeta{
			Type: lfd.Type,
			Addr: lfd.Addr,
			GRPC: lfd.Meta["grpc"] == "true",
		}
	}

	// 3. Send FDs + metadata
	if err := sendFDResponse(conn, files, metas); err != nil {
		return fmt.Errorf("send fds: %w", err)
	}

	// Close the dup'd files (old process still holds the originals)
	for _, f := range files {
		f.Close()
	}

	u.l.Info("listener FDs sent, waiting for new process to be ready", "count", len(files))

	// 4. Wait for ready signal (with extended timeout for new process startup)
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	msg, err = recvMessage(conn)
	if err != nil {
		return fmt.Errorf("receive ready: %w", err)
	}
	if msg.Cmd != CmdReady {
		return fmt.Errorf("unexpected command: %q, expected %q", msg.Cmd, CmdReady)
	}

	// 5. Acknowledge and drain
	if err := sendMessage(conn, Message{Cmd: CmdDrainAck}); err != nil {
		return fmt.Errorf("send drain ack: %w", err)
	}

	u.l.Info("new process ready, initiating drain")
	u.drainFn()

	return nil
}

// RequestUpgrade connects to the old process's control socket and requests
// listener FDs for a graceful upgrade. Returns a map of "type:addr" → *os.File
// and the control connection (caller must call SignalReady when ready, then close).
func RequestUpgrade(sockPath string) (map[string]*os.File, *net.UnixConn, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to upgrade socket %s: %w", sockPath, err)
	}

	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// 1. Send request_fds
	if err := sendMessage(conn, Message{Cmd: CmdRequestFDs}); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("send request: %w", err)
	}

	// 2. Receive FDs + metadata
	files, metas, err := recvFDResponse(conn, 16)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("receive fds: %w", err)
	}

	result := make(map[string]*os.File, len(files))
	for i, meta := range metas {
		key := meta.Type + ":" + meta.Addr
		if meta.GRPC {
			key += ":grpc"
		}
		result[key] = files[i]
	}

	return result, conn, nil
}

// SignalReady tells the old process that the new process is ready to accept connections.
// This should be called after the new process has started serving on inherited FDs.
func SignalReady(sockPath string, conn *net.UnixConn) error {
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	if err := sendMessage(conn, Message{Cmd: CmdReady}); err != nil {
		return fmt.Errorf("send ready: %w", err)
	}

	// Wait for drain_ack
	msg, err := recvMessage(conn)
	if err != nil {
		return fmt.Errorf("receive drain ack: %w", err)
	}
	if msg.Cmd != CmdDrainAck {
		return fmt.Errorf("unexpected command: %q, expected %q", msg.Cmd, CmdDrainAck)
	}

	return nil
}
