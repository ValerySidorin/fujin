//go:build unix

package upgrade

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
)

// FDMeta describes a single listener file descriptor for handoff between processes.
type FDMeta struct {
	Type string `json:"type"` // "tcp", "udp", "unix"
	Addr string `json:"addr"` // ":4850", "/tmp/fujin.sock"
	GRPC bool   `json:"grpc,omitempty"`
}

// Command types for the control protocol.
const (
	CmdRequestFDs = "request_fds"
	CmdFDResponse = "fds_response"
	CmdReady      = "ready"
	CmdDrainAck   = "drain_ack"
)

// Message is a control protocol message exchanged between old and new processes.
type Message struct {
	Cmd string   `json:"cmd"`
	FDs []FDMeta `json:"fds,omitempty"`
}

// sendMessage sends a JSON message over the control connection.
func sendMessage(conn *net.UnixConn, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	_, err = conn.Write(data)
	return err
}

// recvMessage reads a JSON message from the control connection.
func recvMessage(conn *net.UnixConn) (Message, error) {
	buf := make([]byte, 64*1024)
	n, err := conn.Read(buf)
	if err != nil {
		return Message{}, fmt.Errorf("read message: %w", err)
	}
	var msg Message
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

// sendFDResponse sends listener FDs with metadata over the control connection.
func sendFDResponse(conn *net.UnixConn, files []*os.File, metas []FDMeta) error {
	metadata, err := json.Marshal(Message{Cmd: CmdFDResponse, FDs: metas})
	if err != nil {
		return fmt.Errorf("marshal fd response: %w", err)
	}
	return SendFDs(conn, files, metadata)
}

// recvFDResponse receives listener FDs with metadata from the control connection.
func recvFDResponse(conn *net.UnixConn, maxFDs int) ([]*os.File, []FDMeta, error) {
	files, metadata, err := RecvFDs(conn, maxFDs)
	if err != nil {
		return nil, nil, err
	}
	var msg Message
	if err := json.Unmarshal(metadata, &msg); err != nil {
		return nil, nil, fmt.Errorf("unmarshal fd response: %w", err)
	}
	if msg.Cmd != CmdFDResponse {
		return nil, nil, fmt.Errorf("unexpected command: %q, expected %q", msg.Cmd, CmdFDResponse)
	}
	if len(files) != len(msg.FDs) {
		return nil, nil, fmt.Errorf("fd count mismatch: %d files, %d metas", len(files), len(msg.FDs))
	}
	return files, msg.FDs, nil
}
