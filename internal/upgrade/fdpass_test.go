//go:build unix

package upgrade

import (
	"net"
	"os"
	"testing"
)

func TestSendRecvFDs(t *testing.T) {
	sockPath := testSockPath(t)

	// Create a pair of connected Unix sockets
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Create a TCP listener to get a real FD
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpLn.Close()

	tcpFile, err := tcpLn.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpFile.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.AcceptUnix()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		metadata := []byte(`{"test":"value"}`)
		done <- SendFDs(conn, []*os.File{tcpFile}, metadata)
	}()

	clientConn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	files, metadata, err := RecvFDs(clientConn, 4)
	if err != nil {
		t.Fatal(err)
	}

	if err := <-done; err != nil {
		t.Fatal("sender error:", err)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	defer files[0].Close()

	if string(metadata) != `{"test":"value"}` {
		t.Fatalf("unexpected metadata: %q", metadata)
	}

	// Verify the received FD is usable as a listener
	receivedLn, err := net.FileListener(files[0])
	if err != nil {
		t.Fatal("received FD is not a valid listener:", err)
	}
	receivedLn.Close()
}

func TestControlProtocol(t *testing.T) {
	sockPath := testSockPath(t)

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.AcceptUnix()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		// Server side: receive request_fds, send fds_response
		msg, err := recvMessage(conn)
		if err != nil {
			done <- err
			return
		}
		if msg.Cmd != CmdRequestFDs {
			done <- err
			return
		}

		// Create a TCP listener to pass
		tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			done <- err
			return
		}
		defer tcpLn.Close()

		file, err := tcpLn.(*net.TCPListener).File()
		if err != nil {
			done <- err
			return
		}
		defer file.Close()

		metas := []FDMeta{{Type: "tcp", Addr: ":0"}}
		if err := sendFDResponse(conn, []*os.File{file}, metas); err != nil {
			done <- err
			return
		}

		// Wait for ready
		msg, err = recvMessage(conn)
		if err != nil {
			done <- err
			return
		}
		if msg.Cmd != CmdReady {
			done <- err
			return
		}

		done <- sendMessage(conn, Message{Cmd: CmdDrainAck})
	}()

	clientConn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	// Client side: send request_fds
	if err := sendMessage(clientConn, Message{Cmd: CmdRequestFDs}); err != nil {
		t.Fatal(err)
	}

	// Receive FDs
	files, metas, err := recvFDResponse(clientConn, 4)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if metas[0].Type != "tcp" {
		t.Fatalf("expected type tcp, got %s", metas[0].Type)
	}

	// Verify FD is usable
	receivedLn, err := net.FileListener(files[0])
	if err != nil {
		t.Fatal(err)
	}
	receivedLn.Close()
	files[0].Close()

	// Send ready
	if err := sendMessage(clientConn, Message{Cmd: CmdReady}); err != nil {
		t.Fatal(err)
	}

	// Receive drain_ack
	msg, err := recvMessage(clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Cmd != CmdDrainAck {
		t.Fatalf("expected drain_ack, got %s", msg.Cmd)
	}

	if err := <-done; err != nil {
		t.Fatal("server error:", err)
	}
}
