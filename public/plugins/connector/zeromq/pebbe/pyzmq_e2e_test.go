//go:build zeromq_pebbe && cgo

package pebbe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pyPeer struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	output  *bufio.Scanner
	stderr  *bytes.Buffer
}

func requirePyZMQ(t *testing.T) string {
	t.Helper()
	if os.Getenv("FUJIN_ZEROMQ_PYZMQ") != "1" {
		t.Skip("set FUJIN_ZEROMQ_PYZMQ=1 to run independent pyzmq interoperability")
	}
	python := os.Getenv("FUJIN_ZEROMQ_PYTHON")
	if python == "" {
		python = "python3"
	}
	command := exec.Command(python, "-c", "import zmq")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("pyzmq unavailable through %s: %v: %s", python, err, output)
	}
	return python
}

func startPyPeer(t *testing.T, python, operation, kind, endpoint, ownership, subscription string, count int) *pyPeer {
	t.Helper()
	command := exec.Command(python, "testdata/pyzmq_peer.py", operation, kind, endpoint, fmt.Sprint(count), ownership, subscription, "250")
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	require.NoError(t, command.Start())
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	peer := &pyPeer{command: command, stdin: stdin, output: scanner, stderr: stderr}
	require.True(t, peer.output.Scan(), "pyzmq peer exited before readiness: %s", stderr.String())
	require.Equal(t, "READY", peer.output.Text())
	t.Cleanup(func() {
		if peer.command.ProcessState == nil {
			_ = peer.command.Process.Kill()
			_ = peer.command.Wait()
		}
	})
	return peer
}

func (p *pyPeer) send(t *testing.T, messages [][][]byte) {
	t.Helper()
	require.NoError(t, json.NewEncoder(p.stdin).Encode(messages))
	require.NoError(t, p.stdin.Close())
	require.NoError(t, p.command.Wait(), p.stderr.String())
}

func (p *pyPeer) receive(t *testing.T) [][][]byte {
	t.Helper()
	require.NoError(t, p.stdin.Close())
	require.True(t, p.output.Scan(), "pyzmq peer produced no messages: %s", p.stderr.String())
	var messages [][][]byte
	require.NoError(t, json.Unmarshal(p.output.Bytes(), &messages))
	require.NoError(t, p.command.Wait(), p.stderr.String())
	return messages
}

func freeTCPEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return "tcp://" + address
}

func TestPyZMQInteroperabilityMatrix(t *testing.T) {
	python := requirePyZMQ(t)
	t.Run("Fujin PUB bind to pyzmq SUB", func(t *testing.T) {
		r := openTestRuntime(t, RouteSettings{Pattern: PatternPub, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingFujinV1, Topic: "events."}, nil)
		peer := startPyPeer(t, python, "receive", "sub", routeEndpoint(t, r.routes["route"]), "connect", "events.", 1)
		writer, err := r.NewWriter("route", slog.Default())
		require.NoError(t, err)
		writer = connector.EnforceWriterContract(writer)
		callback := make(chan error, 1)
		writer.HProduce(context.Background(), nil, [][]byte{[]byte("kind"), []byte("one"), []byte("kind"), []byte("two")}, func(err error) { callback <- err })
		require.NoError(t, <-callback)
		messages := peer.receive(t)
		require.Len(t, messages, 1)
		assert.Equal(t, [][]byte{[]byte("events."), fujinV1Magic, {0, 4}, []byte("kind"), []byte("one"), []byte("kind"), []byte("two"), {}}, messages[0])
		require.NoError(t, writer.Close())
	})

	t.Run("Fujin PUSH connect to pyzmq PULL", func(t *testing.T) {
		endpoint := freeTCPEndpoint(t)
		r := openTestRuntime(t, RouteSettings{Pattern: PatternPush, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingRaw}, nil)
		peer := startPyPeer(t, python, "receive", "pull", endpoint, "bind", "", 1)
		writer, err := r.NewWriter("route", slog.Default())
		require.NoError(t, err)
		payload := bytes.Repeat([]byte("x"), 1<<20)
		callback := make(chan error, 1)
		writer.Produce(context.Background(), payload, func(err error) { callback <- err })
		require.NoError(t, <-callback)
		messages := peer.receive(t)
		assert.Equal(t, [][][]byte{{payload}}, messages)
		require.NoError(t, writer.Close())
	})

	t.Run("pyzmq PUB bind to Fujin SUB", func(t *testing.T) {
		endpoint := freeTCPEndpoint(t)
		r := openTestRuntime(t, RouteSettings{Pattern: PatternSub, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingFujinV1, Subscriptions: []string{"events."}}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader, err := r.NewReader("route", true, slog.Default())
		require.NoError(t, err)
		ready := make(chan struct{})
		delivered := make(chan decodedMessage, 1)
		subscribeErr := make(chan error, 1)
		go func() {
			subscribeErr <- reader.SubscribeWithHeaders(ctx, func() error { close(ready); return nil }, func(payload []byte, source string, headers [][]byte, _ ...any) {
				delivered <- decodedMessage{payload: payload, source: source, headers: headers}
			})
		}()
		peer := startPyPeer(t, python, "send", "pub", endpoint, "bind", "", 1)
		select {
		case <-ready:
		case <-time.After(3 * time.Second):
			t.Fatal("Fujin SUB did not reach handshake readiness")
		}
		peer.send(t, [][][]byte{{[]byte("events."), fujinV1Magic, {0, 2}, []byte("k"), []byte("v"), {}}})
		select {
		case message := <-delivered:
			assert.Empty(t, message.payload)
			assert.Equal(t, "events.", message.source)
			assert.Equal(t, [][]byte{[]byte("k"), []byte("v")}, message.headers)
		case <-time.After(3 * time.Second):
			t.Fatal("pyzmq PUB message not delivered")
		}
		cancel()
		require.NoError(t, <-subscribeErr)
	})

	t.Run("pyzmq PUSH connect to Fujin PULL", func(t *testing.T) {
		r := openTestRuntime(t, RouteSettings{Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingRaw}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader, err := r.NewReader("route", true, slog.Default())
		require.NoError(t, err)
		ready := make(chan struct{})
		delivered := make(chan []byte, 2)
		subscribeErr := make(chan error, 1)
		go func() {
			subscribeErr <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func(payload []byte, _ string, _ ...any) { delivered <- payload })
		}()
		<-ready
		peer := startPyPeer(t, python, "send", "push", routeEndpoint(t, r.routes["route"]), "connect", "", 2)
		large := bytes.Repeat([]byte("z"), 1<<20)
		peer.send(t, [][][]byte{{{}}, {large}})
		select {
		case payload := <-delivered:
			assert.Empty(t, payload)
		case <-time.After(3 * time.Second):
			t.Fatal("empty pyzmq PUSH message not delivered")
		}
		select {
		case payload := <-delivered:
			assert.Equal(t, large, payload)
		case <-time.After(3 * time.Second):
			t.Fatal("1 MiB pyzmq PUSH message not delivered")
		}
		cancel()
		require.NoError(t, <-subscribeErr)
	})
}
