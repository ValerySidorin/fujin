package websocket

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestStreamPreservesFujinBytesAcrossBinaryMessages(t *testing.T) {
	serverStream := make(chan io.ReadWriter, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		serverStream <- newStream(conn)
	}))
	defer server.Close()

	conn, _, err := gorillaws.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	require.NoError(t, err)
	defer conn.Close()
	stream := <-serverStream
	defer stream.(io.Closer).Close()

	payload := bytes.Repeat([]byte("fujin"), 1024)
	require.NoError(t, conn.WriteMessage(gorillaws.BinaryMessage, payload[:1000]))
	require.NoError(t, conn.WriteMessage(gorillaws.BinaryMessage, payload[1000:]))
	read := make([]byte, len(payload))
	require.NoError(t, stream.(interface{ SetReadDeadline(time.Time) error }).SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.ReadFull(stream, read)
	require.NoError(t, err)
	require.Equal(t, payload, read)

	written := []byte("response")
	_, err = stream.Write(written)
	require.NoError(t, err)
	messageType, response, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, gorillaws.BinaryMessage, messageType)
	require.Equal(t, written, response)
}

func TestStreamCoalescesDeadlineBoundWritesIntoOneBinaryMessage(t *testing.T) {
	serverStream := make(chan *stream, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		serverStream <- newStream(conn)
	}))
	defer server.Close()

	conn, _, err := gorillaws.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	require.NoError(t, err)
	defer conn.Close()
	stream := <-serverStream
	defer stream.Close()

	require.NoError(t, stream.SetWriteDeadline(time.Now().Add(time.Second)))
	_, err = stream.Write([]byte("first"))
	require.NoError(t, err)
	_, err = stream.Write([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, stream.SetWriteDeadline(time.Time{}))

	messageType, response, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, gorillaws.BinaryMessage, messageType)
	require.Equal(t, []byte("firstsecond"), response)
}

func TestStreamRejectsTextMessages(t *testing.T) {
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			serverErr <- err
			return
		}
		stream := newStream(conn)
		defer stream.Close()
		buffer := make([]byte, 1)
		_, err = stream.Read(buffer)
		serverErr <- err
	}))
	defer server.Close()

	conn, _, err := gorillaws.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(gorillaws.TextMessage, []byte("x")))
	require.ErrorContains(t, <-serverErr, "binary")
	_ = conn.Close()
}
