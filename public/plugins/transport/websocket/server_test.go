package websocket

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	gorillaws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"net/http"
)

func TestServerCarriesFujinProtocolOverBinaryMessages(t *testing.T) {
	catalog, err := connector.CompileCatalog(connectorconfig.ConnectorsConfig{}, slog.Default())
	require.NoError(t, err)
	srv := NewServer(serverconfig.WebSocketServerConfig{
		Addr:            "127.0.0.1:0",
		Path:            "/fujin",
		MaxMessageBytes: 4 << 20,
		Fujin:           serverconfig.FujinProtocolConfig{WriteDeadline: time.Second, ForceTerminateTimeout: time.Second},
	}, catalog, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	require.True(t, srv.ReadyForConnections(5*time.Second))

	conn, _, err := gorillaws.DefaultDialer.Dial("ws://"+srv.listener.Addr().String()+"/fujin", nil)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(gorillaws.BinaryMessage, []byte{byte(v1.OP_CODE_STOP)}))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	require.Error(t, err)
	_ = conn.Close()
	cancel()
	require.NoError(t, <-done)
}

func TestServerRejectsUnlistedOrigin(t *testing.T) {
	catalog, err := connector.CompileCatalog(connectorconfig.ConnectorsConfig{}, slog.Default())
	require.NoError(t, err)
	srv := NewServer(serverconfig.WebSocketServerConfig{
		Addr:            "127.0.0.1:0",
		Path:            "/fujin",
		AllowedOrigins:  []string{"https://allowed.example"},
		MaxMessageBytes: 4 << 20,
	}, catalog, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	require.True(t, srv.ReadyForConnections(5*time.Second))

	headers := http.Header{"Origin": []string{"https://denied.example"}}
	conn, response, err := gorillaws.DefaultDialer.Dial("ws://"+srv.listener.Addr().String()+"/fujin", headers)
	require.Error(t, err)
	require.Nil(t, conn)
	require.Equal(t, http.StatusForbidden, response.StatusCode)

	cancel()
	require.NoError(t, <-done)
}
