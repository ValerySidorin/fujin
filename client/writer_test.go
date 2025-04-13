package client_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client"
	"github.com/stretchr/testify/assert"
)

func TestConnectWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, shutdown := RunTestServer(ctx)
	defer shutdown()

	time.Sleep(1 * time.Second)

	addr := "localhost:4848"
	conn, err := client.Connect(ctx, addr, generateTLSConfig(),
		client.WithLogger(
			slog.New(
				slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
					AddSource: true,
					Level:     slog.LevelDebug,
				}),
			),
		))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	writer, err := conn.ConnectWriter("id")
	if err != nil {
		t.Fatalf("failed to connect writer: %v", err)
	}
	defer writer.Close()

	time.Sleep(5 * time.Second)
}

func TestWrite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		time.Sleep(1 * time.Second)

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		writer, err := conn.ConnectWriter("id")
		if err != nil {
			t.Fatalf("failed to connect writer: %v", err)
		}
		defer writer.Close()

		err = writer.Write("pub", []byte("test data"))
		assert.NoError(t, err)
	})

	t.Run("non existent topic", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		time.Sleep(1 * time.Second)

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		writer, err := conn.ConnectWriter("id")
		if err != nil {
			t.Fatalf("failed to connect writer: %v", err)
		}
		defer writer.Close()

		err = writer.Write("non_existent_topic", []byte("test data"))
		assert.NoError(t, err)
	})
}
