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

func TestWriter(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("success with id", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.Write("pub", []byte("test data"))
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("success empty id", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		writer, err := conn.ConnectWriter("")
		if err != nil {
			t.Fatalf("failed to connect writer: %v", err)
		}
		defer writer.Close()
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.Write("pub", []byte("test data"))
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("non existent topic", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.Write("non_existent_topic", []byte("test data"))
		assert.Error(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("write after close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
		writer.Close()
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.Write("pub", []byte("test data"))
		assert.Error(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	// Begin transaction will not open transaction in underlying broker straight away.
	// So it will return ok even with NATS under the hood.
	t.Run("begin tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	// Commit transaction will do nothing, if no messages are written in it.
	// So it will return ok even with NATS under the hood.
	t.Run("commit tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.CommitTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	// Rollback transaction will do nothing, if no messages are written in it.
	// So it will return ok even with NATS under the hood.
	t.Run("rollback tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.RollbackTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	// Write to NATS in transaction will return 'begin tx' error, because is is not supported.
	t.Run("write msg in tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.Write("pub", []byte("test data1"))
		assert.Error(t, err)
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("commit tx invalid tx state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.CommitTx()
		assert.Error(t, err)
		assert.Equal(t, "invalid tx state", err.Error())
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})

	t.Run("rollback tx invalid state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

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
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())

		err = writer.RollbackTx()
		assert.Error(t, err)
		assert.Equal(t, "invalid tx state", err.Error())
		assert.NoError(t, writer.CheckParseStateAfterOpForTests())
	})
}
