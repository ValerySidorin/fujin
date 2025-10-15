package fujin_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client/fujin"
	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
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
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil,
			fujin.WithLogger(
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

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("success with id", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.Produce("pub", []byte("test data"))
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("success empty id", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.Produce("pub", []byte("test data"))
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("non existent topic", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.Produce("non_existent_topic", []byte("test data"))
		assert.Error(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("write after close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect writer: %v", err)
		}
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
		stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.Produce("pub", []byte("test data"))
		assert.Error(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	// Begin transaction will not open transaction in underlying broker straight away.
	// So it will return ok even with NATS under the hood.
	t.Run("begin tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	// Commit transaction will do nothing, if no messages are written in it.
	// So it will return ok even with NATS under the hood.
	t.Run("commit tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		writer, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
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
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.RollbackTx()
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	// Write to NATS in transaction will return 'begin tx' error, because is is not supported.
	t.Run("write msg in tx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.BeginTx()
		assert.NoError(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.Produce("pub", []byte("test data1"))
		assert.Error(t, err)
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("commit tx invalid tx state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.CommitTx()
		assert.Error(t, err)
		assert.Equal(t, "invalid tx state", err.Error())
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})

	t.Run("rollback tx invalid state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, shutdown := RunTestServer(ctx)
		defer shutdown()

		addr := "localhost:4848"
		conn, err := fujin.Dial(ctx, addr, generateTLSConfig(), nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		stream, err := conn.Connect("id")
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer stream.Close()
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())

		err = stream.RollbackTx()
		assert.Error(t, err)
		assert.Equal(t, "invalid tx state", err.Error())
		assert.NoError(t, stream.CheckParseStateAfterOpForTests())
	})
}
