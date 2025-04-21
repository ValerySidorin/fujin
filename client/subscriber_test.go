package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestConnectSubscriber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conf := client.SubscriberConfig{
			Topic:      "sub",
			AutoCommit: true,
		}

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

		subscriber, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {})
		if err != nil {
			t.Fatalf("failed to connect subscriber: %v", err)
		}
		defer subscriber.Close()

		assert.EqualValues(t, 0, subscriber.MsgMetaLen())
	})

	t.Run("non existing topic", func(t *testing.T) {
		conf := client.SubscriberConfig{
			Topic:      "sub1",
			AutoCommit: true,
		}

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

		_, err = conn.ConnectSubscriber(conf, func(msg client.Msg) {})
		assert.Error(t, err)
	})

	t.Run("msg sync", func(t *testing.T) {
		conf := client.SubscriberConfig{
			Topic:      "sub",
			AutoCommit: true,
		}

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

		received := make([]client.Msg, 0)

		_, err = conn.ConnectSubscriber(conf, func(msg client.Msg) {
			received = append(received, msg)
		})

		nc, err := nats.Connect("localhost:4222")
		nc.Publish("my_subject", []byte("test message"))
		nc.Publish("my_subject", []byte("test message"))
		nc.Flush()

		time.Sleep(1 * time.Second)
		assert.Equal(t, 2, len(received))
		assert.Equal(t, "test message", string(received[0].Payload))
		assert.Equal(t, "test message", string(received[1].Payload))
	})

	t.Run("msg async", func(t *testing.T) {
		conf := client.SubscriberConfig{
			Topic:      "sub",
			AutoCommit: true,
			Async:      true,
		}

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

		received := make([]client.Msg, 0)

		_, err = conn.ConnectSubscriber(conf, func(msg client.Msg) {
			received = append(received, msg)
		})

		nc, err := nats.Connect("localhost:4222")
		nc.Publish("my_subject", []byte("test message"))
		nc.Publish("my_subject", []byte("test message"))
		nc.Flush()

		time.Sleep(1 * time.Second)
		assert.Equal(t, 2, len(received))
		assert.Equal(t, "test message", string(received[0].Payload))
		assert.Equal(t, "test message", string(received[1].Payload))
	})
}
