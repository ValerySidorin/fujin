package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client"
	"github.com/ValerySidorin/fujin/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub",
				AutoCommit: true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {})
		if err != nil {
			t.Fatalf("failed to connect subscriber: %v", err)
		}
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})

	t.Run("non existing topic", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub1",
				AutoCommit: true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

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
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub",
				AutoCommit: true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		received := make([]client.Msg, 0)

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			received = append(received, msg)
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		nc, _ := nats.Connect("localhost:4222")
		_ = nc.Publish("my_subject", []byte("test message"))
		_ = nc.Publish("my_subject", []byte("test message"))
		nc.Flush()

		time.Sleep(1 * time.Second)
		assert.Equal(t, 2, len(received))
		assert.Equal(t, "test message", string(received[0].Value))
		assert.Equal(t, "test message", string(received[1].Value))
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})

	t.Run("msg async", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub",
				AutoCommit: true,
				Async:      true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		received := make([]client.Msg, 0)

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			received = append(received, msg)
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		nc, _ := nats.Connect("localhost:4222")
		_ = nc.Publish("my_subject", []byte("test message"))
		_ = nc.Publish("my_subject", []byte("test message"))
		nc.Flush()

		time.Sleep(1 * time.Second)
		assert.Equal(t, 2, len(received))
		assert.Equal(t, "test message", string(received[0].Value))
		assert.Equal(t, "test message", string(received[1].Value))
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})
}

func TestSubscriberKafka(t *testing.T) {
	t.Run("msg sync", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub",
				AutoCommit: true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs := test.RunDefaultServerWithKafka3Brokers(ctx)
		defer func() {
			cancel()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		received := make([]client.Msg, 0)

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)
			received = append(received, msg)
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		err = produce(ctx, "my_pub_topic", "test message sub sync 1")
		assert.NoError(t, err)
		err = produce(ctx, "my_pub_topic", "test message sub sync 2")
		assert.NoError(t, err)

		time.Sleep(5 * time.Second)
		if len(received) != 2 {
			t.Fatal("invalid number of received messages")
		}
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
		assert.Equal(t, "test message sub sync 1", string(received[0].Value))
		assert.Equal(t, "test message sub sync 2", string(received[1].Value))
	})

	t.Run("msg async", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic:      "sub",
				AutoCommit: true,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs := test.RunDefaultServerWithKafka3Brokers(ctx)
		defer func() {
			cancel()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		received := make([]client.Msg, 0)

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)
			received = append(received, msg)
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		err = produce(ctx, "my_pub_topic", "test message sub async 1")
		assert.NoError(t, err)
		err = produce(ctx, "my_pub_topic", "test message sub async 2")
		assert.NoError(t, err)

		time.Sleep(5 * time.Second)
		if len(received) != 2 {
			t.Fatal("invalid number of received messages")
		}
		assert.Equal(t, "test message sub async 1", string(received[0].Value))
		assert.Equal(t, "test message sub async 2", string(received[1].Value))
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})

	t.Run("manual ack", func(t *testing.T) {
		conf := client.SubscriberConfig{
			ReaderConfig: client.ReaderConfig{
				Topic: "sub",
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs := test.RunDefaultServerWithKafka3Brokers(ctx)
		defer func() {
			cancel()
			<-fs.Done()
		}()

		addr := "localhost:4848"
		conn, err := client.Connect(ctx, addr, generateTLSConfig())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		received := make([][]byte, 0)

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)
			buf := make([]byte, len(msg.Value))
			copy(buf, msg.Value)
			received = append(received, buf)
			if err := msg.Ack(); err != nil {
				t.Fatal(err)
			}
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		err = produce(ctx, "my_pub_topic", "test message sub manual 1")
		assert.NoError(t, err)
		err = produce(ctx, "my_pub_topic", "test message sub manual 2")
		assert.NoError(t, err)

		time.Sleep(5 * time.Second)
		if len(received) != 2 {
			t.Fatal("invalid number of received messages")
		}
		assert.Equal(t, "test message sub manual 1", string(received[0]))
		assert.Equal(t, "test message sub manual 2", string(received[1]))
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})
}
