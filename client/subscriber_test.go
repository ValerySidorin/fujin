package client_test

import (
	"context"
	"fmt"
	"sync"
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

		numMsgs := 5
		received := make(chan client.Msg, numMsgs)
		cnt := 0
		var mu sync.Mutex

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)
			received <- msg
			mu.Lock()
			defer mu.Unlock()
			cnt++
			if cnt >= cap(received) {
				close(received)
			}
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		for i := range cap(received) {
			err = produce(ctx, "my_pub_topic", fmt.Sprintf("test message sub sync %d", i))
			assert.NoError(t, err)
		}

		recCnt := 0
		for msg := range received {
			assert.Equal(t, fmt.Sprintf("test message sub sync %d", recCnt), msg.String())
			recCnt++
		}

		assert.Equal(t, numMsgs, recCnt)
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
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

		numMsgs := 5
		received := make(chan client.Msg, numMsgs)
		cnt := 0
		var mu sync.Mutex

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)
			received <- msg
			mu.Lock()
			defer mu.Unlock()
			cnt++
			if cnt >= cap(received) {
				close(received)
			}
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		for i := range cap(received) {
			err = produce(ctx, "my_pub_topic", fmt.Sprintf("test message sub async %d", i))
			assert.NoError(t, err)
		}

		recCnt := 0
		for msg := range received {
			assert.Equal(t, fmt.Sprintf("test message sub async %d", recCnt), msg.String())
			recCnt++
		}

		assert.Equal(t, numMsgs, recCnt)
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

		numMsgs := 5
		received := make(chan []byte, numMsgs)
		cnt := 0
		var mu sync.Mutex

		sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {
			fmt.Println(msg)

			buf := make([]byte, len(msg.Value))
			copy(buf, msg.Value)

			received <- buf
			mu.Lock()
			defer mu.Unlock()
			cnt++
			if cnt >= cap(received) {
				close(received)
			}

			if err := msg.Ack(); err != nil {
				t.Fatal(err)
			}
		})
		assert.NoError(t, err)
		defer sub.Close()
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())

		for i := range cap(received) {
			err = produce(ctx, "my_pub_topic", fmt.Sprintf("test message sub manual %d", i))
			assert.NoError(t, err)
		}

		recCnt := 0
		for msg := range received {
			assert.Equal(t, fmt.Sprintf("test message sub manual %d", recCnt), string(msg))
			recCnt++
		}

		assert.Equal(t, numMsgs, recCnt)
		assert.NoError(t, sub.CheckParseStateAfterOpForTests())
	})
}

// func TestParse(t *testing.T) {
// 	conf := client.SubscriberConfig{
// 		ReaderConfig: client.ReaderConfig{
// 			Topic: "sub",
// 		},
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	addr := "localhost:4848"
// 	conn, err := client.Connect(ctx, addr, generateTLSConfig())
// 	if err != nil {
// 		t.Fatalf("failed to connect: %v", err)
// 	}
// 	defer conn.Close()

// 	sub, err := conn.ConnectSubscriber(conf, func(msg client.Msg) {})
// 	assert.NoError(t, err)
// 	defer sub.Close()

// 	buf := []byte{8, 0, 0, 0, 8, 0, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 3, 6, 109, 121, 95, 112, 117, 98, 95, 116, 111, 112, 105, 99, 0}
// 	err = sub.Parse(buf)
// 	assert.NoError(t, err)
// 	err = sub.CheckParseStateAfterOpForTests()
// 	assert.NoError(t, err)
// }
