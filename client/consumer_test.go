package client_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/client"
	"github.com/ValerySidorin/fujin/test"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConnectConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, shutdown := RunTestServer(ctx)
	defer shutdown()

	conf := client.ReaderConfig{
		Topic:      "sub",
		AutoCommit: true,
	}

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

	consumer, err := conn.ConnectConsumer(conf)
	if err != nil {
		t.Fatalf("failed to connect consumer: %v", err)
	}
	defer consumer.Close()
	assert.NoError(t, consumer.CheckParseStateAfterOpForTests())
}

func TestFetch(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs, shutdown := RunTestServer(ctx)
		defer func() {
			cancel()
			shutdown()
			<-fs.Done()
		}()

		conf := client.ReaderConfig{
			Topic:      "sub",
			AutoCommit: true,
		}

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
			t.Fatalf("failed to connectd: %v", err)
		}
		defer conn.Close()

		consumer, err := conn.ConnectConsumer(conf)
		if err != nil {
			t.Fatalf("failed to connect consumer: %v", err)
		}
		defer consumer.Close()
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())

		_, err = consumer.Fetch(ctx, 1)
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs := test.RunDefaultServerWithKafka3Brokers(ctx)
		defer func() {
			cancel()
			<-fs.Done()
		}()

		conf := client.ReaderConfig{
			Topic:      "sub",
			AutoCommit: true,
		}

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

		consumer, err := conn.ConnectConsumer(conf)
		if err != nil {
			t.Fatalf("failed to connect consumer: %v", err)
		}
		defer consumer.Close()
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())

		err = produce(ctx, "my_pub_topic", "test message consumer")
		assert.NoError(t, err)

		received := make([]client.Msg, 0)

		msgs, err := consumer.Fetch(ctx, 1)
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())
		assert.NoError(t, err)

		for _, msg := range msgs {
			fmt.Println(msg)
			received = append(received, msg)
		}

		assert.NoError(t, err)
		if len(received) != 1 {
			t.Fatal("invalid number of received messages")
		}
		assert.Equal(t, "test message consumer", string(received[0].Value))
	})

	t.Run("manual ack", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fs := test.RunDefaultServerWithKafka3Brokers(ctx)
		defer func() {
			cancel()
			<-fs.Done()
		}()

		conf := client.ReaderConfig{
			Topic:      "sub",
			AutoCommit: false,
		}

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

		consumer, err := conn.ConnectConsumer(conf)
		if err != nil {
			t.Fatalf("failed to connect consumer: %v", err)
		}
		defer consumer.Close()
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())

		err = produce(ctx, "my_pub_topic", "test message consumer manual")
		assert.NoError(t, err)

		received := make([][]byte, 0)

		msgs, err := consumer.Fetch(ctx, 1)
		assert.NoError(t, consumer.CheckParseStateAfterOpForTests())
		assert.NoError(t, err)

		for _, msg := range msgs {
			fmt.Println(string(msg.Value))
			received = append(received, msg.Value)
			if err := msg.Ack(); err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, consumer.CheckParseStateAfterOpForTests())
		}

		if len(received) != 1 {
			t.Fatal("invalid number of received messages")
		}
		assert.Equal(t, "test message consumer manual", string(received[0]))
	})
}

func produce(ctx context.Context, topic, msg string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092", "localhost:9093", "localhost:9094"),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer cl.Close()

	var wg sync.WaitGroup
	defer wg.Wait()

	record := &kgo.Record{Topic: topic, Value: []byte(msg)}
	wg.Add(1)
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}
	})

	return nil
}
