package test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/config"
	"github.com/ValerySidorin/fujin/connector"
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	nats_streaming "github.com/ValerySidorin/fujin/connector/impl/nats/streaming"
	"github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	reader_config "github.com/ValerySidorin/fujin/connector/reader/config"
	writer_config "github.com/ValerySidorin/fujin/connector/writer/config"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/ValerySidorin/fujin/server"
	"github.com/ValerySidorin/fujin/server/fujin"
	"github.com/quic-go/quic-go"
)

const (
	defaultSendBufSize = 32768
	defaultRecvBufSize = 32768
)

var DefaultFujinServerTestConfig = fujin.ServerConfig{
	Addr: ":4848",
	TLS:  generateTLSConfig(),
}

var DefaultTestConfigWithKafka3Brokers = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "kafka",
				Kafka: kafka.ReaderConfig{
					Brokers:                []string{"localhost:9092", "localhost:9093", "localhost:9094"},
					Topic:                  "my_pub_topic",
					Group:                  "fujin",
					AllowAutoTopicCreation: true,
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "kafka",
				Kafka: kafka.WriterConfig{
					Brokers:                []string{"localhost:9092", "localhost:9093", "localhost:9094"},
					Topic:                  "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
}

var DefaultTestConfigWithNats = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nats_streaming",
				NatsStreaming: nats_streaming.ReaderConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nats_streaming",
				NatsStreaming: nats_streaming.WriterConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
				},
			},
		},
	},
}

var DefaultTestConfigWithAMQP091 = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp091",
				AMQP091: amqp091.ReaderConfig{
					Conn: amqp091.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp091.ExchangeConfig{
						Name: "events",
						Kind: "fanout",
					},
					Queue: amqp091.QueueConfig{
						Name: "my_queue",
					},
					QueueBind: amqp091.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Consume: amqp091.ConsumeConfig{
						Consumer: "fujin",
					},
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp091",
				AMQP091: amqp091.WriterConfig{
					Conn: amqp091.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp091.ExchangeConfig{
						Name: "events",
						Kind: "fanout",
					},
					Queue: amqp091.QueueConfig{
						Name: "my_queue",
					},
					QueueBind: amqp091.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Publish: amqp091.PublishConfig{
						ContentType: "text/plain",
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithAMQP10 = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp10",
				AMQP10: amqp10.ReaderConfig{
					Conn: amqp10.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Receiver: amqp10.ReceiverConfig{
						Source: "queue",
					},
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp10",
				AMQP10: amqp10.WriterConfig{
					Conn: amqp10.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Sender: amqp10.SenderConfig{
						Target: "queue",
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithRedisPubSub = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "redis_pubsub",
				RedisPubSub: pubsub.ReaderConfig{
					InitAddress: []string{"localhost:6379"},
					Channel:     "channel",
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "redis_pubsub",
				RedisPubSub: pubsub.WriterConfig{
					InitAddress: []string{"localhost:6379"},
					Channel:     "channel",
					BatchSize:   10000,
					Linger:      5 * time.Millisecond,
				},
			},
		},
	},
}

func RunDefaultServerWithKafka3Brokers(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithKafka3Brokers)
}

func RunDefaultServerWithNats(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNats)
}

func RunDefaultServerWithAMQP091(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP091)
}

func RunDefaultServerWithAMQP10(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP10)
}

func RunDefaultServerWithRedisPubSub(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisPubSub)
}

func RunServer(ctx context.Context, conf config.Config) *server.Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	s, _ := server.NewServer(conf, logger)

	go func() {
		if err := s.ListenAndServe(ctx); err != nil {
			panic(fmt.Errorf("Unable to start fujin server: %w", err))
		}
	}()

	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start fujin server: timeout")
	}

	return s
}

func createClientConn(ctx context.Context, addr string) quic.Connection {
	conn, err := quic.DialAddr(ctx, addr, generateTLSConfig(), nil)
	if err != nil {
		panic(fmt.Errorf("dial addr: %w", err))
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				str, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}

				handlePing(str)
			}
		}
	}()

	return conn
}

func doDefaultConnectProducer(conn quic.Connection) quic.Stream {
	req := []byte{
		byte(request.OP_CODE_CONNECT_WRITER),
		0, 0, 0, 0, // producer id is optional (for transactions)
	}

	str, err := conn.OpenStream()
	if err != nil {
		panic(fmt.Errorf("open stream: %w", err))
	}

	if _, err := str.Write(req); err != nil {
		panic(fmt.Errorf("write connect producer request: %w", err))
	}

	return str
}

func drainStream(b *testing.B, str quic.Stream, ch chan int) {
	buf := make([]byte, defaultRecvBufSize)
	bytes := 0

	for {
		str.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := str.Read(buf)
		bytes += n
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.Errorf("Error on read: %v\n", err)
			}
			break
		}
	}

	ch <- bytes
}

func handlePing(str quic.Stream) {
	defer str.Close()
	var pingBuf [1]byte

	n, err := str.Read(pingBuf[:])
	if err == io.EOF {
		if n != 0 {
			if pingBuf[0] != byte(request.OP_CODE_PING) {
				return
			}
		}
		pingBuf[0] = byte(response.RESP_CODE_PONG)
		if _, err := str.Write(pingBuf[:]); err != nil {
			return
		}
		return
	}
	if err != nil {
		return
	}
}

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{"fujin"}}
}
