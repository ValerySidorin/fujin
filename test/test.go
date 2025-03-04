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
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/config"
	"github.com/ValerySidorin/fujin/mq"
	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
	reader_config "github.com/ValerySidorin/fujin/mq/reader/config"
	writer_config "github.com/ValerySidorin/fujin/mq/writer/config"
	"github.com/ValerySidorin/fujin/server"
	"github.com/ValerySidorin/fujin/server/fujin"
	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
	"github.com/ValerySidorin/fujin/server/fujin/proto/response"
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
	MQ: mq.Config{
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
	MQ: mq.Config{
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nats",
				Nats: nats.ReaderConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
				},
			},
		},
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nats",
				Nats: nats.WriterConfig{
					URL:     "nats://localhost:4222",
					Subject: "my_subject",
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

func RunServer(ctx context.Context, conf config.Config) *server.Server {
	s, _ := server.NewServer(conf, slog.New(slog.DiscardHandler))

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
		byte(request.OP_CODE_CONNECT_PRODUCER),
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
