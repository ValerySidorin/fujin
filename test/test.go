package test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/big"
	mathrand "math/rand"
	"net"
	"os"
	"testing"
	"time"

	_ "github.com/fujin-io/fujin/public/plugins/transport/all"

	"github.com/fujin-io/fujin/public/plugins/connector/azure/amqp1"
	connector_config "github.com/fujin-io/fujin/public/plugins/connector/config"
	kafka "github.com/fujin-io/fujin/public/plugins/connector/kafka/franz"
	mqtt "github.com/fujin-io/fujin/public/plugins/connector/mqtt/paho"
	"github.com/fujin-io/fujin/public/plugins/connector/nats/core"
	"github.com/fujin-io/fujin/public/plugins/connector/nats/jetstream"
	"github.com/fujin-io/fujin/public/plugins/connector/nsq"
	"github.com/fujin-io/fujin/public/plugins/connector/rabbitmq/amqp09"
	redis_config "github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/config"
	"github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/pubsub"
	"github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/streams"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/server"
	"github.com/fujin-io/fujin/public/server/config"
	natsgo "github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/quic-go/quic-go"
)

const (
	defaultSendBufSize  = 512 * 1024
	defaultRecvBufSize  = 512 * 1024
	PERF_TCP_ADDR       = "localhost:4850"
	PERF_UNIX_PATH      = "/tmp/fujin-bench.sock"
	PERF_WEBSOCKET_ADDR = "localhost:4851"
	PERF_WEBSOCKET_PATH = "/fujin"
)

var DefaultQUICServerTestConfig = config.QUICServerConfig{
	Enabled: true,
	Addr:    ":4848",
	TLS:     generateTLSConfig(),
}

var DefaultGRPCServerTestConfig = config.GRPCServerConfig{
	Enabled: true,
	Addr:    ":4849",
	TLS:     generateTLSConfig(),
}

var DefaultTestConfigWithNopQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nop",
	},
})

func MakeGenConfig(msgSize int) connector_config.ConnectorsConfig {
	return connector_config.ConnectorsConfig{
		"connector": {
			Type:     "gen",
			Settings: GenConfig{MsgSize: msgSize},
		},
	}
}

func makeBoundedGenConfig(msgSize int, subscribeLimit uint64) connector_config.ConnectorsConfig {
	config := MakeGenConfig(msgSize)
	connectorConfig := config["connector"]
	connectorConfig.Settings = GenConfig{MsgSize: msgSize, SubscribeLimit: subscribeLimit}
	config["connector"] = connectorConfig
	return config
}

func RunServerWithGenQUIC(ctx context.Context, msgSize int, subscribeLimit uint64) *server.Server {
	return RunServer(ctx, MakeConfigWithQUIC(makeBoundedGenConfig(msgSize, subscribeLimit)))
}

func RunServerWithGenTCP(ctx context.Context, msgSize int, subscribeLimit uint64) *server.Server {
	return RunServer(ctx, MakeConfigWithTCP(makeBoundedGenConfig(msgSize, subscribeLimit)))
}

func RunServerWithGenUnix(ctx context.Context, msgSize int, subscribeLimit uint64) *server.Server {
	return RunServer(ctx, MakeConfigWithUnix(makeBoundedGenConfig(msgSize, subscribeLimit)))
}

var DefaultTestConfigWithKafka3BrokersQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "kafka_franz",
		Settings: kafka.Config{
			Common: kafka.CommonSettings{
				Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			},
			Routes: map[string]kafka.RouteSettings{
				"sub": {
					ConsumeTopics:          []string{"my_pub_topic"},
					Group:                  "fujin",
					AllowAutoTopicCreation: true,
				},
				"pub": {
					ProduceTopic:           "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
})

var DefaultTestConfigWithNatsQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nats_core",
		Settings: core.Config{
			Common: core.CommonSettings{
				URL: "nats://localhost:4222",
			},
			Routes: map[string]core.RouteSettings{
				"pub": {
					Subject: "my_subject",
				},
				"sub": {
					Subject: "my_subject",
				},
			},
		},
	},
})

var DefaultTestConfigWithAMQP091QUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "rabbitmq_amqp09",
		Settings: amqp09.Config{
			Routes: map[string]amqp09.RouteSettings{
				"pub": {
					Conn: amqp09.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp09.ExchangeConfig{
						Name:    "events",
						Kind:    "fanout",
						Durable: true,
					},
					Queue: amqp09.QueueConfig{
						Name:    "my_queue",
						Durable: true,
					},
					QueueBind: amqp09.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Publish: &amqp09.PublishConfig{
						ContentType: "text/plain",
					},
				},
				"sub": {
					Conn: amqp09.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp09.ExchangeConfig{
						Name:    "events",
						Kind:    "fanout",
						Durable: true,
					},
					Queue: amqp09.QueueConfig{
						Name:    "my_queue",
						Durable: true,
					},
					QueueBind: amqp09.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Consume: &amqp09.ConsumeConfig{
						Consumer: "fujin",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithAMQP10QUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "azure_amqp1",
		Settings: amqp1.Config{
			Routes: map[string]amqp1.RouteSettings{
				"pub": {
					Conn: amqp1.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Sender: &amqp1.SenderConfig{
						Target: "queue",
					},
				},
				"sub": {
					Conn: amqp1.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Receiver: &amqp1.ReceiverConfig{
						Source: "queue",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithRedisPubSubQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "redis_rueidis_pubsub",
		Settings: pubsub.Config{
			Common: pubsub.CommonSettings{
				RedisConfig: redis_config.RedisConfig{
					InitAddress:  []string{"localhost:6379"},
					DisableCache: true,
				},
				WriterBatchConfig: redis_config.WriterBatchConfig{
					BatchSize: 1000,
					Linger:    5 * time.Millisecond,
				},
			},
			Routes: map[string]pubsub.RouteSettings{
				"pub": {
					Channel: "channel",
				},
				"sub": {
					Channels: []string{"channel"},
				},
			},
		},
	},
})

var DefaultTestConfigWithRedisStreamsQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "redis_rueidis_streams",
		Settings: streams.Config{
			Common: streams.CommonSettings{
				RedisConfig: redis_config.RedisConfig{
					InitAddress:  []string{"localhost:6379"},
					DisableCache: true,
				},
				WriterBatchConfig: redis_config.WriterBatchConfig{
					BatchSize: 1000,
					Linger:    5 * time.Millisecond,
				},
			},
			Routes: map[string]streams.RouteSettings{
				"pub": {
					Stream: "stream",
				},
				"sub": {
					Streams: map[string]streams.StreamConf{
						"stream": {
							StartID:       ">",
							GroupCreateID: "$",
						},
					},
					Group: streams.GroupConf{
						Name:     "fujin",
						Consumer: "fujin",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithMQTTQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "mqtt_paho",
		Settings: mqtt.Config{
			Common: mqtt.CommonSettings{
				BrokerURL:         "tcp://localhost:1883",
				KeepAlive:         2,
				DisconnectTimeout: 5 * time.Second,
				ConnectTimeout:    300 * time.Second,
			},
			Routes: map[string]mqtt.RouteSettings{
				"pub": {
					ClientID:      "fujin_pub",
					Topic:         "my_topic",
					QoS:           0,
					Retain:        true,
					CleanStart:    true,
					SessionExpiry: 5,
					Pool: mqtt.PoolConfig{
						Size:           1000000,
						PreAlloc:       true,
						ReleaseTimeout: 30 * time.Second,
					},
				},
				"sub": {
					ClientID:         "fujin_sub",
					Topic:            "my_topic",
					QoS:              0,
					Retain:           false,
					CleanStart:       true,
					SessionExpiry:    0,
					SendAcksInterval: 50 * time.Millisecond,
					AckTTL:           5 * time.Minute,
				},
			},
		},
	},
})

var DefaultTestConfigWithNSQQUIC = MakeConfigWithQUIC(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nsq",
		Settings: nsq.Config{
			Common: nsq.CommonSettings{
				Address:   "localhost:4150",
				Addresses: []string{"localhost:4150"},
			},
			Routes: map[string]nsq.RouteSettings{
				"pub": {
					Topic: "my_topic",
					Pool: nsq.PoolConfig{
						Size:           1000000,
						PreAlloc:       true,
						ReleaseTimeout: 5 * time.Second,
					},
				},
				"sub": {
					Topic:       "my_topic",
					Channel:     "my_channel",
					MaxInFlight: 1000000,
				},
			},
		},
	},
})

func RunDefaultServerWithNopQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNopQUIC)
}

func RunDefaultServerWithKafka3BrokersQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithKafka3BrokersQUIC)
}

func RunDefaultServerWithNatsQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNatsQUIC)
}

func RunDefaultServerWithAMQP091QUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP091QUIC)
}

func RunDefaultServerWithAMQP10QUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP10QUIC)
}

func RunDefaultServerWithRedisPubSubQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisPubSubQUIC)
}

func RunDefaultServerWithRedisStreamsQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisStreamsQUIC)
}

func RunDefaultServerWithMQTTQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithMQTTQUIC)
}

func RunDefaultServerWithNSQQUIC(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNSQQUIC)
}

// --- TCP test configs ---

var DefaultTCPServerTestConfig = config.TCPServerConfig{
	Enabled: true,
	Addr:    ":4850",
}

var DefaultTestConfigWithNopTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nop",
	},
})

var DefaultTestConfigWithKafka3BrokersTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "kafka_franz",
		Settings: kafka.Config{
			Common: kafka.CommonSettings{
				Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			},
			Routes: map[string]kafka.RouteSettings{
				"sub": {
					ConsumeTopics:          []string{"my_pub_topic"},
					Group:                  "fujin",
					AllowAutoTopicCreation: true,
				},
				"pub": {
					ProduceTopic:           "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
	// Separate connector with its own consumer group for ack/nack tests.
	// Avoids Kafka consumer group rebalancing conflicts with subscribe/fetch tests.
	"ack_connector": {
		Type: "kafka_franz",
		Settings: kafka.Config{
			Common: kafka.CommonSettings{
				Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			},
			Routes: map[string]kafka.RouteSettings{
				"sub": {
					ConsumeTopics:          []string{"my_pub_topic"},
					Group:                  "fujin_ack",
					AllowAutoTopicCreation: true,
				},
				"pub": {
					ProduceTopic:           "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
	"nack_connector": {
		Type: "kafka_franz",
		Settings: kafka.Config{
			Common: kafka.CommonSettings{
				Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			},
			Routes: map[string]kafka.RouteSettings{
				"sub": {
					ConsumeTopics:          []string{"my_pub_topic"},
					Group:                  "fujin_nack",
					AllowAutoTopicCreation: true,
				},
				"pub": {
					ProduceTopic:           "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
})

var DefaultTestConfigWithNatsTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nats_core",
		Settings: core.Config{
			Common: core.CommonSettings{
				URL: "nats://localhost:4222",
			},
			Routes: map[string]core.RouteSettings{
				"pub": {
					Subject: "my_subject",
				},
				"sub": {
					Subject: "my_subject",
				},
			},
		},
	},
})

var DefaultTestConfigWithNatsJetstreamTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nats_jetstream",
		Settings: jetstream.Config{
			Common: jetstream.CommonSettings{
				URL:    "nats://localhost:4222",
				Stream: "e2e_stream",
			},
			Routes: map[string]jetstream.RouteSettings{
				"pub": {
					Subject: "e2e.subject",
				},
				"sub": {
					Subject:  "e2e.subject",
					Consumer: "e2e_consumer",
				},
			},
		},
	},
	// Separate connectors with own durable consumers to avoid conflicts.
	"ack_connector": {
		Type: "nats_jetstream",
		Settings: jetstream.Config{
			Common: jetstream.CommonSettings{
				URL:    "nats://localhost:4222",
				Stream: "e2e_stream",
			},
			Routes: map[string]jetstream.RouteSettings{
				"pub": {
					Subject: "e2e.subject",
				},
				"sub": {
					Subject:  "e2e.subject",
					Consumer: "e2e_ack_consumer",
				},
			},
		},
	},
	"nack_connector": {
		Type: "nats_jetstream",
		Settings: jetstream.Config{
			Common: jetstream.CommonSettings{
				URL:    "nats://localhost:4222",
				Stream: "e2e_stream",
			},
			Routes: map[string]jetstream.RouteSettings{
				"pub": {
					Subject: "e2e.subject",
				},
				"sub": {
					Subject:  "e2e.subject",
					Consumer: "e2e_nack_consumer",
				},
			},
		},
	},
})

var DefaultTestConfigWithAMQP091TCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "rabbitmq_amqp09",
		Settings: amqp09.Config{
			Routes: map[string]amqp09.RouteSettings{
				"pub": {
					Conn: amqp09.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp09.ExchangeConfig{
						Name:    "events",
						Kind:    "fanout",
						Durable: true,
					},
					Queue: amqp09.QueueConfig{
						Name:    "my_queue",
						Durable: true,
					},
					QueueBind: amqp09.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Publish: &amqp09.PublishConfig{
						ContentType: "text/plain",
					},
				},
				"sub": {
					Conn: amqp09.ConnConfig{
						URL: "amqp://guest:guest@localhost",
					},
					Exchange: amqp09.ExchangeConfig{
						Name:    "events",
						Kind:    "fanout",
						Durable: true,
					},
					Queue: amqp09.QueueConfig{
						Name:    "my_queue",
						Durable: true,
					},
					QueueBind: amqp09.QueueBindConfig{
						RoutingKey: "my_routing_key",
					},
					Consume: &amqp09.ConsumeConfig{
						Consumer: "fujin",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithAMQP10TCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "azure_amqp1",
		Settings: amqp1.Config{
			Routes: map[string]amqp1.RouteSettings{
				"pub": {
					Conn: amqp1.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Sender: &amqp1.SenderConfig{
						Target: "queue",
					},
				},
				"sub": {
					Conn: amqp1.ConnConfig{
						Addr: "amqp://artemis:artemis@localhost:61616",
					},
					Receiver: &amqp1.ReceiverConfig{
						Source: "queue",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithRedisPubSubTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "redis_rueidis_pubsub",
		Settings: pubsub.Config{
			Common: pubsub.CommonSettings{
				RedisConfig: redis_config.RedisConfig{
					InitAddress:  []string{"localhost:6379"},
					DisableCache: true,
				},
				WriterBatchConfig: redis_config.WriterBatchConfig{
					BatchSize: 1000,
					Linger:    5 * time.Millisecond,
				},
			},
			Routes: map[string]pubsub.RouteSettings{
				"pub": {
					Channel: "channel",
				},
				"sub": {
					Channels: []string{"channel"},
				},
			},
		},
	},
})

var DefaultTestConfigWithRedisStreamsTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "redis_rueidis_streams",
		Settings: streams.Config{
			Common: streams.CommonSettings{
				RedisConfig: redis_config.RedisConfig{
					InitAddress:  []string{"localhost:6379"},
					DisableCache: true,
				},
				WriterBatchConfig: redis_config.WriterBatchConfig{
					BatchSize: 1000,
					Linger:    5 * time.Millisecond,
				},
			},
			Routes: map[string]streams.RouteSettings{
				"pub": {
					Stream: "stream",
				},
				"sub": {
					Streams: map[string]streams.StreamConf{
						"stream": {
							StartID:       ">",
							GroupCreateID: "$",
						},
					},
					Group: streams.GroupConf{
						Name:     "fujin",
						Consumer: "fujin",
					},
				},
			},
		},
	},
})

var DefaultTestConfigWithMQTTTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "mqtt_paho",
		Settings: mqtt.Config{
			Common: mqtt.CommonSettings{
				BrokerURL:         "tcp://localhost:1883",
				KeepAlive:         2,
				DisconnectTimeout: 5 * time.Second,
				ConnectTimeout:    300 * time.Second,
			},
			Routes: map[string]mqtt.RouteSettings{
				"pub": {
					ClientID:      "fujin_pub",
					Topic:         "my_topic",
					QoS:           0,
					Retain:        true,
					CleanStart:    true,
					SessionExpiry: 5,
					Pool: mqtt.PoolConfig{
						Size:           1000000,
						PreAlloc:       true,
						ReleaseTimeout: 30 * time.Second,
					},
				},
				"sub": {
					ClientID:         "fujin_sub",
					Topic:            "my_topic",
					QoS:              0,
					Retain:           false,
					CleanStart:       true,
					SessionExpiry:    0,
					SendAcksInterval: 50 * time.Millisecond,
					AckTTL:           5 * time.Minute,
				},
			},
		},
	},
})

var DefaultTestConfigWithNSQTCP = MakeConfigWithTCP(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nsq",
		Settings: nsq.Config{
			Common: nsq.CommonSettings{
				Address:   "localhost:4150",
				Addresses: []string{"localhost:4150"},
			},
			Routes: map[string]nsq.RouteSettings{
				"pub": {
					Topic: "my_topic",
					Pool: nsq.PoolConfig{
						Size:           1000000,
						PreAlloc:       true,
						ReleaseTimeout: 5 * time.Second,
					},
				},
				"sub": {
					Topic:       "my_topic",
					Channel:     "my_channel",
					MaxInFlight: 1000000,
				},
			},
		},
	},
})

func RunDefaultServerWithNopTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNopTCP)
}

func RunDefaultServerWithKafka3BrokersTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithKafka3BrokersTCP)
}

func RunDefaultServerWithNatsTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNatsTCP)
}

func RunDefaultServerWithNatsJetstreamTCP(ctx context.Context) *server.Server {
	// JetStream requires the stream to exist before use.
	nc, err := natsgo.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("nats connect for stream setup: %v", err)
	}
	js, err := natsjs.New(nc)
	if err != nil {
		nc.Close()
		log.Fatalf("jetstream context: %v", err)
	}
	_, err = js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
		Name:     "e2e_stream",
		Subjects: []string{"e2e.>"},
	})
	if err != nil {
		nc.Close()
		log.Fatalf("create stream: %v", err)
	}
	nc.Close()

	return RunServer(ctx, DefaultTestConfigWithNatsJetstreamTCP)
}

func RunDefaultServerWithAMQP091TCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP091TCP)
}

func RunDefaultServerWithAMQP10TCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP10TCP)
}

func RunDefaultServerWithRedisPubSubTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisPubSubTCP)
}

func RunDefaultServerWithRedisStreamsTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisStreamsTCP)
}

func RunDefaultServerWithMQTTTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithMQTTTCP)
}

func RunDefaultServerWithNSQTCP(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNSQTCP)
}

func createTCPClientConn(addr string) net.Conn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(fmt.Errorf("dial tcp: %w", err))
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return conn
}

func doDefaultBindTCP(conn net.Conn) {
	req := bindCmd("connector", nil, nil)
	if _, err := conn.Write(req); err != nil {
		panic(fmt.Errorf("write bind request: %w", err))
	}
}

// --- Unix test configs ---

var DefaultTestConfigWithNopUnix = MakeConfigWithUnix(connector_config.ConnectorsConfig{
	"connector": {
		Type: "nop",
	},
})

var DefaultTestConfigWithKafka3BrokersUnix = MakeConfigWithUnix(connector_config.ConnectorsConfig{
	"connector": {
		Type: "kafka_franz",
		Settings: kafka.Config{
			Common: kafka.CommonSettings{
				Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			},
			Routes: map[string]kafka.RouteSettings{
				"sub": {
					ConsumeTopics:          []string{"my_pub_topic"},
					Group:                  "fujin",
					AllowAutoTopicCreation: true,
				},
				"pub": {
					ProduceTopic:           "my_pub_topic",
					AllowAutoTopicCreation: true,
					Linger:                 10 * time.Millisecond,
				},
			},
		},
	},
})

func RunDefaultServerWithNopUnix(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNopUnix)
}

func RunDefaultServerWithKafka3BrokersUnix(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithKafka3BrokersUnix)
}

func createUnixClientConn(path string) net.Conn {
	conn, err := net.Dial("unix", path)
	if err != nil {
		panic(fmt.Errorf("dial unix: %w", err))
	}
	return conn
}

func doDefaultBindUnix(conn net.Conn) {
	doDefaultBindTCP(conn) // same for any net.Conn
}

func drainUnixConn(b *testing.B, conn net.Conn, ch chan int) {
	drainTCPConn(b, conn, ch)
}

func drainTCPConn(b *testing.B, conn net.Conn, ch chan int) {
	buf := make([]byte, defaultRecvBufSize)
	bytes := 0

	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
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

func RunServer(ctx context.Context, conf config.Config) *server.Server {
	output := io.Writer(os.Stdout)
	if os.Getenv("FUJIN_BENCH_QUIET") != "" {
		output = io.Discard
	}
	logger := slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	s, err := server.NewServer(conf, logger)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := s.ListenAndServe(ctx); err != nil {
			panic(fmt.Errorf("Unable to start fujin server: %w", err))
		}
	}()

	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start fujin server: timeout")
	}

	logger.Info("Server started")

	return s
}

func createClientConn(ctx context.Context, addr string) *quic.Conn {
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

func doDefaultBind(conn *quic.Conn) *quic.Stream {
	req := bindCmd("connector", nil, nil)

	str, err := conn.OpenStream()
	if err != nil {
		panic(fmt.Errorf("open stream: %w", err))
	}

	if _, err := str.Write(req); err != nil {
		panic(fmt.Errorf("write bind request: %w", err))
	}

	return str
}

func drainStream(b *testing.B, str *quic.Stream, ch chan int) {
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

func handlePing(str *quic.Stream) {
	defer str.Close()
	_ = respondToPing(str)
}

func respondToPing(str io.ReadWriter) error {
	var pingBuf [1]byte
	if _, err := io.ReadFull(str, pingBuf[:]); err != nil {
		return err
	}
	if pingBuf[0] != byte(v1.OP_CODE_PING) {
		return fmt.Errorf("unexpected ping opcode %d", pingBuf[0])
	}
	pingBuf[0] = byte(v1.RESP_CODE_PONG)
	_, err := str.Write(pingBuf[:])
	return err
}

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{v1.Version}}
}

func bindCmd(connector string, meta, configOverrides map[string]string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_BIND))
	buf = appendFujinString(buf, connector)
	buf = appendFujinUint16StringArray(buf, meta)
	buf = appendFujinUint16StringArray(buf, configOverrides)

	return buf
}

func appendFujinString(buf []byte, v string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
	buf = append(buf, v...)
	return buf
}

func appendFujinUint16StringArray(buf []byte, m map[string]string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
	for k, v := range m {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedBytes(sz int) []byte {
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[mathrand.Intn(len(ch))]
	}
	return b
}

func sizedString(sz int) string {
	return string(sizedBytes(sz))
}
