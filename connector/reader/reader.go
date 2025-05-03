package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	nats_core "github.com/ValerySidorin/fujin/connector/impl/nats/core"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	redis_streams "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader/config"
)

type ReaderType byte

const (
	Unknown ReaderType = iota
	Subscriber
	Consumer
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error
	Fetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, args ...any),
	)
	Ack(
		ctx context.Context, msgIDs [][]byte,
		ackHandler func(error),
		ackMsgHandler func([]byte, error),
	)
	Nack(
		ctx context.Context, msgIDs [][]byte,
		nackHandler func(error),
		nackMsgHandler func([]byte, error),
	)
	MsgIDStaticArgsLen() int
	EncodeMsgID(buf []byte, topic string, args ...any) []byte
	IsAutoCommit() bool
	Close()
}

func New(conf config.Config, autoCommit bool, l *slog.Logger) (Reader, error) {
	switch conf.Protocol {
	case protocol.Kafka:
		return kafka.NewReader(conf.Kafka, autoCommit, l)
	case protocol.NatsCore:
		return nats_core.NewReader(conf.NatsCore, l)
	case protocol.AMQP091:
		return amqp091.NewReader(conf.AMQP091, autoCommit, l)
	case protocol.AMQP10:
		return amqp10.NewReader(conf.AMQP10, autoCommit, l)
	case protocol.RedisPubSub:
		return redis_pubsub.NewReader(conf.RedisPubSub, l)
	case protocol.RedisStreams:
		return redis_streams.NewReader(conf.RedisStreams, autoCommit, l)
	}

	return nil, fmt.Errorf("invalid reader protocol: %s", conf.Protocol)
}
