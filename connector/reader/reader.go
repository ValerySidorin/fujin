package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	"github.com/ValerySidorin/fujin/connector/impl/nats"
	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader/config"
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, args ...any) error) error
	Consume(ctx context.Context, ch <-chan struct{}, n uint32, h func(message []byte, args ...any) error) error
	Ack(ctx context.Context, meta []byte) error
	Nack(ctx context.Context, meta []byte) error
	MessageMetaLen() byte
	EncodeMeta(buf []byte, args ...any) []byte
	IsAutoCommit() bool
	Close()
}

func New(conf config.Config, l *slog.Logger) (Reader, error) {
	switch conf.Protocol {
	case protocol.Kafka:
		return kafka.NewReader(conf.Kafka, l)
	case protocol.Nats:
		return nats.NewReader(conf.Nats, l)
	case protocol.AMQP091:
		return amqp091.NewReader(conf.AMQP091, l)
	case protocol.AMQP10:
		return amqp10.NewReader(conf.AMQP10, l)
	}

	return nil, fmt.Errorf("invalid reader protocol: %s", conf.Protocol)
}
