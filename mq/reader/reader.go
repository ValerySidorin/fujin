package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
	"github.com/ValerySidorin/fujin/mq/reader/config"
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, args ...any) error) error
	Consume(ctx context.Context, ch <-chan struct{}, n uint32, h func(message []byte, args ...any) error) error
	Ack(ctx context.Context, meta []byte) error
	NAck(ctx context.Context, meta []byte) error
	MessageMetaLen() byte
	EncodeMeta(buf []byte, args ...any) []byte
	IsAutoCommit() bool
	Close()
}

func New(conf config.Config, l *slog.Logger) (Reader, error) {
	switch conf.Protocol {
	case "kafka":
		return kafka.NewReader(conf.Kafka, l)
	case "nats":
		return nats.NewReader(conf.Nats, l)
	}

	return nil, fmt.Errorf("invalid reader protocol: %s", conf.Protocol)
}
