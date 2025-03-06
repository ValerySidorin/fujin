package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
	"github.com/ValerySidorin/fujin/mq/protocol"
	"github.com/ValerySidorin/fujin/mq/writer/config"
)

type Writer interface {
	Write(ctx context.Context, msg []byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
	Endpoint() string
	Close() error
}

func NewWriter(conf config.Config, producerID string, l *slog.Logger) (Writer, error) {
	switch conf.Protocol {
	case protocol.Kafka:
		return kafka.NewWriter(conf.Kafka, producerID, l)
	case protocol.Nats:
		return nats.NewWriter(conf.Nats, producerID, l)
	}

	return nil, fmt.Errorf("invalid writer protocol: %s", conf.Protocol)
}
