package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/reader/config"
)

type ReaderType byte

const (
	Unknown ReaderType = iota
	Subscriber
	Consumer
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error
	SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error
	Fetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, args ...any),
	)
	FetchH(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
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

type ReaderFactoryFunc func(brokerSpecificConfig any, autoCommit bool, l *slog.Logger) (Reader, error)

var readerFactories = make(map[string]ReaderFactoryFunc)

func RegisterReaderFactory(protocol string, factory ReaderFactoryFunc) {
	readerFactories[protocol] = factory
}

func New(conf config.Config, autoCommit bool, l *slog.Logger) (Reader, error) {
	factory, ok := readerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported reader protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, autoCommit, l)
}
