package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/writer/config"
)

var (
	writerFactories                = make(map[string]WriterFactoryFunc)
	writerConfigEndpointParseFuncs = make(map[string]WriterConfigEndpointParseFunc)

	DefaultConfigEndpointParser = func(conf map[string]any) string {
		return ""
	}
)

type Writer interface {
	Write(ctx context.Context, msg []byte, callback func(err error))
	WriteH(ctx context.Context, msg []byte, headers [][]byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
	Endpoint() string
	Close() error
}

type WriterFactoryFunc func(brokerSpecificConfig any, writerID string, l *slog.Logger) (Writer, error)

type WriterConfigEndpointParseFunc func(conf map[string]any) string

func RegisterWriterFactory(protocol string, factory WriterFactoryFunc, parser WriterConfigEndpointParseFunc) {
	writerFactories[protocol] = factory
	writerConfigEndpointParseFuncs[protocol] = parser
}

func NewWriter(conf config.Config, writerID string, l *slog.Logger) (Writer, error) {
	factory, ok := writerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported writer protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, writerID, l)
}

func ParseEndpoint(conf config.Config) (string, error) {
	parser, ok := writerConfigEndpointParseFuncs[conf.Protocol]
	if !ok {
		return "", fmt.Errorf("writer config endpoint parser not found for protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return parser(conf.Settings.(map[string]any)), nil
}
