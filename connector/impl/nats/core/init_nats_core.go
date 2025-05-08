//go:build nats_core

package core

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/util"
	"github.com/ValerySidorin/fujin/connector/writer"
)

func init() {
	writer.RegisterWriterFactory(protocol.NatsCore, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("nats_core writer factory: failed to convert config: %w", err)
		}
		return NewWriter(typedConfig, l)
	})

	reader.RegisterReaderFactory(protocol.NatsCore, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("nats_core reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, l)
	})
}
