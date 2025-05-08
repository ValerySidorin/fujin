//go:build amqp091

package amqp091

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/util"
	"github.com/ValerySidorin/fujin/connector/writer"
)

func init() {
	writer.RegisterWriterFactory(protocol.AMQP091, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("amqp091 writer factory: failed to convert config: %w", err)
		}

		return NewWriter(typedConfig, l)
	})

	reader.RegisterReaderFactory(protocol.AMQP091, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("amqp091 reader factory: failed to convert config: %w", err)
		}

		return NewReader(typedConfig, autoCommit, l)
	})
}
