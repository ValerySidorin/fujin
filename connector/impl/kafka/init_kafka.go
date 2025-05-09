//go:build kafka

package kafka

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/internal/connector/reader"
	"github.com/ValerySidorin/fujin/internal/connector/util"
	"github.com/ValerySidorin/fujin/internal/connector/writer"
)

func init() {
	writer.RegisterWriterFactory(protocol.Kafka, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("kafka writer factory: failed to convert config: %w", err)
		}

		if len(typedConfig.Brokers) > 0 {
			typedConfig.Endpoint = strings.Join(typedConfig.Brokers, ",")
		}

		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("kafka writer factory: invalid config: %w", err)
		}
		return NewWriter(typedConfig, writerID, l)
	})

	reader.RegisterReaderFactory(protocol.Kafka, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("kafka reader factory: failed to convert config: %w", err)
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("kafka reader factory: invalid config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
