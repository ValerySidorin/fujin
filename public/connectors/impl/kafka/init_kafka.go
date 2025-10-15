//go:build kafka

package kafka

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/util"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("kafka",
		func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
			var typedConfig WriterConfig
			if writerConfig, ok := rawBrokerConfig.(WriterConfig); ok {
				typedConfig = writerConfig
			} else {
				if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
					return nil, fmt.Errorf("kafka writer factory: failed to convert config: %w", err)
				}
			}
			if err := typedConfig.Validate(); err != nil {
				return nil, fmt.Errorf("kafka writer factory: invalid config: %w", err)
			}
			return NewWriter(typedConfig, writerID, l)
		},
		func(conf map[string]any) string {
			brokersRaw, ok := conf["brokers"].([]any)
			if !ok {
				return ""
			}
			var brokersStr []string
			for _, b := range brokersRaw {
				if brokerStr, ok := b.(string); ok {
					brokersStr = append(brokersStr, brokerStr)
				}
			}
			return strings.Join(brokersStr, ",")
		},
	)

	reader.RegisterReaderFactory("kafka", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if readerConfig, ok := rawBrokerConfig.(ReaderConfig); ok {
			typedConfig = readerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("kafka reader factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("kafka reader factory: invalid config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
