//go:build mqtt

package mqtt

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/internal/connector/reader"
	"github.com/ValerySidorin/fujin/internal/connector/util"
	"github.com/ValerySidorin/fujin/internal/connector/writer"
)

func init() {
	writer.RegisterWriterFactory(protocol.MQTT, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("mqtt writer factory: failed to convert config: %w", err)
		}
		return NewWriter(typedConfig, l)
	})

	reader.RegisterReaderFactory(protocol.MQTT, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig MQTTConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("mqtt reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
