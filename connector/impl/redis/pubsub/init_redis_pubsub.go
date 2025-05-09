//go:build redis_pubsub

package pubsub

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
	writer.RegisterWriterFactory(protocol.RedisPubSub, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("redis_pubsub writer factory: failed to convert config: %w", err)
		}
		if len(typedConfig.InitAddress) > 0 {
			typedConfig.Endpoint = strings.Join(typedConfig.InitAddress, ",")
		}
		return NewWriter(typedConfig, l)
	})

	reader.RegisterReaderFactory(protocol.RedisPubSub, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("redis_pubsub reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, l)
	})
}
