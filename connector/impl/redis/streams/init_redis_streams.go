//go:build redis_streams

package streams

import (
	"fmt"
	"log/slog"
	"strings"

	// Добавим, если понадобится для перезаписи Endpoint
	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/util"
	"github.com/ValerySidorin/fujin/connector/writer"
)

func init() {
	writer.RegisterWriterFactory(protocol.RedisStreams, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("redis_streams writer factory: failed to convert config: %w", err)
		}
		if len(typedConfig.InitAddress) > 0 {
			typedConfig.Endpoint = strings.Join(typedConfig.InitAddress, ",")
		}
		return NewWriter(typedConfig, l)
	})

	reader.RegisterReaderFactory(protocol.RedisStreams, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("redis_streams reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
