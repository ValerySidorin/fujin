//go:build resp_streams

package streams

import (
	"fmt"
	"log/slog"

	// Добавим, если понадобится для перезаписи Endpoint

	redis "github.com/ValerySidorin/fujin/public/connectors/impl/resp"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/util"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("resp_streams", func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig *WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, typedConfig); err != nil {
			return nil, fmt.Errorf("resp_streams writer factory: failed to convert config: %w", err)
		}

		return NewWriter(typedConfig, l)
	},
		redis.ParseConfigEndpoint,
	)

	reader.RegisterReaderFactory("resp_streams", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_streams reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
