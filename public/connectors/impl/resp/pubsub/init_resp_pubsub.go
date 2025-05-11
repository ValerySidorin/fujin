//go:build resp_pubsub

package pubsub

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/impl/resp"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/util"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("resp_pubsub", func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_pubsub writer factory: failed to convert config: %w", err)
		}

		return NewWriter(typedConfig, l)
	},
		resp.ParseConfigEndpoint,
	)

	reader.RegisterReaderFactory("resp_pubsub", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_pubsub reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
