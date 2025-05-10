//go:build amqp10

package amqp10

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/util"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("amqp10", func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig *WriterConfig
		if err := util.ConvertConfig(rawBrokerConfig, typedConfig); err != nil {
			return nil, fmt.Errorf("amqp10 writer factory: failed to convert config: %w", err)
		}
		return NewWriter(typedConfig, l)
	},
		func(conf map[string]any) string {
			connConf, ok := conf["conn"].(map[string]any)
			if !ok {
				return ""
			}
			addr, ok := connConf["addr"].(string)
			if !ok {
				return ""
			}

			return addr
		},
	)

	reader.RegisterReaderFactory("amqp10", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("amqp10 reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
