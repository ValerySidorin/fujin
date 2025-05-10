//go:build nsq

package nsq

import (
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/util"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("nsq",
		func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
			var typedConfig *WriterConfig
			if err := util.ConvertConfig(rawBrokerConfig, typedConfig); err != nil {
				return nil, fmt.Errorf("nsq writer factory: failed to convert config: %w", err)
			}
			return NewWriter(typedConfig, l)
		},
		writer.DefaultConfigEndpointParser,
	)

	reader.RegisterReaderFactory("nsq", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("nsq reader factory: failed to convert config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
