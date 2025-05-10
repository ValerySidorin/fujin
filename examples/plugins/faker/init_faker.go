package faker

import (
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("faker",
		func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
			return NewWriter(l)
		},
		writer.DefaultConfigEndpointParser,
	)

	reader.RegisterReaderFactory("faker", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		return NewReader(autoCommit, l)
	})
}
