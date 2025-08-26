package observability

import (
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

var (
	OtelWriterWrapper = func(w writer.Writer, connectorName string) writer.Writer {
		return w
	}

	OtelReaderWrapper = func(r reader.Reader, connectorName string) reader.Reader {
		return r
	}

	MetricsWriterWrapper = func(w writer.Writer, connectorName string) writer.Writer {
		return w
	}

	MetricsReaderWrapper = func(r reader.Reader, connectorName string) reader.Reader {
		return r
	}
)
