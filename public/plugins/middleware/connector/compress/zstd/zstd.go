package zstd

import (
	"fmt"
	"log/slog"

	"github.com/klauspost/compress/zstd"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

func init() {
	if err := cmw.Register("compress_zstd", newCompressZstdMiddleware); err != nil {
		panic(fmt.Sprintf("register compress_zstd connector middleware: %v", err))
	}
}

type compressZstdMiddleware struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	l       *slog.Logger
}

func newCompressZstdMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
	level := zstd.SpeedDefault

	if m, ok := config.(map[string]any); ok {
		if v, ok := m["level"].(string); ok {
			switch v {
			case "fastest":
				level = zstd.SpeedFastest
			case "default":
				level = zstd.SpeedDefault
			case "better":
				level = zstd.SpeedBetterCompression
			case "best":
				level = zstd.SpeedBestCompression
			default:
				return nil, fmt.Errorf("compress_zstd middleware: unknown level %q (expected fastest, default, better, best)", v)
			}
		}
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("compress_zstd middleware: create encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("compress_zstd middleware: create decoder: %w", err)
	}

	l.Info("zstd compress middleware initialized", "level", level)

	return &compressZstdMiddleware{
		encoder: encoder,
		decoder: decoder,
		l:       l,
	}, nil
}

func (m *compressZstdMiddleware) compress(msg []byte) []byte {
	return m.encoder.EncodeAll(msg, make([]byte, 0, len(msg)))
}

func (m *compressZstdMiddleware) decompress(msg []byte) ([]byte, error) {
	out, err := m.decoder.DecodeAll(msg, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress failed: %w", err)
	}
	return out, nil
}

func (m *compressZstdMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	return &zstdWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *compressZstdMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	return &zstdReaderWrapper{
		r:  r,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}
