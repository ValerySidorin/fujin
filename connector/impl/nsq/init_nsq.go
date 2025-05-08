//go:build nsq

package nsq

import (
	"fmt"
	"log/slog"

	// "strings"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/writer"
	"gopkg.in/yaml.v3"
)

// convertConfig
func convertConfig(rawBrokerConfig any, output any) error {
	yamlBytes, err := yaml.Marshal(rawBrokerConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal raw broker config: %w", err)
	}
	err = yaml.Unmarshal(yamlBytes, output)
	if err != nil {
		return fmt.Errorf("failed to unmarshal to target config struct: %w", err)
	}
	return nil
}

func init() {
	writer.RegisterWriterFactory(protocol.NSQ, func(rawBrokerConfig any, writerID string, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig // Локальный тип NSQ WriterConfig
		if err := convertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("nsq writer factory: failed to convert config: %w", err)
		}
		// if err := typedConfig.Validate(); err != nil { ... }
		return NewWriter(typedConfig, l) // writerID не используется
	})

	reader.RegisterReaderFactory(protocol.NSQ, func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig // Локальный тип NSQ ReaderConfig
		if err := convertConfig(rawBrokerConfig, &typedConfig); err != nil {
			return nil, fmt.Errorf("nsq reader factory: failed to convert config: %w", err)
		}
		// if err := typedConfig.Validate(); err != nil { ... }
		return NewReader(typedConfig, autoCommit, l)
	})
}
