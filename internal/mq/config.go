package mq

import (
	reader "github.com/ValerySidorin/fujin/internal/mq/reader/config"
	writer "github.com/ValerySidorin/fujin/internal/mq/writer/config"
)

type Config struct {
	Readers map[string]reader.Config `yaml:"readers"`
	Writers map[string]writer.Config `yaml:"writers"`
}
