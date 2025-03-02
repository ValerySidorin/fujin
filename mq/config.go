package mq

import (
	reader "github.com/ValerySidorin/fujin/mq/reader/config"
	writer "github.com/ValerySidorin/fujin/mq/writer/config"
)

type Config struct {
	Readers map[string]reader.Config `yaml:"readers"`
	Writers map[string]writer.Config `yaml:"writers"`
}
