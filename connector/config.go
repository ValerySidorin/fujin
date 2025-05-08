package connector

import (
	reader "github.com/ValerySidorin/fujin/connector/reader/config"
	writer "github.com/ValerySidorin/fujin/connector/writer/config"
)

type Config struct {
	Readers map[string]reader.Config `yaml:"readers"`
	Writers map[string]writer.Config `yaml:"writers"`
}

func (c *Config) Validate() error {
	// TODO: Validate broker config maps
	return nil
}
