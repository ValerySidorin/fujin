package core

import (
	"github.com/ValerySidorin/fujin/internal/connector/cerr"
)

type ReaderConfig struct {
	URL     string `json:"url"`
	Subject string `yaml:"subject"`
}

type WriterConfig struct {
	URL     string `json:"url"`
	Subject string `yaml:"subject"`
}

func (c *ReaderConfig) Validate() error {
	if c.URL == "" {
		return cerr.ValidationErr("url not defined")
	}
	if c.Subject == "" {
		return cerr.ValidationErr("subject not defined")
	}

	return nil
}

func (c *WriterConfig) Validate() error {
	if c.URL == "" {
		return cerr.ValidationErr("url not defined")
	}
	if c.Subject == "" {
		return cerr.ValidationErr("subject not defined")
	}

	return nil
}
