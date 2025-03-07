package nats

import (
	"errors"
	"fmt"
)

var ErrValidateNatsConf = errors.New("validate nats config")

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
		return validationErr("url not defined")
	}
	if c.Subject == "" {
		return validationErr("subject not defined")
	}

	return nil
}

func (c *WriterConfig) Validate() error {
	if c.URL == "" {
		return validationErr("url not defined")
	}
	if c.Subject == "" {
		return validationErr("subject not defined")
	}

	return nil
}

func validationErr(text string) error {
	return fmt.Errorf(text+": %w", ErrValidateNatsConf)
}
