package client

import "github.com/fujin-io/fujin/sdk/go/client/config"

type BindConfig struct {
	Meta            map[string]string
	ConfigOverrides map[string]string
	Stream          *config.StreamConfig
}

type BindOption func(*BindConfig)

func WithMeta(m map[string]string) BindOption {
	return func(bc *BindConfig) {
		bc.Meta = m
	}
}

func WithConfigOverrides(co map[string]string) BindOption {
	return func(bc *BindConfig) {
		bc.ConfigOverrides = co
	}
}

func WithStreamConfig(conf *config.StreamConfig) BindOption {
	return func(bc *BindConfig) {
		bc.Stream = conf
	}
}
