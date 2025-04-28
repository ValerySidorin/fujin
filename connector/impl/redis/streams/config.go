package streams

import "time"

type ParseMsgProtocol string

const (
	ParseMsgProtocolJSON ParseMsgProtocol = "json"
)

type ReaderConfig struct {
	InitAddress  []string      `yaml:"init_address"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	Stream       string        `yaml:"stream"`
	StartId      string        `yaml:"start_id"`
	Block        time.Duration `yaml:"block"`
	Count        int64         `yaml:"count"`
	DisableCache bool          `yaml:"disable_cache"`
	Group        struct {
		Name     string `yaml:"name"`
		Consumer string `yaml:"consumer"`
		CreateId string `yaml:"create_id"`
	} `yaml:"group"`

	ParseMsgProtocol ParseMsgProtocol `yaml:"parse_msg_protocol"`
}

type WriterConfig struct {
	InitAddress  []string `yaml:"init_address"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	DisableCache bool     `yaml:"disable_cache"`

	BatchSize int           `yaml:"batch_size"`
	Linger    time.Duration `yaml:"linger"`

	Stream           string           `yaml:"stream"`
	ParseMsgProtocol ParseMsgProtocol `yaml:"parse_msg_protocol"`

	Endpoint string `yaml:"-"` // Used to compare writers that can be shared in tx
}
