package nats

type ReaderConfig struct {
	URL     string `json:"url"`
	Subject string `yaml:"subject"`
}

type WriterConfig struct {
	URL     string `json:"url"`
	Subject string `yaml:"subject"`
}
