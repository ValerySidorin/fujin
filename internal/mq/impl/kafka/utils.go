package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

func kgoOptsFromWriterConf(conf WriterConfig, producerID string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.DefaultProduceTopic(conf.Topic),
		kgo.ProducerLinger(conf.Linger),
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if conf.Linger != 0 {
		opts = append(opts, kgo.ProducerLinger(conf.Linger))
	}

	if producerID != "" {
		opts = append(opts, kgo.TransactionalID(producerID))
	}

	return opts
}
