package kafka

import (
	"math"

	"github.com/twmb/franz-go/pkg/kgo"
)

func kgoOptsFromWriterConf(conf WriterConfig, writerID string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.DefaultProduceTopic(conf.Topic),
		kgo.ProducerLinger(conf.Linger),
		kgo.MaxBufferedRecords(math.MaxInt), // franz-go can deadlock on high load with small max buffered records for some reason
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if conf.Linger != 0 {
		opts = append(opts, kgo.ProducerLinger(conf.Linger))
	}

	if writerID != "" {
		opts = append(opts, kgo.TransactionalID(writerID))
	}

	return opts
}
