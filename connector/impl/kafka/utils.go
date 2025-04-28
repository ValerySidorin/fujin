package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

func kgoOptsFromWriterConf(conf WriterConfig, writerID string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.DefaultProduceTopic(conf.Topic),
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if writerID != "" {
		opts = append(opts, kgo.TransactionalID(writerID))
	}

	if conf.DisableIdempotentWrite {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	if conf.Linger != 0 {
		opts = append(opts, kgo.ProducerLinger(conf.Linger))
	}

	if conf.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(conf.MaxBufferedRecords))
	}

	return opts
}

func appendBalancersToKgoOpts(opts []kgo.Opt, balancers []Balancer) []kgo.Opt {
	balancerMap := make(map[Balancer]struct{})
	for _, b := range balancers {
		balancerMap[b] = struct{}{}
	}

	if len(balancerMap) > 0 {
		bs := make([]kgo.GroupBalancer, 0, len(balancerMap))
		for b := range balancerMap {
			switch b {
			case BalancerUnknown:
				// skip unknown balancer
			case BalancerSticky:
				bs = append(bs, kgo.StickyBalancer())
			case BalancerCooperativeSticky:
				bs = append(bs, kgo.CooperativeStickyBalancer())
			case BalancerRange:
				bs = append(bs, kgo.RangeBalancer())
			case BalancerRoundRobin:
				bs = append(bs, kgo.RoundRobinBalancer())
			}
		}

		if len(bs) != 0 {
			opts = append(opts, kgo.Balancers(bs...))
		}
	}

	return opts
}
