package franz

import (
	"crypto/tls"

	"github.com/twmb/franz-go/pkg/kgo"
)

func kgoOptsFromConf(conf ConnectorConfig, autoCommit bool, tlsConfig *tls.Config) []kgo.Opt {
	// Common settings
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.DialTLSConfig(tlsConfig),
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	// Producer settings
	if conf.ProduceTopic != "" {
		opts = append(opts, kgo.DefaultProduceTopic(conf.ProduceTopic))
	}

	// Use TransactionalID from config if set
	if conf.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(conf.TransactionalID))
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

	// Consumer settings
	if len(conf.ConsumeTopics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(conf.ConsumeTopics...))
	}

	if conf.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(conf.Group))
		if !autoCommit {
			opts = append(opts, kgo.DisableAutoCommit())
		}
	}

	if conf.AutoCommitInterval != 0 {
		opts = append(opts, kgo.AutoCommitInterval(conf.AutoCommitInterval))
	}

	if conf.AutoCommitMarks {
		opts = append(opts, kgo.AutoCommitMarks())
	}

	if conf.BlockRebalanceOnPoll {
		opts = append(opts, kgo.BlockRebalanceOnPoll())
	}

	if conf.FetchIsolationLevel == IsolationLevelReadCommited {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	opts = appendBalancersToKgoOpts(opts, conf.Balancers)
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
