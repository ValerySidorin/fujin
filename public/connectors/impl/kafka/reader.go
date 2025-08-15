//go:build kafka

package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Reader struct {
	conf ReaderConfig
	cl   *kgo.Client

	handler         func(r *kgo.Record, h func(message []byte, topic string, args ...any))
	headeredHandler func(r *kgo.Record, h func(message []byte, topic string, hs [][]byte, args ...any))
	fetching        atomic.Bool
	autoCommit      bool

	l *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.ConsumeTopics(conf.Topic),
		kgo.ConsumerGroup(conf.Group),
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if !autoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
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

	appendBalancersToKgoOpts(opts, conf.Balancers)

	if conf.PingTimeout <= 0 {
		conf.PingTimeout = 5 * time.Second
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	reader := &Reader{
		conf:       conf,
		cl:         client,
		autoCommit: autoCommit,
		l:          l.With("reader_type", "kafka"),
	}

	if autoCommit {
		reader.handler = func(r *kgo.Record, h func(message []byte, topic string, args ...any)) {
			h(r.Value, r.Topic)
		}
		reader.headeredHandler = func(r *kgo.Record, h func(message []byte, topic string, hs [][]byte, args ...any)) {
			var hs [][]byte
			for _, kh := range r.Headers {
				hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(kh.Key)), len(kh.Key)), kh.Value)
			}
			h(r.Value, r.Topic, hs)
		}
	} else {
		reader.handler = func(r *kgo.Record, h func(message []byte, topic string, args ...any)) {
			h(r.Value, r.Topic, r.Partition, r.LeaderEpoch, r.Offset)
		}
		reader.headeredHandler = func(r *kgo.Record, h func(message []byte, topic string, hs [][]byte, args ...any)) {
			var hs [][]byte
			for _, kh := range r.Headers {
				hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(kh.Key)), len(kh.Key)), kh.Value)
			}
			h(r.Value, r.Topic, hs, r.Partition, r.LeaderEpoch, r.Offset)
		}
	}

	return reader, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, r.conf.PingTimeout)
	defer cancel()

	if err := r.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := r.cl.PollRecords(ctx, r.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				r.handler(iter.Next(), h)
			}
		}
	}
}

func (r *Reader) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, r.conf.PingTimeout)
	defer cancel()

	if err := r.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := r.cl.PollRecords(ctx, r.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				r.headeredHandler(iter.Next(), h)
			}
		}
	}
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	if r.fetching.Load() {
		fetchHandler(0, nil)
		return
	}

	r.fetching.Store(true)
	defer r.fetching.Store(false)

	fetches := r.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		r.handler(rec, msgHandler)
	}

	// We need to commit messages manually for some reason, even if auto commit is enabled
	if r.autoCommit {
		if err := r.cl.CommitRecords(ctx, rec); err != nil {
			r.l.Error("commit record", "err", err)
		}
	}
}

func (r *Reader) FetchH(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	if r.fetching.Load() {
		fetchHandler(0, nil)
		return
	}

	r.fetching.Store(true)
	defer r.fetching.Store(false)

	fetches := r.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		r.headeredHandler(rec, msgHandler)
	}

	if r.autoCommit {
		if err := r.cl.CommitRecords(ctx, rec); err != nil {
			r.l.Error("commit record", "err", err)
		}
	}
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	msgIDMapping := make(map[string]map[int32][][]byte)

	for _, id := range msgIDs {
		partition := int32(binary.BigEndian.Uint16(id[:2]))
		epoch := int32(binary.BigEndian.Uint16(id[2:4]))
		offset := int64(binary.BigEndian.Uint32(id[4:8]))
		topic := string(id[8:])

		if msgIDMapping[topic] == nil {
			msgIDMapping[topic] = make(map[int32][][]byte)
		}

		msgIDMapping[topic][partition] = append(msgIDMapping[topic][partition], id)

		toffsets := offsets[topic]
		if toffsets == nil {
			toffsets = make(map[int32]kgo.EpochOffset)
			offsets[topic] = toffsets
		}

		if at, exists := toffsets[partition]; exists {
			if at.Epoch > epoch || at.Epoch == epoch && at.Offset > offset {
				continue
			}
		}
		toffsets[partition] = kgo.EpochOffset{
			Epoch:  epoch,
			Offset: offset + 1,
		}
	}

	r.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		ackHandler(err)
		if err != nil {
			return
		}

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				err := kerr.ErrorForCode(partition.ErrorCode)
				for _, msgID := range msgIDMapping[topic.Topic][partition.Partition] {
					ackMsgHandler(msgID, err)
				}
			}
		}
	})
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, msgID := range msgIDs {
		nackMsgHandler(msgID, nil)
	}
}

func (e *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[0].(int32)))
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[1].(int32)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[2].(int64)))
	return append(buf, topic...)
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 8
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {
	r.cl.Close()
}
