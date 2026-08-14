package franz

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/fujin-io/fujin/public/plugins/connector"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *Connector) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.conf.PingTimeout)
	defer cancel()

	if err := c.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka_franz: ping: %w", err)
	}
	if err := ready(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := c.cl.PollRecords(ctx, c.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka_franz: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				c.handler(iter.Next(), h)
			}
		}
	}
}

func (c *Connector) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.conf.PingTimeout)
	defer cancel()

	if err := c.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka_franz: ping: %w", err)
	}
	if err := ready(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := c.cl.PollRecords(ctx, c.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka_franz: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				c.headeredHandler(iter.Next(), h)
			}
		}
	}
}

func (c *Connector) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	if !c.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer c.fetching.Store(false)

	fetches := c.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka_franz: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		c.handler(rec, msgHandler)
	}

	// We need to commit messages manually for some reason, even if auto commit is enabled
	if c.autoCommit && rec != nil {
		if err := c.cl.CommitRecords(ctx, rec); err != nil {
			c.l.Error("kafka_franz: commit record", "err", err)
		}
	}
}

func (c *Connector) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	if !c.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer c.fetching.Store(false)

	fetches := c.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka_franz: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		c.headeredHandler(rec, msgHandler)
	}

	if c.autoCommit && rec != nil {
		if err := c.cl.CommitRecords(ctx, rec); err != nil {
			c.l.Error("kafka_franz: commit record", "err", err)
		}
	}
}

func (c *Connector) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	msgIDMapping := make(map[string]map[int32][][]byte)

	for _, id := range msgIDs {
		if err := c.validateMessageID(id); err != nil {
			ackHandler(fmt.Errorf("kafka_franz: ack: %w", err))
			return
		}
		partition := int32(binary.BigEndian.Uint32(id[:4]))
		epoch := int32(binary.BigEndian.Uint32(id[4:8]))
		offset := int64(binary.BigEndian.Uint64(id[8:16]))
		topic := string(id[16:])

		if msgIDMapping[topic] == nil {
			msgIDMapping[topic] = make(map[int32][][]byte)
		}
		msgIDMapping[topic][partition] = append(msgIDMapping[topic][partition], id)

		toffsets := offsets[topic]
		if toffsets == nil {
			toffsets = make(map[int32]kgo.EpochOffset)
			offsets[topic] = toffsets
		}
		if at, exists := toffsets[partition]; exists && (at.Epoch > epoch || at.Epoch == epoch && at.Offset > offset) {
			continue
		}
		toffsets[partition] = kgo.EpochOffset{Epoch: epoch, Offset: offset + 1}
	}

	c.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		ackHandler(err)
		if err != nil {
			return
		}
		results := make(map[string]map[int32]error, len(msgIDMapping))
		if resp != nil {
			for _, topic := range resp.Topics {
				partitions := make(map[int32]error, len(topic.Partitions))
				for _, partition := range topic.Partitions {
					partitions[partition.Partition] = kerr.ErrorForCode(partition.ErrorCode)
				}
				results[topic.Topic] = partitions
			}
		}
		for topic, partitions := range msgIDMapping {
			for partition, ids := range partitions {
				result, ok := results[topic][partition]
				if !ok {
					result = fmt.Errorf("kafka_franz: offset commit response omitted topic %q partition %d", topic, partition)
				}
				for _, id := range ids {
					ackMsgHandler(id, result)
				}
			}
		}
	})
}

func (c *Connector) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(connector.ErrOperationUnsupported)
}

func (c *Connector) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[0].(int32)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[1].(int32)))
	buf = binary.BigEndian.AppendUint64(buf, uint64(args[2].(int64)))
	return append(buf, source...)
}

func (c *Connector) MsgIDArgsLen() int { return 16 }

func (c *Connector) validateMessageID(id []byte) error {
	if err := connector.ValidateMessageIDPayload(id, c.MsgIDArgsLen(), true); err != nil {
		return err
	}
	partition := int32(binary.BigEndian.Uint32(id[:4]))
	offset := int64(binary.BigEndian.Uint64(id[8:16]))
	topic := string(id[16:])
	if partition < 0 || offset < 0 {
		return connector.ErrInvalidMessageID
	}
	for _, configured := range c.conf.ConsumeTopics {
		if topic == configured {
			return nil
		}
	}
	return fmt.Errorf("%w: topic %q is outside the reader scope", connector.ErrInvalidMessageID, topic)
}

func (c *Connector) AutoCommit() bool {
	return c.autoCommit
}
