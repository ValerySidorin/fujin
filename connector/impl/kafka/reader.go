package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Reader struct {
	conf ReaderConfig
	cl   *kgo.Client

	handler func(r *kgo.Record, h func(message []byte, args ...any))

	l *slog.Logger
}

func NewReader(conf ReaderConfig, l *slog.Logger) (*Reader, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.ConsumeTopics(conf.Topic),
		kgo.ConsumerGroup(conf.Group),
	}

	if conf.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if conf.DisableAutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	if conf.FetchIsolationLevel == IsolationLevelReadCommited {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	reader := &Reader{
		conf: conf,
		cl:   client,
		l:    l.With("reader_type", "kafka"),
	}

	if !conf.DisableAutoCommit {
		reader.handler = func(r *kgo.Record, h func(message []byte, args ...any)) {
			h(r.Value)
		}
	} else {
		reader.handler = func(r *kgo.Record, h func(message []byte, args ...any)) {
			h(r.Value, r.Partition, r.LeaderEpoch, r.Offset)
		}
	}

	return reader, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any)) error {
	if err := r.cl.Ping(ctx); err != nil {
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

func (r *Reader) Fetch(ctx context.Context, n uint32,
	fetchResponseHandler func(n uint32),
	msgHandler func(message []byte, args ...any),
) error {
	fetches := r.cl.PollRecords(ctx, int(n))
	fetchResponseHandler(uint32(fetches.NumRecords()))
	if ctx.Err() != nil {
		return nil
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		return fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		r.handler(iter.Next(), msgHandler)
	}
	return nil
}

// func (r *Reader) Consume(ctx context.Context, trigger <-chan struct{}, n uint32,
// 	h func(message []byte, args ...any) error) error {
// 	if err := r.cl.Ping(ctx); err != nil {
// 		return fmt.Errorf("kafka: ping: %w", err)
// 	}

// 	var wg sync.WaitGroup

// 	busy := make(chan struct{}, 1)
// 	errCh := make(chan error)

// 	defer func() {
// 		close(busy)
// 		close(errCh)
// 		wg.Wait()
// 	}()

// 	var handler func(r *kgo.Record) error
// 	if r.IsAutoCommit() {
// 		handler = func(r *kgo.Record) error {
// 			return h(r.Value)
// 		}
// 	} else {
// 		handler = func(r *kgo.Record) error {
// 			return h(r.Value, r.Partition, r.LeaderEpoch, r.Offset)
// 		}
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		case err := <-errCh:
// 			return err
// 		case <-trigger:
// 			select {
// 			case busy <- struct{}{}:
// 				wg.Add(1)
// 				go func() {
// 					defer wg.Done()
// 					fetches := r.cl.PollRecords(ctx, int(n))
// 					if ctx.Err() != nil {
// 						return
// 					}
// 					if errs := fetches.Errors(); len(errs) > 0 {
// 						errCh <- fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
// 						return
// 					}

// 					iter := fetches.RecordIter()
// 					for !iter.Done() {
// 						record := iter.Next()
// 						if err := handler(record); err != nil {
// 							errCh <- fmt.Errorf("kafka: handle message: %w", err)
// 							return
// 						}
// 					}
// 					<-busy
// 				}()
// 			default:
// 			}
// 		}
// 	}
// }

func (r *Reader) Ack(ctx context.Context, meta []byte) error {
	offsets := map[string]map[int32]kgo.EpochOffset{
		r.conf.Topic: {
			int32(binary.BigEndian.Uint16(meta[:2])): {
				Epoch:  int32(binary.BigEndian.Uint16(meta[2:4])),
				Offset: int64(binary.BigEndian.Uint32(meta[4:8])),
			},
		},
	}

	var rerr error

	r.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			rerr = err
			return
		}

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					rerr = err
					return
				}
			}
		}
	})

	return rerr
}

func (r *Reader) Nack(ctx context.Context, meta []byte) error {
	return nil
}

func (r *Reader) EncodeMeta(buf []byte, args ...any) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[0].(int32)))
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[1].(int32)))
	return binary.BigEndian.AppendUint32(buf, uint32(args[2].(int64)))
}

func (r *Reader) MessageMetaLen() byte {
	if r.IsAutoCommit() {
		return 0
	}
	return 8
}

func (r *Reader) IsAutoCommit() bool {
	return !r.conf.DisableAutoCommit
}

func (r *Reader) Close() {
	r.cl.Close()
}
