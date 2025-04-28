package streams

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/redis/rueidis"
)

type Reader struct {
	conf         ReaderConfig
	client       rueidis.Client
	handler      func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, args ...any))
	marshal      func(v any) ([]byte, error)
	id           string
	autoCommit   bool
	strSlicePool sync.Pool
	l            *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  conf.InitAddress,
		Username:     conf.Username,
		Password:     conf.Password,
		DisableCache: conf.DisableCache,
	})
	if err != nil {
		return nil, fmt.Errorf("redis: new client: %w", err)
	}

	r := &Reader{
		conf:       conf,
		client:     client,
		marshal:    marshalFunc(conf.ParseMsgProtocol),
		id:         conf.StartId,
		autoCommit: autoCommit,
		l:          l.With("reader_type", "redis_streams"),
	}

	if r.conf.Group.Name == "" {
		if autoCommit {
			r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, args ...any)) {
				for _, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis: failed to marshal message", "error", err)
							continue
						}
						h(data)
					}
					r.id = msg.ID
				}
			}
		} else {
			r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, args ...any)) {
				for _, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis: failed to marshal message", "error", err)
							continue
						}
						metaParts := strings.Split(msg.ID, "-")
						ts, _ := strconv.ParseUint(metaParts[0], 10, 32)
						seq, _ := strconv.ParseUint(metaParts[1], 10, 32)
						h(data, uint32(ts), uint32(seq))
					}
					r.id = msg.ID
				}
			}
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := client.Do(ctx, client.B().XgroupCreate().Key(conf.Stream).Group(conf.Group.Name).Id(conf.Group.CreateId).Mkstream().Build()).Error(); err != nil {
			if !rueidis.IsRedisBusyGroup(err) {
				return nil, fmt.Errorf("redis: xgroup create: %w", err)
			}
		}

		if autoCommit {
			r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, args ...any)) {
				for _, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis: failed to marshal message", "error", err)
							continue
						}
						h(data)
					}
				}
			}
		} else {
			r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, args ...any)) {
				for _, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis: failed to marshal message", "error", err)
							continue
						}
						metaParts := strings.Split(msg.ID, "-")
						ts, _ := strconv.ParseInt(metaParts[0], 10, 64)
						seq, _ := strconv.ParseInt(metaParts[1], 10, 64)
						h(data, ts, seq)
					}
				}
			}
		}
	}

	return r, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:

			resp, err := r.client.Do(ctx, r.cmd()).AsXRead()

			if err != nil {
				if rueidis.IsRedisNil(err) {
					continue
				}
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				return fmt.Errorf("redis: xread: %w", err)
			}

			r.handler(resp, h)
		}
	}
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchResponseHandler func(n uint32),
	msgHandler func(message []byte, args ...any),
) error {

	resp, err := r.client.Do(ctx, r.cmd()).AsXRead()

	if err != nil {
		if rueidis.IsRedisNil(err) {
			fetchResponseHandler(0)
			return nil
		}
		return fmt.Errorf("redis: xread: %w", err)
	}

	var numMsgs uint32

	for _, msgs := range resp {
		for range msgs {
			numMsgs++
		}
	}
	fetchResponseHandler(numMsgs)
	r.handler(resp, msgHandler)

	return nil
}

func (r *Reader) Ack(ctx context.Context, meta []byte) error {
	if err := r.client.Do(
		ctx,
		r.client.B().Xack().
			Key(r.conf.Stream).Group(r.conf.Group.Name).
			Id(
				strings.Join(
					[]string{
						strconv.FormatUint(uint64(binary.BigEndian.Uint32(meta)), 10),
						strconv.FormatUint(uint64(binary.BigEndian.Uint32(meta[4:])), 10),
					}, "-")).
			Build(),
	).Error(); err != nil {
		return fmt.Errorf("redis: xack: %w", err)
	}

	return nil
}

func (r *Reader) Nack(ctx context.Context, meta []byte) error {
	return nil
}

func (r *Reader) EncodeMeta(buf []byte, args ...any) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[0].(uint32)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[1].(uint32)))
	return buf
}

func (r *Reader) MessageMetaLen() byte {
	if r.IsAutoCommit() {
		return 0
	}
	return 8
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {
	r.client.Close()
}

// This will return when I change ack/nack to receive multiple messages
// func (r *Reader) keys(m map[string]string) []string {
// 	keys := r.strSlicePool.Get().([]string)[:0]
// 	for k := range m {
// 		keys = append(keys, k)
// 	}
// 	return keys
// }

// func (r *Reader) values(m map[string]string) []string {
// 	values := r.strSlicePool.Get().([]string)[:0]
// 	for _, v := range m {
// 		values = append(values, v)
// 	}
// 	return values
// }

func (r *Reader) cmd() rueidis.Completed {
	if r.conf.Group.Name == "" {
		return r.client.B().
			Xread().
			Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).
			Streams().Key(r.conf.Stream).Id(r.id).
			Build()
	}
	if r.autoCommit {
		return r.client.B().
			Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
			Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).Noack().
			Streams().Key(r.conf.Stream).Id(r.id).
			Build()
	}
	return r.client.B().
		Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
		Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).
		Streams().Key(r.conf.Stream).Id(r.id).
		Build()
}

func marshalFunc(proto ParseMsgProtocol) func(v any) ([]byte, error) {
	switch proto {
	case ParseMsgProtocolJSON:
		return json.Marshal
	default:
		return func(v any) ([]byte, error) {
			m, ok := v.(map[string]string)
			if !ok {
				return nil, fmt.Errorf("invalid type: %T", v)
			}

			val, ok := m["msg"]
			if !ok {
				return nil, fmt.Errorf("msg not found in map")
			}

			data := unsafe.Slice((*byte)(unsafe.StringData(val)), len(val))

			return data, nil
		}
	}
}
