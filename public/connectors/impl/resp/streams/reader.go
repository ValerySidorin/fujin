//go:build resp_streams

package streams

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
)

type Reader struct {
	conf         ReaderConfig
	client       rueidis.Client
	handler      func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, stream string, args ...any))
	marshal      func(v any) ([]byte, error)
	autoCommit   bool
	streams      map[string]string
	strSlicePool sync.Pool
	l            *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	tlsConf, err := conf.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("resp: get tls config: %w", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		TLSConfig:    tlsConf,
		InitAddress:  conf.InitAddress,
		Username:     conf.Username,
		Password:     conf.Password,
		DisableCache: conf.DisableCache,
	})
	if err != nil {
		return nil, fmt.Errorf("resp: new client: %w", err)
	}

	streams := make(map[string]string, len(conf.Streams))
	for stream, streamConf := range conf.Streams {
		streams[stream] = streamConf.StartID
	}

	r := &Reader{
		conf:       conf,
		client:     client,
		marshal:    marshalFunc(conf.Marshaller),
		autoCommit: autoCommit,
		streams:    streams,
		strSlicePool: sync.Pool{
			New: func() any {
				return make([]string, 0, len(conf.Streams))
			},
		},
		l: l.With("reader_type", "resp_streams"),
	}

	if r.conf.Group.Name != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		for stream, streamConf := range r.conf.Streams {
			if err := client.Do(
				ctx,
				client.B().
					XgroupCreate().
					Key(stream).Group(conf.Group.Name).Id(streamConf.GroupCreateID).Mkstream().
					Build(),
			).Error(); err != nil {
				if !rueidis.IsRedisBusyGroup(err) {
					return nil, fmt.Errorf("resp: xgroup create: %w", err)
				}
			}
		}

		if autoCommit {
			r.handler = func(
				resp map[string][]rueidis.XRangeEntry,
				h func(message []byte, stream string, args ...any),
			) {
				for stream, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("resp: failed to marshal message", "error", err)
							continue
						}
						h(data, stream)
					}

					if r.streams[stream] != ">" {
						r.streams[stream] = msg.ID
					}
				}
			}
		} else {
			r.handler = func(
				resp map[string][]rueidis.XRangeEntry,
				h func(message []byte, stream string, args ...any),
			) {
				for stream, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("resp: failed to marshal message", "error", err)
							continue
						}
						metaParts := strings.Split(msg.ID, "-")
						ts, _ := strconv.ParseInt(metaParts[0], 10, 64)
						seq, _ := strconv.ParseInt(metaParts[1], 10, 64)
						h(data, stream, uint32(ts), uint32(seq))
					}

					if r.streams[stream] != ">" {
						r.streams[stream] = msg.ID
					}
				}
			}
		}

		return r, nil
	}

	if autoCommit {
		r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, stream string, args ...any)) {
			for stream, msgs := range resp {
				var msg rueidis.XRangeEntry
				for _, msg = range msgs {
					data, err := r.marshal(msg.FieldValues)
					if err != nil {
						r.l.Error("resp: failed to marshal message", "error", err)
						continue
					}
					h(data, stream)
				}
				r.streams[stream] = msg.ID
			}
		}
	} else {
		r.handler = func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, stream string, args ...any)) {
			for stream, msgs := range resp {
				var msg rueidis.XRangeEntry
				for _, msg = range msgs {
					data, err := r.marshal(msg.FieldValues)
					if err != nil {
						r.l.Error("resp: failed to marshal message", "error", err)
						continue
					}
					metaParts := strings.Split(msg.ID, "-")
					ts, _ := strconv.ParseUint(metaParts[0], 10, 32)
					seq, _ := strconv.ParseUint(metaParts[1], 10, 32)
					h(data, stream, uint32(ts), uint32(seq))
				}
				r.streams[stream] = msg.ID
			}
		}
	}

	return r, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(msg []byte, topic string, args ...any)) error {
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
				return fmt.Errorf("resp: xread: %w", err)
			}

			r.handler(resp, h)
		}
	}
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {

	resp, err := r.client.Do(ctx, r.cmd()).AsXRead()

	if err != nil {
		if rueidis.IsRedisNil(err) {
			fetchHandler(0, nil)
			return
		}
		fetchHandler(0, fmt.Errorf("resp: xread: %w", err))
		return
	}

	var numMsgs uint32

	for _, msgs := range resp {
		for range msgs {
			numMsgs++
		}
	}
	fetchHandler(numMsgs, nil)
	r.handler(resp, msgHandler)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, msgID := range msgIDs {
		id := strings.Join(
			[]string{
				strconv.FormatUint(uint64(binary.BigEndian.Uint32(msgID[:4])), 10),
				strconv.FormatUint(uint64(binary.BigEndian.Uint32(msgID[4:8])), 10),
			}, "-")
		stream := string(msgID[8:])
		err := r.client.Do(
			ctx,
			r.client.B().Xack().Key(stream).Group(r.conf.Group.Name).Id(id).Build(),
		).Error()
		ackMsgHandler(msgID, err)
	}
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

func (r *Reader) EncodeMsgID(buf []byte, stream string, args ...any) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[0].(uint32)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[1].(uint32)))
	return append(buf, stream...)
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 8
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {
	r.client.Close()
}

func (r *Reader) keys(m map[string]string) []string {
	keys := r.strSlicePool.Get().([]string)[:0]
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (r *Reader) values(m map[string]string) []string {
	values := r.strSlicePool.Get().([]string)[:0]
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func (r *Reader) cmd() rueidis.Completed {
	streams := r.keys(r.streams)
	ids := r.values(r.streams)

	if r.conf.Group.Name == "" {
		return r.client.B().
			Xread().
			Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).
			Streams().Key(streams...).Id(ids...).
			Build()
	}
	if r.autoCommit {
		return r.client.B().
			Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
			Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).Noack().
			Streams().Key(streams...).Id(ids...).
			Build()
	}
	return r.client.B().
		Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
		Count(r.conf.Count).Block(r.conf.Block.Milliseconds()).
		Streams().Key(streams...).Id(ids...).
		Build()
}

func marshalFunc(proto Marshaller) func(v any) ([]byte, error) {
	switch proto {
	case JSON:
		return sonic.Marshal
	default:
		return func(v any) ([]byte, error) {
			val := v.(map[string]string)["msg"]
			data := unsafe.Slice((*byte)(unsafe.StringData(val)), len(val))
			return data, nil
		}
	}
}
