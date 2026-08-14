package streams

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type Reader struct {
	conf            ConnectorConfig
	client          rueidis.Client
	handler         func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, stream string, args ...any))
	headeredHandler func(resp map[string][]rueidis.XRangeEntry, h func(message []byte, stream string, hs [][]byte, args ...any))
	marshal         func(v any) ([]byte, error)
	autoCommit      bool
	streams         map[string]string
	fetching        atomic.Bool
	l               *slog.Logger
}

func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	tlsConf, err := conf.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("redis_rueidis_streams: get tls config: %w", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		TLSConfig:    tlsConf,
		InitAddress:  conf.InitAddress,
		Username:     conf.Username,
		Password:     conf.Password,
		DisableCache: conf.DisableCache,
	})
	if err != nil {
		return nil, fmt.Errorf("redis_rueidis_streams: new client: %w", err)
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
		l:          l.With("reader_type", "redis_rueidis_streams"),
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
					return nil, fmt.Errorf("redis_rueidis_streams: xgroup create: %w", err)
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
							r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
							continue
						}
						h(data, stream)
					}

					if r.streams[stream] != ">" {
						r.streams[stream] = msg.ID
					}
				}
			}
			r.headeredHandler = func(
				resp map[string][]rueidis.XRangeEntry,
				h func(message []byte, stream string, hs [][]byte, args ...any),
			) {
				for stream, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
							continue
						}
						h(data, stream, nil)
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
							r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
							continue
						}
						ts, seq, err := parseStreamID(msg.ID)
						if err != nil {
							r.l.Error("redis_rueidis_streams: invalid message ID", "id", msg.ID, "error", err)
							continue
						}
						h(data, stream, ts, seq)
					}

					if r.streams[stream] != ">" {
						r.streams[stream] = msg.ID
					}
				}
			}
			r.headeredHandler = func(
				resp map[string][]rueidis.XRangeEntry,
				h func(message []byte, stream string, hs [][]byte, args ...any),
			) {
				for stream, msgs := range resp {
					var msg rueidis.XRangeEntry
					for _, msg = range msgs {
						data, err := r.marshal(msg.FieldValues)
						if err != nil {
							r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
							continue
						}
						ts, seq, err := parseStreamID(msg.ID)
						if err != nil {
							r.l.Error("redis_rueidis_streams: invalid message ID", "id", msg.ID, "error", err)
							continue
						}
						h(data, stream, nil, ts, seq)
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
						r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
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
						r.l.Error("redis_rueidis_streams: failed to marshal message", "error", err)
						continue
					}
					ts, seq, err := parseStreamID(msg.ID)
					if err != nil {
						r.l.Error("redis_rueidis_streams: invalid message ID", "id", msg.ID, "error", err)
						continue
					}
					h(data, stream, ts, seq)
				}
				r.streams[stream] = msg.ID
			}
		}
	}

	return r, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(msg []byte, source string, args ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := r.client.Do(ctx, r.cmd(r.conf.Count)).AsXRead()

			if err != nil {
				if rueidis.IsRedisNil(err) {
					continue
				}
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				return fmt.Errorf("redis_rueidis_streams: xread: %w", err)
			}

			r.handler(resp, h)
		}
	}
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := r.client.Do(ctx, r.cmd(r.conf.Count)).AsXRead()

			if err != nil {
				if rueidis.IsRedisNil(err) {
					continue
				}
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				return fmt.Errorf("redis_rueidis_streams: xread: %w", err)
			}

			r.headeredHandler(resp, h)
		}
	}
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	if !r.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer r.fetching.Store(false)
	resp, err := r.client.Do(ctx, r.cmd(int64(n))).AsXRead()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			fetchHandler(0, nil)
			return
		}
		fetchHandler(0, fmt.Errorf("redis_rueidis_streams: xread: %w", err))
		return
	}
	resp, count := limitXReadResponse(resp, n)
	fetchHandler(count, nil)
	r.handler(resp, msgHandler)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	if !r.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer r.fetching.Store(false)
	resp, err := r.client.Do(ctx, r.cmd(int64(n))).AsXRead()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			fetchHandler(0, nil)
			return
		}
		fetchHandler(0, fmt.Errorf("redis_rueidis_streams: xread: %w", err))
		return
	}
	resp, count := limitXReadResponse(resp, n)
	fetchHandler(count, nil)
	r.headeredHandler(resp, msgHandler)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, msgID := range msgIDs {
		if err := r.validateMessageID(msgID); err != nil {
			ackMsgHandler(msgID, fmt.Errorf("redis_rueidis_streams: ack: %w", err))
			continue
		}
		id := strings.Join(
			[]string{
				strconv.FormatUint(binary.BigEndian.Uint64(msgID[:8]), 10),
				strconv.FormatUint(binary.BigEndian.Uint64(msgID[8:16]), 10),
			}, "-")
		stream := string(msgID[16:])
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
	nackHandler(connector.ErrOperationUnsupported)
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	buf = binary.BigEndian.AppendUint64(buf, args[0].(uint64))
	buf = binary.BigEndian.AppendUint64(buf, args[1].(uint64))
	return append(buf, source...)
}

func (r *Reader) MsgIDArgsLen() int { return 16 }

func (r *Reader) validateMessageID(id []byte) error {
	if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), true); err != nil {
		return err
	}
	if _, ok := r.conf.Streams[string(id[16:])]; !ok {
		return fmt.Errorf("%w: stream is outside the reader scope", connector.ErrInvalidMessageID)
	}
	return nil
}

func parseStreamID(id string) (uint64, uint64, error) {
	timestamp, sequence, ok := strings.Cut(id, "-")
	if !ok || timestamp == "" || sequence == "" {
		return 0, 0, fmt.Errorf("invalid Redis stream ID %q", id)
	}
	ts, err := strconv.ParseUint(timestamp, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Redis stream timestamp %q: %w", timestamp, err)
	}
	seq, err := strconv.ParseUint(sequence, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Redis stream sequence %q: %w", sequence, err)
	}
	return ts, seq, nil
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	r.client.Close()
	return nil
}

func (r *Reader) cmd(count int64) rueidis.Completed {
	streams := make([]string, 0, len(r.streams))
	ids := make([]string, 0, len(r.streams))
	for k, v := range r.streams {
		streams = append(streams, k)
		ids = append(ids, v)
	}
	if count <= 0 {
		count = r.conf.Count
	}
	if r.conf.Group.Name == "" {
		return r.client.B().
			Xread().
			Count(count).Block(r.conf.Block.Milliseconds()).
			Streams().Key(streams...).Id(ids...).
			Build()
	}
	if r.autoCommit {
		return r.client.B().
			Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
			Count(count).Block(r.conf.Block.Milliseconds()).Noack().
			Streams().Key(streams...).Id(ids...).
			Build()
	}
	return r.client.B().
		Xreadgroup().Group(r.conf.Group.Name, r.conf.Group.Consumer).
		Count(count).Block(r.conf.Block.Milliseconds()).
		Streams().Key(streams...).Id(ids...).
		Build()
}

func limitXReadResponse(resp map[string][]rueidis.XRangeEntry, maximum uint32) (map[string][]rueidis.XRangeEntry, uint32) {
	remaining := int(maximum)
	count := uint32(0)
	for stream, messages := range resp {
		if remaining == 0 {
			delete(resp, stream)
			continue
		}
		if len(messages) > remaining {
			messages = messages[:remaining]
			resp[stream] = messages
		}
		count += uint32(len(messages))
		remaining -= len(messages)
	}
	return resp, count
}

func marshalFunc(proto Marshaller) func(v any) ([]byte, error) {
	switch proto {
	case JSON:
		return sonic.Marshal
	default:
		return func(v any) ([]byte, error) {
			val := v.(map[string]string)["msg"]
			return unsafe.Slice((*byte)(unsafe.StringData(val)), len(val)), nil
		}
	}
}
