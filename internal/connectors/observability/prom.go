package observability

import (
	"context"
	"time"

	obs "github.com/ValerySidorin/fujin/internal/observability"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

func WrapMetricsWriterIfEnabled(w writer.Writer, connectorName string) writer.Writer {
	if !obs.MetricsEnabled() {
		return w
	}
	return &metricsWriterDecorator{w: w, connectorName: connectorName}
}

type metricsWriterDecorator struct {
	w             writer.Writer
	connectorName string
}

func (d *metricsWriterDecorator) Write(ctx context.Context, msg []byte, callback func(err error)) {
	start := time.Now()
	d.w.Write(ctx, msg, func(err error) {
		obs.IncOp("PRODUCE", d.connectorName)
		if err != nil {
			obs.IncError("produce", d.connectorName)
		}
		obs.ObserveWriteLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *metricsWriterDecorator) WriteH(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	start := time.Now()
	d.w.WriteH(ctx, msg, headers, func(err error) {
		obs.IncOp("HPRODUCE", d.connectorName)
		if err != nil {
			obs.IncError("produce_h", d.connectorName)
		}
		obs.ObserveWriteLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *metricsWriterDecorator) Flush(ctx context.Context) error {
	err := d.w.Flush(ctx)
	if err != nil {
		obs.IncError("flush", d.connectorName)
	}
	return err
}

func (d *metricsWriterDecorator) BeginTx(ctx context.Context) error {
	obs.IncOp("BEGIN_TX", d.connectorName)
	err := d.w.BeginTx(ctx)
	if err != nil {
		obs.IncError("begin_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterDecorator) CommitTx(ctx context.Context) error {
	obs.IncOp("COMMIT_TX", d.connectorName)
	err := d.w.CommitTx(ctx)
	if err != nil {
		obs.IncError("commit_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterDecorator) RollbackTx(ctx context.Context) error {
	obs.IncOp("ROLLBACK_TX", d.connectorName)
	err := d.w.RollbackTx(ctx)
	if err != nil {
		obs.IncError("rollback_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterDecorator) Endpoint() string {
	return d.w.Endpoint()
}

func (d *metricsWriterDecorator) Close() error {
	return d.w.Close()
}

func WrapMetricsReaderIfEnabled(r reader.Reader, connectorName string) reader.Reader {
	if !obs.MetricsEnabled() {
		return r
	}
	return &metricsReaderDecorator{r: r, connectorName: connectorName}
}

type metricsReaderDecorator struct {
	r             reader.Reader
	connectorName string
}

func (d *metricsReaderDecorator) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	obs.IncOp("SUBSCRIBE", d.connectorName)
	return d.r.Subscribe(
		ctx,
		func(message []byte, topic string, args ...any) {
			obs.IncOp("MSG", d.connectorName)
			h(message, topic, args...)
		},
	)
}

func (d *metricsReaderDecorator) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	obs.IncOp("HSUBSCRIBE", d.connectorName)
	return d.r.SubscribeH(
		ctx,
		func(message []byte, topic string, hs [][]byte, args ...any) {
			obs.IncOp("HMSG", d.connectorName)
			h(message, topic, hs, args...)
		},
	)
}

func (d *metricsReaderDecorator) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	frh := func(n uint32, err error) {
		obs.IncOp("FETCH", d.connectorName)
		if err != nil {
			obs.IncError("fetch", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.Fetch(ctx, n, frh, msgHandler)
}

func (d *metricsReaderDecorator) FetchH(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	frh := func(n uint32, err error) {
		obs.IncOp("HFETCH", d.connectorName)
		if err != nil {
			obs.IncError("fetch_h", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.FetchH(ctx, n, frh, msgHandler)
}

func (d *metricsReaderDecorator) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	d.r.Ack(
		ctx, msgIDs,
		func(err error) {
			obs.IncOp("ACK", d.connectorName)
			if err != nil {
				obs.IncError("ack", d.connectorName)
			}
			ackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				obs.IncError("ack_msg", d.connectorName)
			}
			ackMsgHandler(b, err)
		},
	)
}

func (d *metricsReaderDecorator) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	d.r.Ack(
		ctx, msgIDs,
		func(err error) {
			obs.IncOp("NACK", d.connectorName)
			if err != nil {
				obs.IncError("nack", d.connectorName)
			}
			nackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				obs.IncError("nack_msg", d.connectorName)
			}
			nackMsgHandler(b, err)
		},
	)
}

func (d *metricsReaderDecorator) MsgIDStaticArgsLen() int {
	return d.r.MsgIDStaticArgsLen()
}

func (d *metricsReaderDecorator) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return d.r.EncodeMsgID(buf, topic, args...)
}
func (d *metricsReaderDecorator) IsAutoCommit() bool {
	return d.r.IsAutoCommit()
}
func (d *metricsReaderDecorator) Close() {
	d.r.Close()
}
