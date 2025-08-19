package observability

import (
	"context"

	obs "github.com/ValerySidorin/fujin/internal/observability"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func WrapOtelWriterIfEnabled(w writer.Writer, connectorName string) writer.Writer {
	if !obs.TracingEnabled() {
		return w
	}
	return &otelWriterDecorator{w: w, connectorName: connectorName}
}

type otelWriterDecorator struct {
	w             writer.Writer
	connectorName string
}

func (d *otelWriterDecorator) Write(ctx context.Context, msg []byte, callback func(err error)) {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.write")
	span.SetAttributes(attribute.String("connector", d.connectorName))
	d.w.Write(ctx, msg, func(err error) {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		callback(err)
	})
}

func (d *otelWriterDecorator) WriteH(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.write_h")
	span.SetAttributes(attribute.String("connector", d.connectorName), attribute.Int("header_pairs", len(headers)/2))
	d.w.WriteH(ctx, msg, headers, func(err error) {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		callback(err)
	})
}

func (d *otelWriterDecorator) Flush(ctx context.Context) error {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.flush")
	span.SetAttributes(attribute.String("connector", d.connectorName))
	defer span.End()
	return d.w.Flush(ctx)
}

func (d *otelWriterDecorator) BeginTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.tx_begin")
	span.SetAttributes(attribute.String("connector", d.connectorName))
	defer span.End()
	return d.w.BeginTx(ctx)
}

func (d *otelWriterDecorator) CommitTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.tx_commit")
	span.SetAttributes(attribute.String("connector", d.connectorName))
	defer span.End()
	return d.w.CommitTx(ctx)
}

func (d *otelWriterDecorator) RollbackTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = obs.Tracer().Start(ctx, "writer.tx_rollback")
	span.SetAttributes(attribute.String("connector", d.connectorName))
	defer span.End()
	return d.w.RollbackTx(ctx)
}

func (d *otelWriterDecorator) Endpoint() string {
	return d.w.Endpoint()
}

func (d *otelWriterDecorator) Close() error {
	return d.w.Close()
}

func WrapOtelReaderIfEnabled(r reader.Reader, connectorName string) reader.Reader {
	if !obs.TracingEnabled() {
		return r
	}
	return &otelReaderDecorator{r: r, connectorName: connectorName}
}

type otelReaderDecorator struct {
	r             reader.Reader
	connectorName string
}

func (d *otelReaderDecorator) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	return d.r.Subscribe(
		ctx,
		func(message []byte, topic string, args ...any) {
			_, span := obs.Tracer().Start(ctx, "reader.subscribe.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			h(message, topic, args...)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return d.r.SubscribeH(
		ctx,
		func(message []byte, topic string, hs [][]byte, args ...any) {
			_, span := obs.Tracer().Start(ctx, "reader.subscribe_h.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			h(message, topic, hs, args...)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	d.r.Fetch(ctx, n,
		func(n uint32, err error) {
			_, span := obs.Tracer().Start(ctx, "reader.fetch.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			fetchResponseHandler(n, err)
			span.End()
		},
		func(message []byte, topic string, args ...any) {
			_, span := obs.Tracer().Start(ctx, "reader.fetch.handle_msg")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			msgHandler(message, topic, args...)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) FetchH(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	d.r.FetchH(ctx, n,
		func(n uint32, err error) {
			_, span := obs.Tracer().Start(ctx, "reader.fetch.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			fetchResponseHandler(n, err)
			span.End()
		},
		func(message []byte, topic string, hs [][]byte, args ...any) {
			_, span := obs.Tracer().Start(ctx, "reader.fetch.handle_msg")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			msgHandler(message, topic, hs, args...)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	d.r.Ack(ctx, msgIDs,
		func(err error) {
			_, span := obs.Tracer().Start(ctx, "reader.ack.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			ackHandler(err)
			span.End()
		},
		func(b []byte, err error) {
			_, span := obs.Tracer().Start(ctx, "reader.ack.handle_msg")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			ackMsgHandler(b, err)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	d.r.Nack(ctx, msgIDs,
		func(err error) {
			_, span := obs.Tracer().Start(ctx, "reader.nack.handle")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			nackHandler(err)
			span.End()
		},
		func(b []byte, err error) {
			_, span := obs.Tracer().Start(ctx, "reader.nack.handle_msg")
			span.SetAttributes(attribute.String("connector", d.connectorName))
			nackMsgHandler(b, err)
			span.End()
		},
	)
}

func (d *otelReaderDecorator) MsgIDStaticArgsLen() int {
	return d.r.MsgIDStaticArgsLen()
}

func (d *otelReaderDecorator) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return d.r.EncodeMsgID(buf, topic, args...)
}

func (d *otelReaderDecorator) IsAutoCommit() bool {
	return d.r.IsAutoCommit()
}

func (d *otelReaderDecorator) Close() {
	d.r.Close()
}
