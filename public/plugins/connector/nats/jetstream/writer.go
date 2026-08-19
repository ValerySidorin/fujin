package jetstream

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Writer implements connector.WriteCloser for NATS JetStream.
type Writer struct {
	conf ConnectorConfig
	nc   *nats.Conn
	js   jetstream.JetStream
	l    *slog.Logger
}

// NewWriter creates a new NATS JetStream writer.
func NewWriter(conf ConnectorConfig, l *slog.Logger) (connector.WriteCloser, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats_jetstream: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats_jetstream: create jetstream context: %w", err)
	}

	return &Writer{
		conf: conf,
		nc:   nc,
		js:   js,
		l:    l.With("writer_type", "nats_jetstream"),
	}, nil
}

func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	_, err := w.js.Publish(ctx, w.conf.Subject, msg)
	callback(err)
}

func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	natsMsg := &nats.Msg{
		Subject: w.conf.Subject,
		Data:    msg,
	}

	if len(headers) > 0 {
		natsMsg.Header = make(nats.Header)
		for i := 0; i < len(headers); i += 2 {
			natsMsg.Header.Add(string(headers[i]), string(headers[i+1]))
		}
	}

	_, err := w.js.PublishMsg(ctx, natsMsg)
	callback(err)
}

func (w *Writer) Flush(_ context.Context) error {
	return w.nc.Flush()
}

func (w *Writer) BeginTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) CommitTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) RollbackTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) Close() error {
	if err := w.nc.Flush(); err != nil {
		w.l.Error("flush on close", "err", err)
	}
	w.nc.Close()
	return nil
}
