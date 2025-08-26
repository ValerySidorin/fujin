//go:build nats_core

package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/nats-io/nats.go"
)

type Writer struct {
	conf WriterConfig
	nc   *nats.Conn
	l    *slog.Logger
}

func NewWriter(conf WriterConfig, l *slog.Logger) (*Writer, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	return &Writer{
		conf: conf,
		nc:   nc,
		l:    l.With("writer_type", "nats_core"),
	}, nil
}

func (w *Writer) Produce(_ context.Context, msg []byte, callback func(err error)) {
	err := w.nc.Publish(w.conf.Subject, msg)
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
			if i+1 < len(headers) {
				key := string(headers[i])
				value := string(headers[i+1])
				natsMsg.Header.Set(key, value)
			}
		}
	}

	err := w.nc.PublishMsg(natsMsg)
	callback(err)
}

func (w *Writer) Flush(_ context.Context) error {
	w.nc.Flush()
	return nil
}

func (w *Writer) BeginTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) CommitTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) RollbackTx(_ context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) Endpoint() string {
	return w.conf.URL
}

func (w *Writer) Close() error {
	w.nc.Flush()
	w.nc.Close()
	return nil
}
