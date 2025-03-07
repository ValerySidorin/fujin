package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

var ErrNotSupported = errors.New("not supported")

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
		l:    l.With("writer_type", "nats"),
	}, nil
}

func (w *Writer) Write(_ context.Context, msg []byte, callback func(err error)) {
	err := w.nc.Publish(w.conf.Subject, msg)
	callback(err)
}

func (w *Writer) Flush(_ context.Context) error {
	w.nc.Flush()
	return nil
}

func (w *Writer) BeginTx(_ context.Context) error {
	return ErrNotSupported
}

func (w *Writer) CommitTx(_ context.Context) error {
	return ErrNotSupported
}

func (w *Writer) RollbackTx(_ context.Context) error {
	return ErrNotSupported
}

func (w *Writer) Endpoint() string {
	return w.conf.URL
}

func (w *Writer) Close() error {
	w.nc.Flush()
	w.nc.Close()
	return nil
}
