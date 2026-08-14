package faker

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

type Writer struct {
	l *slog.Logger
}

func NewWriter(l *slog.Logger) (connector.WriteCloser, error) {
	l.Info("faker writer initialized")
	return &Writer{
		l: l.With("writer_type", "faker"),
	}, nil
}

func (w *Writer) Produce(_ context.Context, msg []byte, callback func(err error)) {
	w.l.Info("fake produce", "msg_len", len(msg))
	callback(nil)
}

func (w *Writer) HProduce(_ context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	w.l.Info("fake produce with headers", "msg_len", len(msg), "headers_count", len(headers))
	callback(nil)
}

func (w *Writer) Flush(_ context.Context) error {
	return nil
}

func (w *Writer) BeginTx(_ context.Context) error {
	w.l.Info("fake begin tx")
	return nil
}

func (w *Writer) CommitTx(_ context.Context) error {
	w.l.Info("fake commit tx")
	return nil
}

func (w *Writer) RollbackTx(_ context.Context) error {
	w.l.Info("fake rollback tx")
	return nil
}

func (w *Writer) Close() error {
	return nil
}
