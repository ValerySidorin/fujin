package faker

import (
	"context"
	"log/slog"
)

type Writer struct {
	l *slog.Logger
}

func NewWriter(l *slog.Logger) (*Writer, error) {
	l.Info("faker writer initialized")
	return &Writer{
		l: l.With("writer_type", "faker"),
	}, nil
}

func (w *Writer) Write(_ context.Context, msg []byte, callback func(err error)) {
	w.l.Info("fake write")
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

func (w *Writer) Endpoint() string {
	return ""
}

func (w *Writer) Close() error {
	return nil
}
