package websocket

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	gorillaws "github.com/gorilla/websocket"
)

type stream struct {
	conn *gorillaws.Conn

	readMu sync.Mutex
	reader io.Reader

	writeMu      sync.Mutex
	writer       io.WriteCloser
	batchedWrite bool
}

func newStream(conn *gorillaws.Conn) *stream { return &stream{conn: conn} }

func (s *stream) Read(buffer []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for {
		if s.reader != nil {
			n, err := s.reader.Read(buffer)
			if errors.Is(err, io.EOF) {
				s.reader = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		messageType, reader, err := s.conn.NextReader()
		if err != nil {
			return 0, err
		}
		if messageType != gorillaws.BinaryMessage {
			return 0, fmt.Errorf("websocket transport requires binary messages, got type %d", messageType)
		}
		s.reader = reader
	}
}

func (s *stream) Write(buffer []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writer == nil {
		writer, err := s.conn.NextWriter(gorillaws.BinaryMessage)
		if err != nil {
			return 0, err
		}
		s.writer = writer
	}
	n, writeErr := s.writer.Write(buffer)
	if writeErr != nil || !s.batchedWrite {
		closeErr := s.closeWriterLocked()
		return n, errors.Join(writeErr, closeErr)
	}
	return n, nil
}

func (s *stream) closeWriterLocked() error {
	if s.writer == nil {
		return nil
	}
	writer := s.writer
	s.writer = nil
	return writer.Close()
}

func (s *stream) Close() error {
	s.writeMu.Lock()
	writerErr := s.closeWriterLocked()
	s.writeMu.Unlock()
	return errors.Join(writerErr, s.conn.Close())
}

func (s *stream) SetDeadline(deadline time.Time) error {
	return errors.Join(s.SetReadDeadline(deadline), s.SetWriteDeadline(deadline))
}

func (s *stream) SetReadDeadline(deadline time.Time) error {
	return s.conn.SetReadDeadline(deadline)
}

func (s *stream) SetWriteDeadline(deadline time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if deadline.IsZero() {
		writerErr := s.closeWriterLocked()
		s.batchedWrite = false
		return errors.Join(writerErr, s.conn.SetWriteDeadline(deadline))
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	s.batchedWrite = true
	return nil
}
