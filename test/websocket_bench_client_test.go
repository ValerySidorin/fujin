//go:build grpc

package test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	gorillaws "github.com/gorilla/websocket"
)

type benchmarkWebSocketStream struct {
	connection *gorillaws.Conn

	readMu  sync.Mutex
	reader  io.Reader
	writeMu sync.Mutex
}

func createWebSocketBenchmarkConn() *benchmarkWebSocketStream {
	connection, _, err := gorillaws.DefaultDialer.Dial(
		"ws://"+PERF_WEBSOCKET_ADDR+PERF_WEBSOCKET_PATH,
		nil,
	)
	if err != nil {
		panic(fmt.Errorf("dial websocket: %w", err))
	}
	return &benchmarkWebSocketStream{connection: connection}
}

func (s *benchmarkWebSocketStream) Read(buffer []byte) (int, error) {
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
		messageType, reader, err := s.connection.NextReader()
		if err != nil {
			return 0, err
		}
		if messageType != gorillaws.BinaryMessage {
			return 0, fmt.Errorf("websocket benchmark received message type %d", messageType)
		}
		s.reader = reader
	}
}

func (s *benchmarkWebSocketStream) Write(buffer []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	writer, err := s.connection.NextWriter(gorillaws.BinaryMessage)
	if err != nil {
		return 0, err
	}
	n, writeErr := writer.Write(buffer)
	return n, errors.Join(writeErr, writer.Close())
}

func (s *benchmarkWebSocketStream) Close() error { return s.connection.Close() }

func (s *benchmarkWebSocketStream) SetDeadline(deadline time.Time) error {
	return errors.Join(s.SetReadDeadline(deadline), s.SetWriteDeadline(deadline))
}

func (s *benchmarkWebSocketStream) SetReadDeadline(deadline time.Time) error {
	return s.connection.SetReadDeadline(deadline)
}

func (s *benchmarkWebSocketStream) SetWriteDeadline(deadline time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.connection.SetWriteDeadline(deadline)
}
