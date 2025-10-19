package v1

import (
	"fmt"
	"log/slog"
	"sync"

	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
	"google.golang.org/grpc"
)

// conn implements the Conn interface
type conn struct {
	addr      string
	grpcConn  *grpc.ClientConn
	client    pb.FujinServiceClient
	logger    *slog.Logger
	mu        sync.RWMutex
	closed    bool
	streams   map[string]*stream
	streamsMu sync.RWMutex
}

// NewConn creates a new gRPC connection
func NewConn(addr string, logger *slog.Logger, opts ...grpc.DialOption) (Conn, error) {
	if logger == nil {
		logger = slog.Default()
	}

	c := &conn{
		addr:    addr,
		logger:  logger.With("component", "grpc-conn"),
		streams: make(map[string]*stream),
	}

	grpcConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	c.grpcConn = grpcConn
	c.client = pb.NewFujinServiceClient(grpcConn)

	c.logger.Info("connected to gRPC server", "address", addr)
	return c, nil
}

// Connect creates a new stream with the given ID
func (c *conn) Connect(id string) (Stream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("connection is closed")
	}
	c.mu.RUnlock()

	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	if existingStream, exists := c.streams[id]; exists {
		return existingStream, nil
	}

	stream, err := newStream(c.client, id, c.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	c.streams[id] = stream
	c.logger.Info("created new stream", "stream_id", id)
	return stream, nil
}

// Close closes the connection and all streams
func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	for id, stream := range c.streams {
		if err := stream.Close(); err != nil {
			c.logger.Error("failed to close stream", "stream_id", id, "error", err)
		}
	}
	c.streams = make(map[string]*stream)

	if c.grpcConn != nil {
		if err := c.grpcConn.Close(); err != nil {
			c.logger.Error("failed to close gRPC connection", "error", err)
			return err
		}
	}

	c.logger.Info("connection closed")
	return nil
}
