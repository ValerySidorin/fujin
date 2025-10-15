package v1

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// conn implements the Conn interface
type conn struct {
	address   string
	tlsConfig *tls.Config
	grpcConn  *grpc.ClientConn
	client    pb.FujinServiceClient
	logger    *slog.Logger
	mu        sync.RWMutex
	closed    bool
	streams   map[string]*stream
	streamsMu sync.RWMutex
}

// ConnConfig holds connection configuration
type ConnConfig struct {
	Address              string
	TLSEnabled           bool
	TLSCertPath          string
	TLSKeyPath           string
	InsecureSkipVerify   bool
	KeepAlive            time.Duration
	KeepAliveTimeout     time.Duration
	MaxRetries           int
	RetryDelay           time.Duration
	ConnectionTimeout    time.Duration
	MaxConcurrentStreams int
}

// DefaultConnConfig returns default connection configuration
func DefaultConnConfig() *ConnConfig {
	return &ConnConfig{
		Address:              "localhost:9091",
		TLSEnabled:           false,
		InsecureSkipVerify:   true,
		KeepAlive:            30 * time.Second,
		KeepAliveTimeout:     5 * time.Second,
		MaxRetries:           3,
		RetryDelay:           1 * time.Second,
		ConnectionTimeout:    10 * time.Second,
		MaxConcurrentStreams: 100,
	}
}

// NewConn creates a new gRPC connection
func NewConn(config *ConnConfig, logger *slog.Logger) (Conn, error) {
	if config == nil {
		config = DefaultConnConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}

	c := &conn{
		address: config.Address,
		logger:  logger.With("component", "grpc-conn"),
		streams: make(map[string]*stream),
	}

	// Setup TLS
	if config.TLSEnabled {
		if config.TLSCertPath != "" && config.TLSKeyPath != "" {
			// Load TLS certificates
			cert, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificates: %w", err)
			}
			c.tlsConfig = &tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: config.InsecureSkipVerify,
			}
		} else {
			c.tlsConfig = &tls.Config{
				InsecureSkipVerify: config.InsecureSkipVerify,
			}
		}
	}

	// Setup gRPC connection options
	var opts []grpc.DialOption

	if c.tlsConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Connect to gRPC server
	grpcConn, err := grpc.NewClient(config.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	c.grpcConn = grpcConn
	c.client = pb.NewFujinServiceClient(grpcConn)

	c.logger.Info("connected to gRPC server", "address", config.Address)
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

	// Check if stream already exists
	if existingStream, exists := c.streams[id]; exists {
		return existingStream, nil
	}

	// Create new stream
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

	// Close all streams
	for id, stream := range c.streams {
		if err := stream.Close(); err != nil {
			c.logger.Error("failed to close stream", "stream_id", id, "error", err)
		}
	}
	c.streams = make(map[string]*stream)

	// Close gRPC connection
	if c.grpcConn != nil {
		if err := c.grpcConn.Close(); err != nil {
			c.logger.Error("failed to close gRPC connection", "error", err)
			return err
		}
	}

	c.logger.Info("connection closed")
	return nil
}
