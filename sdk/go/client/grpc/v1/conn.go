package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	client "github.com/fujin-io/fujin/sdk/go/client"
	pb "github.com/fujin-io/fujin/sdk/go/client/grpc/v1/proto"
	"google.golang.org/grpc"
)

type conn struct {
	grpcConn *grpc.ClientConn
	client   pb.FujinServiceClient
	mu       sync.RWMutex
	closed   bool
}

func Dial(addr string, _ *slog.Logger, opts ...grpc.DialOption) (client.Conn, error) {
	grpcConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to server: %w", err)
	}
	return &conn{grpcConn: grpcConn, client: pb.NewFujinServiceClient(grpcConn)}, nil
}

func (c *conn) Bind(ctx context.Context, connector string, opts ...client.BindOption) (client.Stream, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("connection is closed")
	}
	conf := &client.BindConfig{}
	for _, option := range opts {
		if option != nil {
			option(conf)
		}
	}
	return newStream(ctx, c.client, connector, conf.Meta, conf.ConfigOverrides, conf.Stream)
}

func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.grpcConn.Close()
}
