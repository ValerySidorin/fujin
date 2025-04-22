package client

import (
	"context"
	"fmt"

	"github.com/ValerySidorin/fujin/connector/reader"
)

type Consumer struct {
	*clientReader
}

func (c *Conn) ConnectConsumer(conf ReaderConfig) (*Consumer, error) {
	r, err := c.connectReader(conf, reader.Subscriber)
	if err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}

	cm := &Consumer{
		clientReader: r,
	}

	cm.start(cm.parse)

	return cm, nil
}

func (c *Consumer) Fetch(ctx context.Context, n int, handler func(msg Msg)) error {
	return nil
}

// TODO: parse correlation id and create correlation entry with a chan of messages
// Send <number of messages in batch> messages to this chan, and then close it
func (c *Consumer) parse(buf []byte) error {
	return nil
}
