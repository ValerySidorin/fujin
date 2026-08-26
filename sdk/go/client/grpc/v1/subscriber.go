package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
)

// Subscriber provides high-performance message consumption
type Subscriber struct {
	stream client.Stream
	logger *slog.Logger

	workerTimeout time.Duration

	messageCh     chan client.Message
	handleMessage func(ctx context.Context, msg client.Message) error

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	BufferSize    int
	MaxConcurrent int
	WorkerTimeout time.Duration
}

// DefaultConsumerConfig returns default consumer configuration
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BufferSize:    1000,
		MaxConcurrent: 10,
		WorkerTimeout: 30 * time.Second,
	}
}

func NewSubscriber(stream client.Stream, config *ConsumerConfig, logger *slog.Logger) *Subscriber {
	if config == nil {
		config = DefaultConsumerConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	consumer := &Subscriber{
		stream:        stream,
		logger:        logger.With("component", "subscriber"),
		workerTimeout: config.WorkerTimeout,
		messageCh:     make(chan client.Message, config.BufferSize),
		handleMessage: func(ctx context.Context, msg client.Message) error { return nil },
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start worker pool
	for i := 0; i < config.MaxConcurrent; i++ {
		consumer.wg.Add(1)
		go consumer.worker(i)
	}

	return consumer
}

// Subscribe subscribes to a topic with high-performance processing
func (c *Subscriber) Subscribe(route string, autoSettle bool, handler func(ctx context.Context, msg client.Message) error) error {
	c.handleMessage = handler
	_, err := c.stream.Subscribe(c.ctx, route, autoSettle, func(msg client.Message) {
		select {
		case c.messageCh <- msg:
		case <-c.ctx.Done():
		default:
			c.logger.Warn("message buffer full, dropping message", "route", route)
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	return nil
}

// worker processes messages from the message channel
func (c *Subscriber) worker(workerID int) {
	defer c.wg.Done()

	for {
		select {
		case msg := <-c.messageCh:
			c.processMessage(workerID, msg)
		case <-c.ctx.Done():
			return
		}
	}
}

// processMessage processes a single message
func (c *Subscriber) processMessage(workerID int, msg client.Message) {
	ctx, cancel := context.WithTimeout(c.ctx, c.workerTimeout)
	defer cancel()

	if err := c.handleMessage(ctx, msg); err != nil {
		c.logger.Error("message processing error",
			"worker_id", workerID,
			"subscription_id", msg.SubscriptionID,
			"error", err)
	}
}

// Close closes the consumer
func (c *Subscriber) Close() error {
	c.cancel()
	c.wg.Wait()
	c.logger.Info("subscriber closed")
	return nil
}
