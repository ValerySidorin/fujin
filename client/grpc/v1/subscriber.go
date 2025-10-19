package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ValerySidorin/fujin/client/models"
)

// Subscriber provides high-performance message consumption
type Subscriber struct {
	stream Stream
	logger *slog.Logger

	maxConcurrent int

	messageCh     chan models.Msg
	workerPool    chan struct{}
	handleMessage func(ctx context.Context, msg models.Msg) error

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

// NewSubscriber creates a new high-performance consumer
func NewSubscriber(stream Stream, config *ConsumerConfig, logger *slog.Logger) *Subscriber {
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
		maxConcurrent: config.MaxConcurrent,
		messageCh:     make(chan models.Msg, config.BufferSize),
		workerPool:    make(chan struct{}, config.MaxConcurrent),
		handleMessage: func(ctx context.Context, msg models.Msg) error { return nil },
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
func (c *Subscriber) Subscribe(topic string, autoCommit bool, handler func(ctx context.Context, msg models.Msg) error) error {
	// Create a wrapper handler that processes messages through the worker pool
	wrapperHandler := func(msg models.Msg) {
		select {
		case c.messageCh <- msg:
			// Message queued for processing
		case <-c.ctx.Done():
			// Consumer is closing
		default:
			// Buffer is full, drop message
			c.logger.Warn("message buffer full, dropping message", "topic", topic)
		}
	}

	// Subscribe using the stream
	subscriptionID, err := c.stream.Subscribe(topic, autoCommit, wrapperHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	c.logger.Info("subscribed to topic", "topic", topic, "subscription_id", subscriptionID)
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
func (c *Subscriber) processMessage(workerID int, msg models.Msg) {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
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

	close(c.messageCh)

	c.wg.Wait()

	c.logger.Info("subscriber closed")
	return nil
}
