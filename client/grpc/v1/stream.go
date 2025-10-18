package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/client/correlator"
	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
)

// stream implements the Stream interface
type stream struct {
	client   pb.FujinServiceClient
	streamID string
	logger   *slog.Logger

	// gRPC stream
	grpcStream pb.FujinService_StreamClient

	// State management
	connected atomic.Bool
	closed    atomic.Bool

	// Correlation ID management
	correlationID atomic.Uint32

	// Correlators for different response types
	produceCorrelator     *correlator.Correlator[error]
	subscribeCorrelator   *correlator.Correlator[uint32]
	unsubscribeCorrelator *correlator.Correlator[error]

	// Subscriptions
	subscriptions map[uint32]*subscription
	subsMu        sync.RWMutex

	// Response handling
	responseCh   chan *pb.FujinResponse
	responseDone chan struct{}

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Wait group for goroutines
	wg sync.WaitGroup
}

// newStream creates a new stream
func newStream(client pb.FujinServiceClient, streamID string, logger *slog.Logger) (*stream, error) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &stream{
		client:                client,
		streamID:              streamID,
		logger:                logger.With("stream_id", streamID),
		subscriptions:         make(map[uint32]*subscription),
		responseCh:            make(chan *pb.FujinResponse, 1000),
		responseDone:          make(chan struct{}),
		ctx:                   ctx,
		cancel:                cancel,
		produceCorrelator:     correlator.New[error](),
		subscribeCorrelator:   correlator.New[uint32](),
		unsubscribeCorrelator: correlator.New[error](),
	}

	// Start the stream
	if err := s.start(); err != nil {
		cancel()
		return nil, err
	}

	return s, nil
}

// start initializes the gRPC stream and starts response handling
func (s *stream) start() error {
	// Create gRPC stream
	grpcStream, err := s.client.Stream(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to create gRPC stream: %w", err)
	}
	s.grpcStream = grpcStream

	// Start response reader
	s.wg.Add(1)
	go s.readResponses()

	// Send CONNECT request
	correlationID := s.correlationID.Add(1)
	connectReq := &pb.FujinRequest{
		Request: &pb.FujinRequest_Connect{
			Connect: &pb.ConnectRequest{
				CorrelationId: correlationID,
				StreamId:      s.streamID,
			},
		},
	}

	if err := grpcStream.Send(connectReq); err != nil {
		return fmt.Errorf("failed to send connect request: %w", err)
	}

	// Wait for connect response
	select {
	case resp := <-s.responseCh:
		if connectResp, ok := resp.Response.(*pb.FujinResponse_Connect); ok {
			if connectResp.Connect.CorrelationId == correlationID {
				if connectResp.Connect.Error != "" {
					return fmt.Errorf("connect error: %s", connectResp.Connect.Error)
				}
				s.connected.Store(true)
				s.logger.Info("stream connected")
				return nil
			}
		}
		return fmt.Errorf("unexpected connect response")
	case <-time.After(10 * time.Second):
		return fmt.Errorf("connect timeout")
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// readResponses handles incoming responses from the server
func (s *stream) readResponses() {
	defer s.wg.Done()
	defer close(s.responseDone)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			resp, err := s.grpcStream.Recv()
			if err != nil {
				if s.ctx.Err() == nil {
					s.logger.Error("failed to receive response", "error", err)
				}
				return
			}

			// Route response to appropriate correlator
			s.routeResponse(resp)
		}
	}
}

// routeResponse routes incoming responses to appropriate correlators
func (s *stream) routeResponse(resp *pb.FujinResponse) {
	switch r := resp.Response.(type) {
	case *pb.FujinResponse_Connect:
		// Connect responses are handled synchronously in start()
		select {
		case s.responseCh <- resp:
		case <-s.ctx.Done():
		}
	case *pb.FujinResponse_Produce:
		if r.Produce.Error != "" {
			s.produceCorrelator.Send(r.Produce.CorrelationId, fmt.Errorf("produce error: %s", r.Produce.Error))
		} else {
			s.produceCorrelator.Send(r.Produce.CorrelationId, nil)
		}
	case *pb.FujinResponse_Subscribe:
		if r.Subscribe.Error != "" {
			s.subscribeCorrelator.Send(r.Subscribe.CorrelationId, 0)
		} else {
			s.subscribeCorrelator.Send(r.Subscribe.CorrelationId, r.Subscribe.SubscriptionId)
		}
	case *pb.FujinResponse_Unsubscribe:
		if r.Unsubscribe.Error != "" {
			s.unsubscribeCorrelator.Send(r.Unsubscribe.CorrelationId, fmt.Errorf("unsubscribe error: %s", r.Unsubscribe.Error))
		} else {
			s.unsubscribeCorrelator.Send(r.Unsubscribe.CorrelationId, nil)
		}
	case *pb.FujinResponse_Message:
		// Route messages to subscription handlers
		// Note: correlation_id in Message refers to subscription_id
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[r.Message.CorrelationId]; exists {
			sub.handler(r)
		}
		s.subsMu.RUnlock()
	case *pb.FujinResponse_Ack:
		// ACK responses - could be handled by a separate correlator if needed
		s.logger.Debug("received ack response", "correlation_id", r.Ack.CorrelationId)
	case *pb.FujinResponse_Nack:
		// NACK responses - could be handled by a separate correlator if needed
		s.logger.Debug("received nack response", "correlation_id", r.Nack.CorrelationId)
	}
}

// Produce sends a message to the specified topic
func (s *stream) Produce(topic string, p []byte) error {
	return s.produce(topic, p)
}

// produceWithHeaders sends a message with optional headers
func (s *stream) produce(topic string, p []byte) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.produceCorrelator.Next(ch)

	// Create produce request
	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Produce{
			Produce: &pb.ProduceRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				Message:       p,
			},
		},
	}

	// Send request
	if err := s.grpcStream.Send(req); err != nil {
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send produce request: %w", err)
	}

	// Wait for response
	select {
	case err := <-ch:
		s.produceCorrelator.Delete(correlationID)
		return err
	case <-time.After(10 * time.Second):
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("produce timeout")
	case <-s.ctx.Done():
		s.produceCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// Subscribe subscribes to a topic
func (s *stream) Subscribe(topic string, autoCommit bool, handler func(msg *pb.FujinResponse_Message)) (uint32, error) {
	return s.subscribe(topic, autoCommit, handler)
}

func (s *stream) subscribe(topic string, autoCommit bool, handler func(msg *pb.FujinResponse_Message)) (uint32, error) {
	if !s.connected.Load() {
		return 0, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return 0, fmt.Errorf("stream is closed")
	}

	ch := make(chan uint32, 1)
	correlationID := s.subscribeCorrelator.Next(ch)

	// Create subscribe request
	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Subscribe{
			Subscribe: &pb.SubscribeRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				AutoCommit:    autoCommit,
			},
		},
	}

	// Send request
	if err := s.grpcStream.Send(req); err != nil {
		s.subscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	// Wait for subscribe response
	select {
	case subscriptionID := <-ch:
		s.subscribeCorrelator.Delete(correlationID)

		if subscriptionID == 0 {
			return 0, fmt.Errorf("subscribe failed")
		}

		// Create subscription
		sub := &subscription{
			id:      subscriptionID,
			topic:   topic,
			handler: handler,
			stream:  s,
		}

		s.subsMu.Lock()
		s.subscriptions[subscriptionID] = sub
		s.subsMu.Unlock()

		// Start message handler
		s.wg.Add(1)
		go s.handleMessages(sub)

		s.logger.Info("subscribed to topic", "topic", topic, "subscription_id", subscriptionID)
		return subscriptionID, nil
	case <-time.After(10 * time.Second):
		s.subscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("subscribe timeout")
	case <-s.ctx.Done():
		s.subscribeCorrelator.Delete(correlationID)
		return 0, s.ctx.Err()
	}
}

// handleMessages handles incoming messages for a subscription
func (s *stream) handleMessages(sub *subscription) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case resp := <-s.responseCh:
			if msgResp, ok := resp.Response.(*pb.FujinResponse_Message); ok {
				if msgResp.Message.CorrelationId == sub.id {
					// Call the handler
					func() {
						sub.handler(msgResp)
					}()
				}
			}
		}
	}
}

// Close closes the stream
func (s *stream) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)
	s.cancel()

	// Close all subscriptions
	s.subsMu.Lock()
	for _, sub := range s.subscriptions {
		sub.closed.Store(true)
	}
	s.subscriptions = make(map[uint32]*subscription)
	s.subsMu.Unlock()

	// Wait for goroutines to finish
	s.wg.Wait()

	s.logger.Info("stream closed")
	return nil
}

// Unsubscribe unsubscribes from a topic
func (s *stream) Unsubscribe(subscriptionID uint32) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.unsubscribeCorrelator.Next(ch)

	// Create unsubscribe request
	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Unsubscribe{
			Unsubscribe: &pb.UnsubscribeRequest{
				CorrelationId:  correlationID,
				SubscriptionId: subscriptionID,
			},
		},
	}

	// Send request
	if err := s.grpcStream.Send(req); err != nil {
		s.unsubscribeCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send unsubscribe request: %w", err)
	}

	// Wait for response
	select {
	case err := <-ch:
		s.unsubscribeCorrelator.Delete(correlationID)

		if err != nil {
			return err
		}

		// Remove subscription from our map
		s.subsMu.Lock()
		delete(s.subscriptions, subscriptionID)
		s.subsMu.Unlock()

		s.logger.Info("unsubscribed from topic", "subscription_id", subscriptionID)
		return nil
	case <-time.After(10 * time.Second):
		s.unsubscribeCorrelator.Delete(correlationID)
		return fmt.Errorf("unsubscribe timeout")
	case <-s.ctx.Done():
		s.unsubscribeCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}
