package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/client/correlator"
	"github.com/ValerySidorin/fujin/client/models"
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
	ackCorrelator         *correlator.Correlator[models.AckResult]
	nackCorrelator        *correlator.Correlator[models.NackResult]

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
		ackCorrelator:         correlator.New[models.AckResult](),
		nackCorrelator:        correlator.New[models.NackResult](),
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
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[r.Message.SubscriptionId]; exists {
			sub.handler(models.Msg{
				SubscriptionID: r.Message.SubscriptionId,
				MessageID:      r.Message.MessageId,
				Payload:        r.Message.Payload,
			})
		}
		s.subsMu.RUnlock()
	case *pb.FujinResponse_Ack:
		result := models.AckResult{}
		if r.Ack.Error != "" {
			result.Error = fmt.Errorf("ack error: %s", r.Ack.Error)
		}
		// Convert per-message results
		if len(r.Ack.Results) > 0 {
			result.MessageResults = make([]models.AckMessageResult, len(r.Ack.Results))
			for i, res := range r.Ack.Results {
				result.MessageResults[i] = models.AckMessageResult{
					MessageID: res.MessageId,
				}
				if res.Error != "" {
					result.MessageResults[i].Error = fmt.Errorf("%s", res.Error)
				}
			}
		}
		s.ackCorrelator.Send(r.Ack.CorrelationId, result)
	case *pb.FujinResponse_Nack:
		result := models.NackResult{}
		if r.Nack.Error != "" {
			result.Error = fmt.Errorf("nack error: %s", r.Nack.Error)
		}
		// Convert per-message results
		if len(r.Nack.Results) > 0 {
			result.MessageResults = make([]models.NackMessageResult, len(r.Nack.Results))
			for i, res := range r.Nack.Results {
				result.MessageResults[i] = models.NackMessageResult{
					MessageID: res.MessageId,
				}
				if res.Error != "" {
					result.MessageResults[i].Error = fmt.Errorf("%s", res.Error)
				}
			}
		}
		s.nackCorrelator.Send(r.Nack.CorrelationId, result)
	}
}

// Produce sends a message to the specified topic
func (s *stream) Produce(topic string, p []byte) error {
	return s.produce(topic, p)
}

// produce sends a message to a topic
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
func (s *stream) Subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	return s.subscribe(topic, autoCommit, handler)
}

func (s *stream) subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
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
				if msgResp.Message.SubscriptionId == sub.id {
					// Call the handler
					func() {
						sub.handler(models.Msg{
							SubscriptionID: msgResp.Message.SubscriptionId,
							MessageID:      msgResp.Message.MessageId,
							Payload:        msgResp.Message.Payload,
						})
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

// Ack acknowledges one or more messages for a subscription
func (s *stream) Ack(subscriptionID uint32, messageIDs ...[]byte) (models.AckResult, error) {
	if !s.connected.Load() {
		return models.AckResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.AckResult{}, fmt.Errorf("stream is closed")
	}

	if len(messageIDs) == 0 {
		return models.AckResult{}, fmt.Errorf("at least one message ID is required")
	}

	ch := make(chan models.AckResult, 1)
	correlationID := s.ackCorrelator.Next(ch)

	// Create ack request
	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Ack{
			Ack: &pb.AckRequest{
				CorrelationId:  correlationID,
				MessageIds:     messageIDs,
				SubscriptionId: subscriptionID,
			},
		},
	}

	// Send request
	if err := s.grpcStream.Send(req); err != nil {
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, fmt.Errorf("failed to send ack request: %w", err)
	}

	// Wait for response
	select {
	case result := <-ch:
		s.ackCorrelator.Delete(correlationID)
		if result.Error != nil {
			return result, result.Error
		}
		s.logger.Debug("ack successful", "subscription_id", subscriptionID, "message_count", len(messageIDs))
		return result, nil
	case <-time.After(10 * time.Second):
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, fmt.Errorf("ack timeout")
	case <-s.ctx.Done():
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, s.ctx.Err()
	}
}

// Nack negatively acknowledges one or more messages for a subscription
func (s *stream) Nack(subscriptionID uint32, messageIDs ...[]byte) (models.NackResult, error) {
	if !s.connected.Load() {
		return models.NackResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.NackResult{}, fmt.Errorf("stream is closed")
	}

	if len(messageIDs) == 0 {
		return models.NackResult{}, fmt.Errorf("at least one message ID is required")
	}

	ch := make(chan models.NackResult, 1)
	correlationID := s.nackCorrelator.Next(ch)

	// Create nack request
	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Nack{
			Nack: &pb.NackRequest{
				CorrelationId:  correlationID,
				MessageIds:     messageIDs,
				SubscriptionId: subscriptionID,
			},
		},
	}

	// Send request
	if err := s.grpcStream.Send(req); err != nil {
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, fmt.Errorf("failed to send nack request: %w", err)
	}

	// Wait for response
	select {
	case result := <-ch:
		s.nackCorrelator.Delete(correlationID)
		if result.Error != nil {
			return result, result.Error
		}
		s.logger.Debug("nack successful", "subscription_id", subscriptionID, "message_count", len(messageIDs))
		return result, nil
	case <-time.After(10 * time.Second):
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, fmt.Errorf("nack timeout")
	case <-s.ctx.Done():
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, s.ctx.Err()
	}
}
