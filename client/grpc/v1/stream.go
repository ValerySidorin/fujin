package v1

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ValerySidorin/fujin/client/correlator"
	"github.com/ValerySidorin/fujin/client/models"
	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
)

// stringToBytes converts string to []byte without allocation using unsafe.
// The returned byte slice must not be modified as it shares underlying data with the string.
func stringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// bytesToString converts []byte to string without allocation using unsafe.
// The byte slice must not be modified after calling this function.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// stream implements the Stream interface
type stream struct {
	client   pb.FujinServiceClient
	streamID string
	logger   *slog.Logger

	grpcStream pb.FujinService_StreamClient

	connected atomic.Bool
	closed    atomic.Bool

	correlationID atomic.Uint32

	produceCorrelator     *correlator.Correlator[error]
	subscribeCorrelator   *correlator.Correlator[uint32]
	hsubscribeCorrelator  *correlator.Correlator[uint32]
	fetchCorrelator       *correlator.Correlator[models.FetchResult]
	hfetchCorrelator      *correlator.Correlator[models.FetchResult]
	beginTxCorrelator     *correlator.Correlator[error]
	commitTxCorrelator    *correlator.Correlator[error]
	rollbackTxCorrelator  *correlator.Correlator[error]
	unsubscribeCorrelator *correlator.Correlator[error]
	ackCorrelator         *correlator.Correlator[models.AckResult]
	nackCorrelator        *correlator.Correlator[models.NackResult]

	subscriptions map[uint32]*subscription
	subsMu        sync.RWMutex

	responseCh   chan *pb.FujinResponse
	responseDone chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

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
		hsubscribeCorrelator:  correlator.New[uint32](),
		fetchCorrelator:       correlator.New[models.FetchResult](),
		hfetchCorrelator:      correlator.New[models.FetchResult](),
		beginTxCorrelator:     correlator.New[error](),
		commitTxCorrelator:    correlator.New[error](),
		rollbackTxCorrelator:  correlator.New[error](),
		unsubscribeCorrelator: correlator.New[error](),
		ackCorrelator:         correlator.New[models.AckResult](),
		nackCorrelator:        correlator.New[models.NackResult](),
	}

	if err := s.start(); err != nil {
		cancel()
		return nil, err
	}

	return s, nil
}

// start initializes the gRPC stream and starts response handling
func (s *stream) start() error {
	grpcStream, err := s.client.Stream(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to create gRPC stream: %w", err)
	}
	s.grpcStream = grpcStream

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

			s.routeResponse(resp)
		}
	}
}

// routeResponse routes incoming responses to appropriate correlators
func (s *stream) routeResponse(resp *pb.FujinResponse) {
	switch r := resp.Response.(type) {
	case *pb.FujinResponse_Connect:
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
	case *pb.FujinResponse_Hproduce:
		if r.Hproduce.Error != "" {
			s.produceCorrelator.Send(r.Hproduce.CorrelationId, fmt.Errorf("hproduce error: %s", r.Hproduce.Error))
		} else {
			s.produceCorrelator.Send(r.Hproduce.CorrelationId, nil)
		}
	case *pb.FujinResponse_Subscribe:
		if r.Subscribe.Error != "" {
			s.subscribeCorrelator.Send(r.Subscribe.CorrelationId, 0)
		} else {
			s.subscribeCorrelator.Send(r.Subscribe.CorrelationId, r.Subscribe.SubscriptionId)
		}
	case *pb.FujinResponse_Hsubscribe:
		if r.Hsubscribe.Error != "" {
			s.hsubscribeCorrelator.Send(r.Hsubscribe.CorrelationId, 0)
		} else {
			s.hsubscribeCorrelator.Send(r.Hsubscribe.CorrelationId, r.Hsubscribe.SubscriptionId)
		}
	case *pb.FujinResponse_Fetch:
		result := models.FetchResult{
			SubscriptionID: r.Fetch.SubscriptionId,
			Messages:       make([]models.Msg, 0, len(r.Fetch.Messages)),
		}
		if r.Fetch.Error != "" {
			result.Error = fmt.Errorf("fetch error: %s", r.Fetch.Error)
		}
		for _, msg := range r.Fetch.Messages {
			result.Messages = append(result.Messages, models.Msg{
				SubscriptionID: r.Fetch.SubscriptionId,
				MessageID:      msg.MessageId,
				Payload:        msg.Payload,
			})
		}
		s.fetchCorrelator.Send(r.Fetch.CorrelationId, result)
	case *pb.FujinResponse_Hfetch:
		result := models.FetchResult{
			SubscriptionID: r.Hfetch.SubscriptionId,
			Messages:       make([]models.Msg, 0, len(r.Hfetch.Messages)),
		}
		if r.Hfetch.Error != "" {
			result.Error = fmt.Errorf("hfetch error: %s", r.Hfetch.Error)
		}
		for _, msg := range r.Hfetch.Messages {
			headers := make(map[string]string, len(msg.Headers))
			for _, h := range msg.Headers {
				headers[bytesToString(h.Key)] = bytesToString(h.Value)
			}
			result.Messages = append(result.Messages, models.Msg{
				SubscriptionID: r.Hfetch.SubscriptionId,
				MessageID:      msg.MessageId,
				Payload:        msg.Payload,
				Headers:        headers,
			})
		}
		s.hfetchCorrelator.Send(r.Hfetch.CorrelationId, result)
	case *pb.FujinResponse_BeginTx:
		if r.BeginTx.Error != "" {
			s.beginTxCorrelator.Send(r.BeginTx.CorrelationId, fmt.Errorf("begin tx error: %s", r.BeginTx.Error))
		} else {
			s.beginTxCorrelator.Send(r.BeginTx.CorrelationId, nil)
		}
	case *pb.FujinResponse_CommitTx:
		if r.CommitTx.Error != "" {
			s.commitTxCorrelator.Send(r.CommitTx.CorrelationId, fmt.Errorf("commit tx error: %s", r.CommitTx.Error))
		} else {
			s.commitTxCorrelator.Send(r.CommitTx.CorrelationId, nil)
		}
	case *pb.FujinResponse_RollbackTx:
		if r.RollbackTx.Error != "" {
			s.rollbackTxCorrelator.Send(r.RollbackTx.CorrelationId, fmt.Errorf("rollback tx error: %s", r.RollbackTx.Error))
		} else {
			s.rollbackTxCorrelator.Send(r.RollbackTx.CorrelationId, nil)
		}
	case *pb.FujinResponse_Unsubscribe:
		if r.Unsubscribe.Error != "" {
			s.unsubscribeCorrelator.Send(r.Unsubscribe.CorrelationId, fmt.Errorf("unsubscribe error: %s", r.Unsubscribe.Error))
		} else {
			s.unsubscribeCorrelator.Send(r.Unsubscribe.CorrelationId, nil)
		}
	case *pb.FujinResponse_Message:
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[r.Message.SubscriptionId]; exists {
			sub.handler(models.Msg{
				SubscriptionID: r.Message.SubscriptionId,
				MessageID:      r.Message.MessageId,
				Payload:        r.Message.Payload,
				Headers:        nil,
			})
		}
		s.subsMu.RUnlock()
	case *pb.FujinResponse_Hmessage:
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[r.Hmessage.SubscriptionId]; exists {
			headers := make(map[string]string, len(r.Hmessage.Headers))
			for _, h := range r.Hmessage.Headers {
				headers[bytesToString(h.Key)] = bytesToString(h.Value)
			}
			sub.handler(models.Msg{
				SubscriptionID: r.Hmessage.SubscriptionId,
				MessageID:      r.Hmessage.MessageId,
				Payload:        r.Hmessage.Payload,
				Headers:        headers,
			})
		}
		s.subsMu.RUnlock()
	case *pb.FujinResponse_Ack:
		result := models.AckResult{}
		if r.Ack.Error != "" {
			result.Error = fmt.Errorf("ack error: %s", r.Ack.Error)
		}
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

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Produce{
			Produce: &pb.ProduceRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				Message:       p,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send produce request: %w", err)
	}

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

// HProduce sends a message with headers to the specified topic
func (s *stream) HProduce(topic string, p []byte, headers map[string]string) error {
	return s.hproduce(topic, p, headers)
}

// hproduce sends a message with headers to a topic
func (s *stream) hproduce(topic string, p []byte, headers map[string]string) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.produceCorrelator.Next(ch)

	protoHeaders := make([]*pb.Header, 0, len(headers))
	for k, v := range headers {
		protoHeaders = append(protoHeaders, &pb.Header{
			Key:   stringToBytes(k),
			Value: stringToBytes(v),
		})
	}

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Hproduce{
			Hproduce: &pb.HProduceRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				Headers:       protoHeaders,
				Message:       p,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send hproduce request: %w", err)
	}

	select {
	case err := <-ch:
		s.produceCorrelator.Delete(correlationID)
		return err
	case <-time.After(10 * time.Second):
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("hproduce timeout")
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

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Subscribe{
			Subscribe: &pb.SubscribeRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				AutoCommit:    autoCommit,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.subscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	select {
	case subscriptionID := <-ch:
		s.subscribeCorrelator.Delete(correlationID)

		if subscriptionID == 0 {
			return 0, fmt.Errorf("subscribe failed")
		}

		sub := &subscription{
			id:      subscriptionID,
			topic:   topic,
			handler: handler,
			stream:  s,
		}

		s.subsMu.Lock()
		s.subscriptions[subscriptionID] = sub
		s.subsMu.Unlock()

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

// HSubscribe subscribes to a topic with headers support
func (s *stream) HSubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	return s.hsubscribe(topic, autoCommit, handler)
}

func (s *stream) hsubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	if !s.connected.Load() {
		return 0, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return 0, fmt.Errorf("stream is closed")
	}

	ch := make(chan uint32, 1)
	correlationID := s.hsubscribeCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Hsubscribe{
			Hsubscribe: &pb.HSubscribeRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				AutoCommit:    autoCommit,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("failed to send hsubscribe request: %w", err)
	}

	select {
	case subscriptionID := <-ch:
		s.hsubscribeCorrelator.Delete(correlationID)

		if subscriptionID == 0 {
			return 0, fmt.Errorf("hsubscribe failed")
		}

		sub := &subscription{
			id:      subscriptionID,
			topic:   topic,
			handler: handler,
			stream:  s,
		}

		s.subsMu.Lock()
		s.subscriptions[subscriptionID] = sub
		s.subsMu.Unlock()

		s.wg.Add(1)
		go s.handleMessages(sub)

		s.logger.Info("hsubscribed to topic", "topic", topic, "subscription_id", subscriptionID)
		return subscriptionID, nil
	case <-time.After(10 * time.Second):
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("hsubscribe timeout")
	case <-s.ctx.Done():
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, s.ctx.Err()
	}
}

// Fetch requests a batch of messages from a topic (pull-based)
func (s *stream) Fetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	return s.fetch(topic, autoCommit, batchSize)
}

// fetch sends a fetch request and waits for the response
func (s *stream) fetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	if !s.connected.Load() {
		return models.FetchResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.FetchResult{}, fmt.Errorf("stream is closed")
	}

	ch := make(chan models.FetchResult, 1)
	correlationID := s.fetchCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Fetch{
			Fetch: &pb.FetchRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				AutoCommit:    autoCommit,
				BatchSize:     batchSize,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("failed to send fetch request: %w", err)
	}

	select {
	case result := <-ch:
		s.fetchCorrelator.Delete(correlationID)
		return result, result.Error
	case <-time.After(10 * time.Second):
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("fetch timeout")
	case <-s.ctx.Done():
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, s.ctx.Err()
	}
}

// HFetch requests a batch of messages with headers from a topic (pull-based)
func (s *stream) HFetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	return s.hfetch(topic, autoCommit, batchSize)
}

// hfetch sends an hfetch request and waits for the response
func (s *stream) hfetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	if !s.connected.Load() {
		return models.FetchResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.FetchResult{}, fmt.Errorf("stream is closed")
	}

	ch := make(chan models.FetchResult, 1)
	correlationID := s.hfetchCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Hfetch{
			Hfetch: &pb.HFetchRequest{
				CorrelationId: correlationID,
				Topic:         topic,
				AutoCommit:    autoCommit,
				BatchSize:     batchSize,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("failed to send hfetch request: %w", err)
	}

	select {
	case result := <-ch:
		s.hfetchCorrelator.Delete(correlationID)
		return result, result.Error
	case <-time.After(10 * time.Second):
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("hfetch timeout")
	case <-s.ctx.Done():
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, s.ctx.Err()
	}
}

// BeginTx begins a transaction
func (s *stream) BeginTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.beginTxCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_BeginTx{
			BeginTx: &pb.BeginTxRequest{
				CorrelationId: correlationID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.beginTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send begin tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.beginTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(10 * time.Second):
		s.beginTxCorrelator.Delete(correlationID)
		return fmt.Errorf("begin tx timeout")
	case <-s.ctx.Done():
		s.beginTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// CommitTx commits the current transaction
func (s *stream) CommitTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.commitTxCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_CommitTx{
			CommitTx: &pb.CommitTxRequest{
				CorrelationId: correlationID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.commitTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send commit tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.commitTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(10 * time.Second):
		s.commitTxCorrelator.Delete(correlationID)
		return fmt.Errorf("commit tx timeout")
	case <-s.ctx.Done():
		s.commitTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// RollbackTx rolls back the current transaction
func (s *stream) RollbackTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.rollbackTxCorrelator.Next(ch)

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_RollbackTx{
			RollbackTx: &pb.RollbackTxRequest{
				CorrelationId: correlationID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.rollbackTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send rollback tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.rollbackTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(10 * time.Second):
		s.rollbackTxCorrelator.Delete(correlationID)
		return fmt.Errorf("rollback tx timeout")
	case <-s.ctx.Done():
		s.rollbackTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
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
					sub.handler(models.Msg{
						SubscriptionID: msgResp.Message.SubscriptionId,
						MessageID:      msgResp.Message.MessageId,
						Payload:        msgResp.Message.Payload,
					})
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

	s.subsMu.Lock()
	for _, sub := range s.subscriptions {
		sub.closed.Store(true)
	}
	s.subscriptions = make(map[uint32]*subscription)
	s.subsMu.Unlock()

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

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Unsubscribe{
			Unsubscribe: &pb.UnsubscribeRequest{
				CorrelationId:  correlationID,
				SubscriptionId: subscriptionID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.unsubscribeCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send unsubscribe request: %w", err)
	}

	select {
	case err := <-ch:
		s.unsubscribeCorrelator.Delete(correlationID)

		if err != nil {
			return err
		}

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

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Ack{
			Ack: &pb.AckRequest{
				CorrelationId:  correlationID,
				MessageIds:     messageIDs,
				SubscriptionId: subscriptionID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, fmt.Errorf("failed to send ack request: %w", err)
	}

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

	req := &pb.FujinRequest{
		Request: &pb.FujinRequest_Nack{
			Nack: &pb.NackRequest{
				CorrelationId:  correlationID,
				MessageIds:     messageIDs,
				SubscriptionId: subscriptionID,
			},
		},
	}

	if err := s.grpcStream.Send(req); err != nil {
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, fmt.Errorf("failed to send nack request: %w", err)
	}

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
