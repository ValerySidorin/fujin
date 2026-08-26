package v1

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/config"
	pb "github.com/fujin-io/fujin/sdk/go/client/grpc/v1/proto"
	"github.com/fujin-io/fujin/sdk/go/client/internal/session"
)

const defaultRPCWait = 10 * time.Second

type stream struct {
	wire    pb.FujinService_StreamClient
	core    *session.Core
	rpcWait time.Duration

	cancel context.CancelFunc
	closed atomic.Bool
	sendMu sync.Mutex
	done   chan struct{}
}

func newStream(parent context.Context, service pb.FujinServiceClient, connector string, meta, overrides map[string]string, cfg *config.StreamConfig) (*stream, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	s := &stream{core: session.New(), rpcWait: defaultRPCWait, cancel: cancel, done: make(chan struct{})}
	if cfg != nil && cfg.GRPC != nil && cfg.GRPC.RPCWait > 0 {
		s.rpcWait = cfg.GRPC.RPCWait
	}
	wire, err := service.Stream(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create gRPC stream: %w", err)
	}
	s.wire = wire
	if err := s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Bind{Bind: &pb.BindRequest{Connector: connector, Meta: meta, ConfigOverrides: overrides}}}); err != nil {
		cancel()
		return nil, err
	}
	response, err := wire.Recv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("receive BIND: %w", err)
	}
	bind := response.GetBind()
	if bind == nil {
		cancel()
		return nil, fmt.Errorf("unexpected BIND response %T", response.GetResponse())
	}
	if bind.Error != nil {
		cancel()
		return nil, operationError(bind.Error)
	}
	s.core.Bind(routeCapabilities(bind.Routes))
	go s.readLoop()
	return s, nil
}

func (s *stream) Routes() map[string]client.RouteCapabilities { return s.core.Routes() }

func (s *stream) Produce(ctx context.Context, route string, payload []byte) error {
	if err := s.core.EnsureNoTransaction(); err != nil {
		return err
	}
	return s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_Produce{Produce: &pb.ProduceRequest{CorrelationId: id, Route: route, Message: payload}}}
	})
}
func (s *stream) HProduce(ctx context.Context, route string, payload []byte, headers []client.Header) error {
	if err := s.core.EnsureNoTransaction(); err != nil {
		return err
	}
	return s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_Hproduce{Hproduce: &pb.HProduceRequest{CorrelationId: id, Route: route, Message: payload, Headers: protoHeaders(headers)}}}
	})
}
func (s *stream) TxProduce(ctx context.Context, payload []byte) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	return s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_TxProduce{TxProduce: &pb.TxProduceRequest{CorrelationId: id, Message: payload}}}
	})
}
func (s *stream) TxHProduce(ctx context.Context, payload []byte, headers []client.Header) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	return s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_TxHproduce{TxHproduce: &pb.TxHProduceRequest{CorrelationId: id, Message: payload, Headers: protoHeaders(headers)}}}
	})
}
func (s *stream) BeginTx(ctx context.Context, route string) error {
	if err := s.core.BeginTransaction(); err != nil {
		return err
	}
	err := s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_BeginTx{BeginTx: &pb.BeginTxRequest{CorrelationId: id, Route: route}}}
	})
	if err != nil {
		s.core.EndTransaction()
	}
	return err
}
func (s *stream) CommitTx(ctx context.Context) error   { return s.endTx(ctx, true) }
func (s *stream) RollbackTx(ctx context.Context) error { return s.endTx(ctx, false) }
func (s *stream) endTx(ctx context.Context, commit bool) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	defer s.core.EndTransaction()
	if commit {
		return s.call(ctx, func(id uint32) *pb.FujinRequest {
			return &pb.FujinRequest{Request: &pb.FujinRequest_CommitTx{CommitTx: &pb.CommitTxRequest{CorrelationId: id}}}
		})
	}
	return s.call(ctx, func(id uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_RollbackTx{RollbackTx: &pb.RollbackTxRequest{CorrelationId: id}}}
	})
}

func (s *stream) Fetch(ctx context.Context, route string, autoSettle bool, maximum uint32) (client.FetchResult, error) {
	return s.fetch(ctx, route, autoSettle, maximum, false)
}
func (s *stream) HFetch(ctx context.Context, route string, autoSettle bool, maximum uint32) (client.FetchResult, error) {
	return s.fetch(ctx, route, autoSettle, maximum, true)
}
func (s *stream) fetch(ctx context.Context, route string, autoSettle bool, maximum uint32, headered bool) (client.FetchResult, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	response, err := s.core.Call(ctx, func(id uint32) error {
		if headered {
			return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Hfetch{Hfetch: &pb.HFetchRequest{CorrelationId: id, Route: route, AutoCommit: autoSettle, BatchSize: maximum}}})
		}
		return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Fetch{Fetch: &pb.FetchRequest{CorrelationId: id, Route: route, AutoCommit: autoSettle, BatchSize: maximum}}})
	})
	if err != nil {
		return client.FetchResult{}, err
	}
	result, ok := response.Value.(client.FetchResult)
	if !ok {
		return client.FetchResult{}, fmt.Errorf("invalid fetch response")
	}
	return result, nil
}

func (s *stream) Subscribe(ctx context.Context, route string, autoSettle bool, handler func(client.Message)) (client.Subscription, error) {
	return s.subscribe(ctx, route, autoSettle, false, handler)
}
func (s *stream) HSubscribe(ctx context.Context, route string, autoSettle bool, handler func(client.Message)) (client.Subscription, error) {
	return s.subscribe(ctx, route, autoSettle, true, handler)
}
func (s *stream) subscribe(ctx context.Context, route string, autoSettle, headered bool, handler func(client.Message)) (client.Subscription, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if handler == nil {
		handler = func(client.Message) {}
	}
	response, err := s.core.Call(ctx, func(id uint32) error {
		if err := s.core.PrepareSubscription(id, handler); err != nil {
			return err
		}
		if headered {
			return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Hsubscribe{Hsubscribe: &pb.HSubscribeRequest{CorrelationId: id, Route: route, AutoCommit: autoSettle}}})
		}
		return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Subscribe{Subscribe: &pb.SubscribeRequest{CorrelationId: id, Route: route, AutoCommit: autoSettle}}})
	})
	if err != nil {
		return nil, err
	}
	id, ok := response.Value.(uint32)
	if !ok {
		return nil, fmt.Errorf("invalid subscription response")
	}
	return &subscription{id: id, stream: s}, nil
}

func (s *stream) Unsubscribe(ctx context.Context, id uint32) error {
	err := s.call(ctx, func(correlationID uint32) *pb.FujinRequest {
		return &pb.FujinRequest{Request: &pb.FujinRequest_Unsubscribe{Unsubscribe: &pb.UnsubscribeRequest{CorrelationId: correlationID, SubscriptionId: id}}}
	})
	if err == nil {
		s.core.RemoveSubscription(id)
	}
	return err
}
func (s *stream) Ack(ctx context.Context, id uint32, messageIDs ...[]byte) ([]client.SettlementResult, error) {
	return s.settle(ctx, id, messageIDs, false)
}
func (s *stream) Nack(ctx context.Context, id uint32, messageIDs ...[]byte) ([]client.SettlementResult, error) {
	return s.settle(ctx, id, messageIDs, true)
}
func (s *stream) settle(ctx context.Context, id uint32, messageIDs [][]byte, nack bool) ([]client.SettlementResult, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	response, err := s.core.Call(ctx, func(correlationID uint32) error {
		if nack {
			return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Nack{Nack: &pb.NackRequest{CorrelationId: correlationID, SubscriptionId: id, MessageIds: messageIDs}}})
		}
		return s.send(&pb.FujinRequest{Request: &pb.FujinRequest_Ack{Ack: &pb.AckRequest{CorrelationId: correlationID, SubscriptionId: id, MessageIds: messageIDs}}})
	})
	if err != nil {
		return nil, err
	}
	results, ok := response.Value.([]client.SettlementResult)
	if !ok {
		return nil, fmt.Errorf("invalid settlement response")
	}
	return results, nil
}

func (s *stream) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.wire.CloseSend(); err != nil {
		s.cancel()
		return err
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.cancel()
		<-s.done
		return ctx.Err()
	}
}

func (s *stream) call(ctx context.Context, request func(uint32) *pb.FujinRequest) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	_, err := s.core.Call(ctx, func(id uint32) error { return s.send(request(id)) })
	return err
}
func (s *stream) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || s.rpcWait <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.rpcWait)
}
func (s *stream) send(request *pb.FujinRequest) error {
	if s.closed.Load() {
		return session.ErrClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed.Load() {
		return session.ErrClosed
	}
	return s.wire.Send(request)
}

func (s *stream) readLoop() {
	defer close(s.done)
	for {
		response, err := s.wire.Recv()
		if err != nil {
			s.core.Close(err)
			return
		}
		s.routeResponse(response)
	}
}

func (s *stream) routeResponse(response *pb.FujinResponse) {
	switch value := response.GetResponse().(type) {
	case *pb.FujinResponse_Produce:
		s.complete(value.Produce.CorrelationId, value.Produce.Error, nil)
	case *pb.FujinResponse_Hproduce:
		s.complete(value.Hproduce.CorrelationId, value.Hproduce.Error, nil)
	case *pb.FujinResponse_TxProduce:
		s.complete(value.TxProduce.CorrelationId, value.TxProduce.Error, nil)
	case *pb.FujinResponse_TxHproduce:
		s.complete(value.TxHproduce.CorrelationId, value.TxHproduce.Error, nil)
	case *pb.FujinResponse_BeginTx:
		s.complete(value.BeginTx.CorrelationId, value.BeginTx.Error, nil)
	case *pb.FujinResponse_CommitTx:
		s.complete(value.CommitTx.CorrelationId, value.CommitTx.Error, nil)
	case *pb.FujinResponse_RollbackTx:
		s.complete(value.RollbackTx.CorrelationId, value.RollbackTx.Error, nil)
	case *pb.FujinResponse_Unsubscribe:
		s.complete(value.Unsubscribe.CorrelationId, value.Unsubscribe.Error, nil)
	case *pb.FujinResponse_Subscribe:
		s.completeSubscription(value.Subscribe.CorrelationId, value.Subscribe.SubscriptionId, value.Subscribe.Error)
	case *pb.FujinResponse_Hsubscribe:
		s.completeSubscription(value.Hsubscribe.CorrelationId, value.Hsubscribe.SubscriptionId, value.Hsubscribe.Error)
	case *pb.FujinResponse_Fetch:
		s.complete(value.Fetch.CorrelationId, value.Fetch.Error, fetchResult(value.Fetch.SubscriptionId, value.Fetch.Messages))
	case *pb.FujinResponse_Hfetch:
		s.complete(value.Hfetch.CorrelationId, value.Hfetch.Error, hfetchResult(value.Hfetch.SubscriptionId, value.Hfetch.Messages))
	case *pb.FujinResponse_Ack:
		s.complete(value.Ack.CorrelationId, value.Ack.Error, ackResults(value.Ack.Results))
	case *pb.FujinResponse_Nack:
		s.complete(value.Nack.CorrelationId, value.Nack.Error, nackResults(value.Nack.Results))
	case *pb.FujinResponse_Message:
		s.core.Deliver(client.Message{SubscriptionID: value.Message.SubscriptionId, MessageID: value.Message.MessageId, Payload: value.Message.Payload})
	case *pb.FujinResponse_Hmessage:
		s.core.Deliver(client.Message{SubscriptionID: value.Hmessage.SubscriptionId, MessageID: value.Hmessage.MessageId, Payload: value.Hmessage.Payload, Headers: headers(value.Hmessage.Headers)})
	}
}
func (s *stream) complete(id uint32, protoErr *pb.OperationError, value any) {
	s.core.Complete(id, session.Response{Value: value, Error: operationError(protoErr)})
}
func (s *stream) completeSubscription(id, subscriptionID uint32, protoErr *pb.OperationError) {
	if protoErr == nil {
		if err := s.core.ActivateSubscription(id, subscriptionID); err != nil {
			s.core.Complete(id, session.Response{Error: &client.OperationError{Code: client.StatusInternal, Outcome: client.OutcomeUnknown, Reason: "CLIENT_SUBSCRIPTION_STATE", Message: err.Error()}})
			return
		}
	}
	s.complete(id, protoErr, subscriptionID)
}

func operationError(value *pb.OperationError) *client.OperationError {
	if value == nil {
		return nil
	}
	return &client.OperationError{Code: client.StatusCode(value.Code), Outcome: client.OperationOutcome(value.Outcome), Reason: value.Reason, Message: value.Message, Details: value.Details}
}
func routeCapabilities(values map[string]*pb.RouteCapabilities) map[string]client.RouteCapabilities {
	result := make(map[string]client.RouteCapabilities, len(values))
	for route, value := range values {
		result[route] = client.RouteCapabilities{Produce: value.Produce, Headers: value.Headers, Transactions: value.Transactions, Subscribe: value.Subscribe, Fetch: value.Fetch, ManualSettlement: value.ManualSettlement, ProduceGuarantee: client.ProduceGuarantee(value.ProduceGuarantee), AckGranularity: client.AckGranularity(value.AckGranularity), NackEffect: client.NackEffect(value.NackEffect)}
	}
	return result
}
func protoHeaders(values []client.Header) []*pb.KV {
	result := make([]*pb.KV, len(values))
	for i, value := range values {
		result[i] = &pb.KV{Key: value.Key, Value: value.Value}
	}
	return result
}
func headers(values []*pb.KV) []client.Header {
	result := make([]client.Header, len(values))
	for i, value := range values {
		result[i] = client.Header{Key: value.Key, Value: value.Value}
	}
	return result
}
func fetchResult(id uint32, messages []*pb.FetchMessage) client.FetchResult {
	result := client.FetchResult{SubscriptionID: id, Messages: make([]client.Message, len(messages))}
	for i, message := range messages {
		result.Messages[i] = client.Message{SubscriptionID: id, MessageID: message.MessageId, Payload: message.Payload}
	}
	return result
}
func hfetchResult(id uint32, messages []*pb.HFetchMessage) client.FetchResult {
	result := client.FetchResult{SubscriptionID: id, Messages: make([]client.Message, len(messages))}
	for i, message := range messages {
		result.Messages[i] = client.Message{SubscriptionID: id, MessageID: message.MessageId, Payload: message.Payload, Headers: headers(message.Headers)}
	}
	return result
}
func ackResults(values []*pb.AckMessageResult) []client.SettlementResult {
	result := make([]client.SettlementResult, len(values))
	for i, value := range values {
		result[i] = client.SettlementResult{MessageID: value.MessageId, Error: operationError(value.Error)}
	}
	return result
}
func nackResults(values []*pb.NackMessageResult) []client.SettlementResult {
	result := make([]client.SettlementResult, len(values))
	for i, value := range values {
		result[i] = client.SettlementResult{MessageID: value.MessageId, Error: operationError(value.Error)}
	}
	return result
}
