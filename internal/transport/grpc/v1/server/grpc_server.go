//go:build grpc

package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/fujin-io/fujin/internal/core"
	"github.com/fujin-io/fujin/public/plugins/connector"
	pb "github.com/fujin-io/fujin/public/proto/grpc/v1"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer implements the Fujin gRPC service
type GRPCServer struct {
	pb.UnimplementedFujinServiceServer

	conf    serverconfig.GRPCServerConfig
	catalog *connector.Catalog
	l       *slog.Logger

	lis        net.Listener // stored for ListenerFDs
	grpcServer *grpc.Server
	healthSrv  *health.Server
	ready      chan struct{}
	done       chan struct{}
}

// NewGRPCServer creates a new gRPC server instance.
func NewGRPCServer(conf serverconfig.GRPCServerConfig, catalog *connector.Catalog, l *slog.Logger) *GRPCServer {
	return &GRPCServer{
		conf:    conf,
		catalog: catalog,
		l:       l.With("server", "grpc"),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// ListenAndServe starts the gRPC server
func (s *GRPCServer) ListenAndServe(ctx context.Context) error {
	defer close(s.done)
	lis, err := net.Listen("tcp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.conf.Addr, err)
	}

	s.initGRPCServer()
	s.l.Info("grpc server started", "addr", s.conf.Addr)

	return s.serveListener(ctx, lis)
}

// ListenAndServeInherited starts the gRPC server on an inherited listener.
func (s *GRPCServer) ListenAndServeInherited(ctx context.Context, lis net.Listener) error {
	defer close(s.done)
	s.initGRPCServer()
	s.l.Info("grpc server started (inherited)", "addr", s.conf.Addr)

	return s.serveListener(ctx, lis)
}

func (s *GRPCServer) initGRPCServer() {
	var serverOpts []grpc.ServerOption

	// Connection settings
	if s.conf.ConnectionTimeout > 0 {
		serverOpts = append(serverOpts, grpc.ConnectionTimeout(s.conf.ConnectionTimeout))
	}

	if s.conf.MaxConcurrentStreams > 0 {
		serverOpts = append(serverOpts, grpc.MaxConcurrentStreams(s.conf.MaxConcurrentStreams))
	}

	// Message size limits
	if s.conf.MaxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(s.conf.MaxRecvMsgSize))
	}
	if s.conf.MaxSendMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(s.conf.MaxSendMsgSize))
	}

	// Flow control window sizes
	if s.conf.InitialWindowSize > 0 {
		serverOpts = append(serverOpts, grpc.InitialWindowSize(s.conf.InitialWindowSize))
	}
	if s.conf.InitialConnWindowSize > 0 {
		serverOpts = append(serverOpts, grpc.InitialConnWindowSize(s.conf.InitialConnWindowSize))
	}

	// Server KeepAlive settings
	if s.conf.ServerKeepAlive.Time > 0 || s.conf.ServerKeepAlive.Timeout > 0 {
		kaParams := keepalive.ServerParameters{
			Time:    s.conf.ServerKeepAlive.Time,
			Timeout: s.conf.ServerKeepAlive.Timeout,
		}
		if s.conf.ServerKeepAlive.MaxConnectionIdle > 0 {
			kaParams.MaxConnectionIdle = s.conf.ServerKeepAlive.MaxConnectionIdle
		}
		if s.conf.ServerKeepAlive.MaxConnectionAge > 0 {
			kaParams.MaxConnectionAge = s.conf.ServerKeepAlive.MaxConnectionAge
		}
		if s.conf.ServerKeepAlive.MaxConnectionAgeGrace > 0 {
			kaParams.MaxConnectionAgeGrace = s.conf.ServerKeepAlive.MaxConnectionAgeGrace
		}
		serverOpts = append(serverOpts, grpc.KeepaliveParams(kaParams))
	}

	// Client KeepAlive settings
	if s.conf.ClientKeepAlive.MinTime > 0 {
		kaPolicy := keepalive.EnforcementPolicy{
			MinTime:             s.conf.ClientKeepAlive.MinTime,
			PermitWithoutStream: s.conf.ClientKeepAlive.PermitWithoutStream,
		}
		serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(kaPolicy))
	}

	// TLS configuration
	if s.conf.TLS != nil && len(s.conf.TLS.Certificates) > 0 {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(s.conf.TLS)))
	} else {
		s.l.Warn("tls not configured, this is not recommended for production environment")
	}

	s.grpcServer = grpc.NewServer(serverOpts...)
	pb.RegisterFujinServiceServer(s.grpcServer, s)

	// Register standard gRPC health check service
	s.healthSrv = health.NewServer()
	healthpb.RegisterHealthServer(s.grpcServer, s.healthSrv)
	s.healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}

// Stop gracefully stops the gRPC server
func (s *GRPCServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func (s *GRPCServer) serveListener(ctx context.Context, lis net.Listener) error {
	s.lis = lis

	errCh := make(chan error, 1)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	s.healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	close(s.ready)

	select {
	case <-ctx.Done():
		s.l.Info("shutting down grpc server")
		s.healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		s.grpcServer.GracefulStop()
		s.l.Info("grpc server stopped")
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *GRPCServer) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	case <-s.done:
		return false
	}
}

func (s *GRPCServer) Done() <-chan struct{} {
	return s.done
}

// Stream implements the bidirectional streaming RPC.
func (s *GRPCServer) Stream(stream pb.FujinService_StreamServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ss := &streamSession{
		stream:   stream,
		core:     core.New(ctx, s.catalog.Current(), s.catalog.Current, s.l),
		l:        s.l,
		ctx:      ctx,
		cancel:   cancel,
		terminal: make(chan error, 1),
	}

	err := ss.receiveLoop()
	if closeErr := ss.core.Close(); closeErr != nil {
		s.l.Error("close session", "err", closeErr)
	}
	if err == io.EOF {
		return nil
	}
	return err
}

// streamSession is a thin protobuf and stream-lifecycle adapter around Session Core.
type streamSession struct {
	stream   pb.FujinService_StreamServer
	core     *core.Core
	l        *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	terminal chan error
	sendMu   sync.Mutex
}

type grpcFetchLease struct {
	messages  []*pb.FetchMessage
	hmessages []*pb.HFetchMessage
}

var grpcFetchLeases = sync.Pool{
	New: func() any {
		return new(grpcFetchLease)
	},
}

func getGRPCFetchLease(batchSize int, withHeaders bool) *grpcFetchLease {
	lease := grpcFetchLeases.Get().(*grpcFetchLease)
	lease.messages = lease.messages[:0]
	lease.hmessages = lease.hmessages[:0]
	if withHeaders {
		if cap(lease.hmessages) < batchSize {
			lease.hmessages = make([]*pb.HFetchMessage, 0, batchSize)
		}
	} else if cap(lease.messages) < batchSize {
		lease.messages = make([]*pb.FetchMessage, 0, batchSize)
	}
	return lease
}

func putGRPCFetchLease(lease *grpcFetchLease) {
	clear(lease.messages)
	clear(lease.hmessages)
	lease.messages = lease.messages[:0]
	lease.hmessages = lease.hmessages[:0]
	grpcFetchLeases.Put(lease)
}

func (s *streamSession) receiveLoop() error {
	for {
		request, err := s.stream.Recv()
		if err != nil {
			select {
			case terminalErr := <-s.terminal:
				return fmt.Errorf("subscription ended: %w", terminalErr)
			default:
			}
			if err == io.EOF {
				return io.EOF
			}
			return fmt.Errorf("receive error: %w", err)
		}
		if err := s.handleRequest(request); err != nil {
			return fmt.Errorf("handle request: %w", err)
		}
	}
}

func (s *streamSession) handleRequest(req *pb.FujinRequest) error {
	switch r := req.Request.(type) {
	case *pb.FujinRequest_Bind:
		return s.handleBind(r.Bind)
	case *pb.FujinRequest_Produce:
		return s.handleProduce(r.Produce)
	case *pb.FujinRequest_Hproduce:
		return s.handleHProduce(r.Hproduce)
	case *pb.FujinRequest_TxProduce:
		return s.handleTxProduce(r.TxProduce)
	case *pb.FujinRequest_TxHproduce:
		return s.handleTxHProduce(r.TxHproduce)
	case *pb.FujinRequest_BeginTx:
		return s.handleBeginTx(r.BeginTx)
	case *pb.FujinRequest_CommitTx:
		return s.handleCommitTx(r.CommitTx)
	case *pb.FujinRequest_RollbackTx:
		return s.handleRollbackTx(r.RollbackTx)
	case *pb.FujinRequest_Subscribe:
		return s.handleSubscribe(r.Subscribe)
	case *pb.FujinRequest_Hsubscribe:
		return s.handleHSubscribe(r.Hsubscribe)
	case *pb.FujinRequest_Fetch:
		return s.handleFetch(r.Fetch)
	case *pb.FujinRequest_Hfetch:
		return s.handleHFetch(r.Hfetch)
	case *pb.FujinRequest_Unsubscribe:
		return s.handleUnsubscribe(r.Unsubscribe)
	case *pb.FujinRequest_Ack:
		return s.handleAck(r.Ack)
	case *pb.FujinRequest_Nack:
		return s.handleNack(r.Nack)
	default:
		return fmt.Errorf("unknown request type")
	}
}

func (s *streamSession) handleBind(req *pb.BindRequest) error {
	result, err := s.core.Bind(req.Connector, req.Meta, req.ConfigOverrides)
	response := &pb.BindResponse{Error: grpcOperationError(err)}
	if err == nil {
		response.Routes = grpcRouteCapabilities(result.Routes)
	}
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Bind{Bind: response},
	})
}

func grpcRouteCapabilities(routes map[string]connector.RouteProfile) map[string]*pb.RouteCapabilities {
	result := make(map[string]*pb.RouteCapabilities, len(routes))
	for route, profile := range routes {
		result[route] = &pb.RouteCapabilities{
			Produce:          profile.Produce,
			Headers:          profile.Headers,
			Transactions:     profile.Transactions,
			Subscribe:        profile.Subscribe,
			Fetch:            profile.Fetch,
			ManualSettlement: profile.ManualSettlement,
			ProduceGuarantee: pb.ProduceGuarantee(profile.ProduceGuarantee),
			AckGranularity:   pb.AckGranularity(profile.Settlement.Ack),
			NackEffect:       pb.NackEffect(profile.Settlement.Nack),
		}
	}
	return result
}

func (s *streamSession) handleProduce(req *pb.ProduceRequest) error {
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Produce{Produce: &pb.ProduceResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
			}},
		}); sendErr != nil {
			s.l.Error("send produce response", "err", sendErr)
		}
	}
	if err := s.core.Produce(req.Route, req.Message, nil, respond); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) handleHProduce(req *pb.HProduceRequest) error {
	headers := protoHeadersToConnector(req.Headers)
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hproduce{Hproduce: &pb.HProduceResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
			}},
		}); sendErr != nil {
			s.l.Error("send hproduce response", "err", sendErr)
		}
	}
	if err := s.core.Produce(req.Route, req.Message, headers, respond); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) handleTxProduce(req *pb.TxProduceRequest) error {
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_TxProduce{TxProduce: &pb.TxProduceResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
			}},
		}); sendErr != nil {
			s.l.Error("send tx produce response", "err", sendErr)
		}
	}
	if err := s.core.TxProduce(req.Message, nil, respond); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) handleTxHProduce(req *pb.TxHProduceRequest) error {
	headers := protoHeadersToConnector(req.Headers)
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_TxHproduce{TxHproduce: &pb.TxHProduceResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
			}},
		}); sendErr != nil {
			s.l.Error("send tx hproduce response", "err", sendErr)
		}
	}
	if err := s.core.TxProduce(req.Message, headers, respond); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) handleBeginTx(req *pb.BeginTxRequest) error {
	err := s.core.Begin(req.Route)
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_BeginTx{BeginTx: &pb.BeginTxResponse{
			CorrelationId: req.CorrelationId,
			Error:         grpcOperationError(err),
		}},
	})
}

func (s *streamSession) handleCommitTx(req *pb.CommitTxRequest) error {
	err := s.core.Commit()
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_CommitTx{CommitTx: &pb.CommitTxResponse{
			CorrelationId: req.CorrelationId,
			Error:         grpcOperationError(err),
		}},
	})
}

func (s *streamSession) handleRollbackTx(req *pb.RollbackTxRequest) error {
	err := s.core.Rollback()
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_RollbackTx{RollbackTx: &pb.RollbackTxResponse{
			CorrelationId: req.CorrelationId,
			Error:         grpcOperationError(err),
		}},
	})
}

func (s *streamSession) handleSubscribe(req *pb.SubscribeRequest) error {
	return s.subscribe(req.CorrelationId, req.Route, req.AutoCommit, false)
}

func (s *streamSession) handleHSubscribe(req *pb.HSubscribeRequest) error {
	return s.subscribe(req.CorrelationId, req.Route, req.AutoCommit, true)
}

func (s *streamSession) subscribe(correlationID uint32, route string, autoCommit, withHeaders bool) error {
	ready := func(subscriptionID byte) error {
		if withHeaders {
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hsubscribe{Hsubscribe: &pb.HSubscribeResponse{
					CorrelationId:  correlationID,
					SubscriptionId: uint32(subscriptionID),
				}},
			})
		}
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Subscribe{Subscribe: &pb.SubscribeResponse{
				CorrelationId:  correlationID,
				SubscriptionId: uint32(subscriptionID),
			}},
		})
	}

	handlers := core.SubscriptionMessageHandlers{}
	if withHeaders {
		handlers.MessageWithHeaders = func(subscriptionID byte, reader connector.Reader) func([]byte, string, [][]byte, ...any) {
			return func(payload []byte, source string, headers [][]byte, args ...any) {
				messageID := encodeMessageID(reader, autoCommit, source, args...)
				if err := s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Hmessage{Hmessage: &pb.HMessage{
						SubscriptionId: uint32(subscriptionID),
						MessageId:      messageID,
						Payload:        payload,
						Headers:        connectorHeadersToProto(headers),
					}},
				}); err != nil {
					s.l.Error("send hmessage", "subscription_id", subscriptionID, "err", err)
				}
			}
		}
	} else {
		handlers.Message = func(subscriptionID byte, reader connector.Reader) func([]byte, string, ...any) {
			return func(payload []byte, source string, args ...any) {
				messageID := encodeMessageID(reader, autoCommit, source, args...)
				if err := s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Message{Message: &pb.Message{
						SubscriptionId: uint32(subscriptionID),
						MessageId:      messageID,
						Payload:        payload,
					}},
				}); err != nil {
					s.l.Error("send message", "subscription_id", subscriptionID, "err", err)
				}
			}
		}
	}

	err := s.core.Subscribe(route, autoCommit, withHeaders, ready, handlers, func(err error) {
		s.l.Error("subscription ended", "route", route, "err", err)
		select {
		case s.terminal <- err:
		default:
		}
		s.cancel()
	})
	if err == nil {
		return nil
	}
	if withHeaders {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hsubscribe{Hsubscribe: &pb.HSubscribeResponse{
				CorrelationId: correlationID,
				Error:         grpcOperationError(err),
			}},
		})
	}
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Subscribe{Subscribe: &pb.SubscribeResponse{
			CorrelationId: correlationID,
			Error:         grpcOperationError(err),
		}},
	})
}

func (s *streamSession) handleFetch(req *pb.FetchRequest) error {
	return s.fetch(req.CorrelationId, req.Route, req.AutoCommit, false, req.BatchSize)
}

func (s *streamSession) handleHFetch(req *pb.HFetchRequest) error {
	return s.fetch(req.CorrelationId, req.Route, req.AutoCommit, true, req.BatchSize)
}

func (s *streamSession) fetch(correlationID uint32, route string, autoCommit, withHeaders bool, batchSize uint32) error {
	lease := getGRPCFetchLease(int(batchSize), withHeaders)

	handlers := core.FetchMessageHandlers{}
	switch {
	case autoCommit && withHeaders:
		handlers.AutoCommitWithHeaders = func(payload []byte, _ string, headers [][]byte, _ ...any) {
			lease.hmessages = append(lease.hmessages, &pb.HFetchMessage{
				Payload: payload,
				Headers: connectorHeadersToProto(headers),
			})
		}
	case autoCommit:
		handlers.AutoCommit = func(payload []byte, _ string, _ ...any) {
			lease.messages = append(lease.messages, &pb.FetchMessage{Payload: payload})
		}
	default:
		handlers.Manual = func(_ byte, reader connector.Reader, payload []byte, source string, headers [][]byte, args ...any) {
			messageID := encodeMessageID(reader, false, source, args...)
			if withHeaders {
				lease.hmessages = append(lease.hmessages, &pb.HFetchMessage{
					MessageId: messageID,
					Payload:   payload,
					Headers:   connectorHeadersToProto(headers),
				})
			} else {
				lease.messages = append(lease.messages, &pb.FetchMessage{MessageId: messageID, Payload: payload})
			}
		}
	}

	subscriptionID, _, fetchErr := s.core.Fetch(route, autoCommit, withHeaders, batchSize, handlers)
	if withHeaders {
		response := &pb.HFetchResponse{
			CorrelationId:  correlationID,
			Error:          grpcOperationError(fetchErr),
			SubscriptionId: uint32(subscriptionID),
			Messages:       lease.hmessages,
		}
		sendErr := s.sendResponse(&pb.FujinResponse{Response: &pb.FujinResponse_Hfetch{Hfetch: response}})
		putGRPCFetchLease(lease)
		if sendErr != nil {
			s.l.Error("send hfetch response", "err", sendErr)
		}
		return nil
	}
	response := &pb.FetchResponse{
		CorrelationId:  correlationID,
		Error:          grpcOperationError(fetchErr),
		SubscriptionId: uint32(subscriptionID),
		Messages:       lease.messages,
	}
	sendErr := s.sendResponse(&pb.FujinResponse{Response: &pb.FujinResponse_Fetch{Fetch: response}})
	putGRPCFetchLease(lease)
	if sendErr != nil {
		s.l.Error("send fetch response", "err", sendErr)
	}
	return nil
}

func (s *streamSession) handleUnsubscribe(req *pb.UnsubscribeRequest) error {
	err := s.core.Unsubscribe(byte(req.SubscriptionId))
	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Unsubscribe{Unsubscribe: &pb.UnsubscribeResponse{
			CorrelationId: req.CorrelationId,
			Error:         grpcOperationError(err),
		}},
	})
}

func (s *streamSession) handleAck(req *pb.AckRequest) error {
	remaining := len(req.MessageIds)
	results := make([]*pb.AckMessageResult, 0, remaining)
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Ack{Ack: &pb.AckResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
				Results:       results,
			}},
		}); sendErr != nil {
			s.l.Error("send ack response", "err", sendErr)
		}
	}
	handlers := core.AckResultHandlers{
		Result: func(err error) {
			if err != nil || remaining == 0 {
				respond(err)
			}
		},
		Message: func(messageID []byte, err error) {
			results = append(results, &pb.AckMessageResult{MessageId: messageID, Error: grpcOperationError(err)})
			remaining--
			if remaining == 0 {
				respond(nil)
			}
		},
	}
	if err := s.core.Ack(byte(req.SubscriptionId), req.MessageIds, handlers); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) handleNack(req *pb.NackRequest) error {
	remaining := len(req.MessageIds)
	results := make([]*pb.NackMessageResult, 0, remaining)
	respond := func(err error) {
		if sendErr := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Nack{Nack: &pb.NackResponse{
				CorrelationId: req.CorrelationId,
				Error:         grpcOperationError(err),
				Results:       results,
			}},
		}); sendErr != nil {
			s.l.Error("send nack response", "err", sendErr)
		}
	}
	handlers := core.AckResultHandlers{
		Result: func(err error) {
			if err != nil || remaining == 0 {
				respond(err)
			}
		},
		Message: func(messageID []byte, err error) {
			results = append(results, &pb.NackMessageResult{MessageId: messageID, Error: grpcOperationError(err)})
			remaining--
			if remaining == 0 {
				respond(nil)
			}
		},
	}
	if err := s.core.Nack(byte(req.SubscriptionId), req.MessageIds, handlers); err != nil {
		respond(err)
	}
	return nil
}

func (s *streamSession) sendResponse(resp *pb.FujinResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.stream.Send(resp); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	return nil
}

func protoHeadersToConnector(headers []*pb.KV) [][]byte {
	if len(headers) == 0 {
		return [][]byte{}
	}
	result := make([][]byte, 0, len(headers)*2)
	for _, header := range headers {
		result = append(result, header.Key, header.Value)
	}
	return result
}

func connectorHeadersToProto(headers [][]byte) []*pb.KV {
	if len(headers) == 0 {
		return nil
	}
	result := make([]*pb.KV, 0, (len(headers)+1)/2)
	for i := 0; i < len(headers); i += 2 {
		var value []byte
		if i+1 < len(headers) {
			value = headers[i+1]
		}
		result = append(result, &pb.KV{Key: headers[i], Value: value})
	}
	return result
}

func encodeMessageID(reader connector.Reader, autoCommit bool, source string, args ...any) []byte {
	if autoCommit {
		return nil
	}
	buf := make([]byte, 0, len(source)+reader.MsgIDArgsLen())
	return reader.EncodeMsgID(buf, source, args...)
}

func grpcOperationError(err error) *pb.OperationError {
	if err == nil {
		return nil
	}
	operationErr := core.ClassifyError(err)
	return &pb.OperationError{
		Code:    pb.StatusCode(operationErr.Code),
		Outcome: pb.OperationOutcome(operationErr.Outcome),
		Reason:  operationErr.Reason,
		Message: operationErr.Message,
		Details: operationErr.Details,
	}
}
