package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/ValerySidorin/fujin/internal/connectors"
	internal_reader "github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
	pb "github.com/ValerySidorin/fujin/public/grpc/v1"
	"github.com/ValerySidorin/fujin/public/server/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer implements the Fujin gRPC service
type GRPCServer struct {
	pb.UnimplementedFujinServiceServer

	conf config.GRPCServerConfig
	cman *connectors.Manager
	l    *slog.Logger

	grpcServer *grpc.Server
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(conf config.GRPCServerConfig, cman *connectors.Manager, l *slog.Logger) *GRPCServer {
	return &GRPCServer{
		conf: conf,
		cman: cman,
		l:    l.With("server", "grpc"),
	}
}

// ListenAndServe starts the gRPC server
func (s *GRPCServer) ListenAndServe(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.conf.Addr, err)
	}

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
	s.l.Info("grpc server started", "addr", s.conf.Addr)
	errCh := make(chan error, 1)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.l.Info("shutting down grpc server")
		s.grpcServer.GracefulStop()
		s.l.Info("grpc server stopped")
		return nil
	case err := <-errCh:
		return err
	}
}

// Stop gracefully stops the gRPC server
func (s *GRPCServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// Stream implements the bidirectional streaming RPC
func (s *GRPCServer) Stream(stream pb.FujinService_StreamServer) error {
	ctx := stream.Context()

	session := &streamSession{
		stream:    stream,
		cman:      s.cman,
		l:         s.l,
		ctx:       ctx,
		writers:   make(map[string]writer.Writer),
		readers:   make(map[byte]*readerState),
		nextSubID: 0,
		connected: false,
	}

	// Start goroutines for receiving and sending
	errCh := make(chan error, 2)

	// Receiver goroutine
	go func() {
		errCh <- session.receiveLoop()
	}()

	// Wait for either error or context cancellation
	select {
	case <-ctx.Done():
		session.cleanup()
		return ctx.Err()
	case err := <-errCh:
		session.cleanup()
		if err == io.EOF {
			return nil
		}
		return err
	}
}

// streamSession represents a single bidirectional stream session
type streamSession struct {
	stream pb.FujinService_StreamServer
	cman   *connectors.Manager
	l      *slog.Logger
	ctx    context.Context

	mu        sync.RWMutex
	sendMu    sync.Mutex
	writers   map[string]writer.Writer
	readers   map[byte]*readerState
	nextSubID byte
	streamID  string
	connected bool
}

type readerState struct {
	reader     internal_reader.Reader
	topic      string
	autoCommit bool
	cancel     context.CancelFunc
}

// receiveLoop handles incoming requests from client
func (s *streamSession) receiveLoop() error {
	for {
		req, err := s.stream.Recv()
		if err != nil {
			if err == io.EOF {
				return io.EOF
			}
			return fmt.Errorf("receive error: %w", err)
		}

		if err := s.handleRequest(req); err != nil {
			s.l.Error("handle request", "err", err)
			return fmt.Errorf("handle request: %w", err)
		}
	}
}

// handleRequest processes a single request
func (s *streamSession) handleRequest(req *pb.FujinRequest) error {
	switch r := req.Request.(type) {
	case *pb.FujinRequest_Connect:
		return s.handleConnect(r.Connect)
	case *pb.FujinRequest_Produce:
		return s.handleProduce(r.Produce)
	case *pb.FujinRequest_Subscribe:
		return s.handleSubscribe(r.Subscribe)
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

// handleConnect processes CONNECT request
func (s *streamSession) handleConnect(req *pb.ConnectRequest) error {
	if s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Connect{
				Connect: &pb.ConnectResponse{
					CorrelationId: req.CorrelationId,
					Error:         "already connected",
				},
			},
		})
	}

	s.streamID = req.StreamId
	s.connected = true

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Connect{
			Connect: &pb.ConnectResponse{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	})
}

// handleProduce processes PRODUCE request
func (s *streamSession) handleProduce(req *pb.ProduceRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Produce{
				Produce: &pb.ProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	w, err := s.getWriter(req.Topic)
	if err != nil {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Produce{
				Produce: &pb.ProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         err.Error(),
				},
			},
		})
	}

	correlationID := req.CorrelationId

	w.Produce(
		s.ctx, req.Message,
		func(writeErr error) {
			var errMsg string
			if writeErr != nil {
				errMsg = writeErr.Error()
			}

			err := s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Produce{
					Produce: &pb.ProduceResponse{
						CorrelationId: correlationID,
						Error:         errMsg,
					},
				},
			})
			if err != nil {
				s.l.Error("send produce response", "err", err)
			}
		})

	return nil
}

// handleSubscribe processes SUBSCRIBE request
func (s *streamSession) handleSubscribe(req *pb.SubscribeRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Subscribe{
				Subscribe: &pb.SubscribeResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "not connected",
					SubscriptionId: 0,
				},
			},
		})
	}

	s.mu.Lock()
	if s.nextSubID == 255 {
		s.mu.Unlock()
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Subscribe{
				Subscribe: &pb.SubscribeResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "maximum subscriptions reached (255)",
					SubscriptionId: 0,
				},
			},
		})
	}
	s.nextSubID++
	subID := s.nextSubID
	s.mu.Unlock()

	r, err := s.cman.GetReader(req.Topic, req.AutoCommit)
	if err != nil {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Subscribe{
				Subscribe: &pb.SubscribeResponse{
					CorrelationId:  req.CorrelationId,
					Error:          err.Error(),
					SubscriptionId: 0,
				},
			},
		})
	}

	ctx, cancel := context.WithCancel(s.ctx)

	state := &readerState{
		reader:     r,
		topic:      req.Topic,
		autoCommit: req.AutoCommit,
		cancel:     cancel,
	}

	s.mu.Lock()
	s.readers[subID] = state
	s.mu.Unlock()

	if err := s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Subscribe{
			Subscribe: &pb.SubscribeResponse{
				CorrelationId:  req.CorrelationId,
				Error:          "",
				SubscriptionId: uint32(subID),
			},
		},
	}); err != nil {
		return err
	}

	go s.subscribeLoop(ctx, subID, state)

	return nil
}

// handleUnsubscribe processes UNSUBSCRIBE request
func (s *streamSession) handleUnsubscribe(req *pb.UnsubscribeRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Unsubscribe{
				Unsubscribe: &pb.UnsubscribeResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	subID := byte(req.SubscriptionId)

	s.mu.Lock()
	state, exists := s.readers[subID]
	if !exists {
		s.mu.Unlock()
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Unsubscribe{
				Unsubscribe: &pb.UnsubscribeResponse{
					CorrelationId: req.CorrelationId,
					Error:         "subscription not found",
				},
			},
		})
	}

	state.cancel()
	delete(s.readers, subID)
	s.mu.Unlock()

	state.reader.Close()

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Unsubscribe{
			Unsubscribe: &pb.UnsubscribeResponse{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	})
}

// subscribeLoop continuously reads messages and sends them to client
func (s *streamSession) subscribeLoop(ctx context.Context, subID byte, state *readerState) {
	defer func() {
		s.mu.Lock()
		delete(s.readers, subID)
		s.mu.Unlock()
		state.reader.Close()
	}()

	var msgIDBuf []byte
	if !state.autoCommit {
		msgIDBuf = make([]byte, state.reader.MsgIDStaticArgsLen())
	}

	msgHandler := func(message []byte, topic string, args ...any) {
		var msgID []byte
		if !state.autoCommit && len(args) > 0 {
			msgID = state.reader.EncodeMsgID(msgIDBuf, topic, args...)
		}

		if err := s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Message{
				Message: &pb.Message{
					SubscriptionId: uint32(subID),
					MessageId:      msgID,
					Payload:        message,
				},
			},
		}); err != nil {
			s.l.Error("failed to send message", "sub_id", subID, "err", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := state.reader.Subscribe(ctx, msgHandler)
			if err != nil && ctx.Err() == nil {
				s.l.Error("subscribe error", "sub_id", subID, "err", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// handleAck processes ACK request
func (s *streamSession) handleAck(req *pb.AckRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Ack{
				Ack: &pb.AckResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	// Convert subscription_id to byte (as per native protocol)
	subID := byte(req.SubscriptionId)

	s.mu.RLock()
	state, exists := s.readers[subID]
	s.mu.RUnlock()

	if !exists {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Ack{
				Ack: &pb.AckResponse{
					CorrelationId: req.CorrelationId,
					Error:         fmt.Sprintf("subscription %d not found", subID),
				},
			},
		})
	}

	// Use message IDs from the request (now supports multiple IDs)
	msgIDs := req.MessageIds

	// Collect per-message results
	results := make([]*pb.AckMessageResult, 0, len(msgIDs))
	var resultsMu sync.Mutex

	// Call Ack on the reader
	state.reader.Ack(
		s.ctx,
		msgIDs,
		func(err error) {
			// Ack handler - called when all acks are processed
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			if sendErr := s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Ack{
					Ack: &pb.AckResponse{
						CorrelationId: req.CorrelationId,
						Error:         errMsg,
						Results:       results,
					},
				},
			}); sendErr != nil {
				s.l.Error("failed to send ack response", "err", sendErr)
			}
		},
		func(msgID []byte, err error) {
			// Individual message ack handler
			var errMsg string
			if err != nil {
				errMsg = err.Error()
				s.l.Error("ack message", "msgID", msgID, "err", err)
			}

			resultsMu.Lock()
			results = append(results, &pb.AckMessageResult{
				MessageId: msgID,
				Error:     errMsg,
			})
			resultsMu.Unlock()
		},
	)

	return nil
}

// handleNack processes NACK request
func (s *streamSession) handleNack(req *pb.NackRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Nack{
				Nack: &pb.NackResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	// Convert subscription_id to byte (as per native protocol)
	subID := byte(req.SubscriptionId)

	s.mu.RLock()
	state, exists := s.readers[subID]
	s.mu.RUnlock()

	if !exists {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Nack{
				Nack: &pb.NackResponse{
					CorrelationId: req.CorrelationId,
					Error:         fmt.Sprintf("subscription %d not found", subID),
				},
			},
		})
	}

	// Use message IDs from the request (now supports multiple IDs)
	msgIDs := req.MessageIds

	// Collect per-message results
	results := make([]*pb.NackMessageResult, 0, len(msgIDs))
	var resultsMu sync.Mutex

	// Call Nack on the reader
	state.reader.Nack(
		s.ctx,
		msgIDs,
		func(err error) {
			// Nack handler - called when all nacks are processed
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			if sendErr := s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Nack{
					Nack: &pb.NackResponse{
						CorrelationId: req.CorrelationId,
						Error:         errMsg,
						Results:       results,
					},
				},
			}); sendErr != nil {
				s.l.Error("failed to send nack response", "err", sendErr)
			}
		},
		func(msgID []byte, err error) {
			// Individual message nack handler
			var errMsg string
			if err != nil {
				errMsg = err.Error()
				s.l.Error("nack message", "msgID", msgID, "err", err)
			}

			resultsMu.Lock()
			results = append(results, &pb.NackMessageResult{
				MessageId: msgID,
				Error:     errMsg,
			})
			resultsMu.Unlock()
		},
	)

	return nil
}

// getWriter retrieves or creates a writer for the given topic
func (s *streamSession) getWriter(topic string) (writer.Writer, error) {
	s.mu.RLock()
	if w, exists := s.writers[topic]; exists {
		s.mu.RUnlock()
		return w, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.cman.GetWriter(topic, s.streamID)
	if err != nil {
		return nil, err
	}

	s.writers[topic] = w
	return w, nil
}

// sendResponse sends a response to the client
func (s *streamSession) sendResponse(resp *pb.FujinResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.stream.Send(resp); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	return nil
}

// cleanup releases all resources
func (s *streamSession) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all writers
	for topic, w := range s.writers {
		w.Flush(s.ctx)
		s.cman.PutWriter(w, topic, s.streamID)
	}
	s.writers = make(map[string]writer.Writer)

	// Cancel all readers
	for _, state := range s.readers {
		state.cancel()
		state.reader.Close()
	}
	s.readers = make(map[byte]*readerState)
}
