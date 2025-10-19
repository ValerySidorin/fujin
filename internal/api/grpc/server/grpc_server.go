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
		stream:       stream,
		cman:         s.cman,
		l:            s.l,
		ctx:          ctx,
		writers:      make(map[string]writer.Writer),
		readers:      make(map[byte]*readerState),
		fetchReaders: make(map[string]byte),
		nextSubID:    0,
		connected:    false,
	}

	errCh := make(chan error, 2)

	go func() {
		errCh <- session.receiveLoop()
	}()

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

	mu           sync.RWMutex
	sendMu       sync.Mutex
	writers      map[string]writer.Writer
	readers      map[byte]*readerState
	fetchReaders map[string]byte // topic -> subscription_id mapping for fetch implicit subscriptions
	nextSubID    byte
	streamID     string
	connected    bool

	// Transaction state
	inTx                 bool
	currentTxWriter      writer.Writer
	currentTxWriterTopic string // Original topic used to get the writer (for PutWriter)
}

type readerState struct {
	reader      internal_reader.Reader
	topic       string
	autoCommit  bool
	withHeaders bool
	cancel      context.CancelFunc
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
	case *pb.FujinRequest_Hproduce:
		return s.handleHProduce(r.Hproduce)
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

	// Use transaction writer if in transaction
	s.mu.Lock()
	inTx := s.inTx
	if inTx {
		// Get or create transactional writer
		if s.currentTxWriter == nil {
			w, err := s.cman.GetWriter(req.Topic, s.streamID)
			if err != nil {
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Produce{
						Produce: &pb.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         err.Error(),
						},
					},
				})
			}
			if err := w.BeginTx(s.ctx); err != nil {
				s.cman.PutWriter(w, req.Topic, s.streamID)
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Produce{
						Produce: &pb.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         err.Error(),
						},
					},
				})
			}
			s.currentTxWriter = w
			s.currentTxWriterTopic = req.Topic
		} else {
			// Check if the new topic has the same endpoint as current transactional writer
			if !s.cman.WriterMatchEndpoint(s.currentTxWriter, req.Topic) {
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Produce{
						Produce: &pb.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "transaction cannot span different broker endpoints",
						},
					},
				})
			}
		}
	}
	s.mu.Unlock()

	var w writer.Writer
	var err error
	if inTx {
		s.mu.RLock()
		w = s.currentTxWriter
		s.mu.RUnlock()
	} else {
		w, err = s.getWriter(req.Topic)
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

// handleHProduce processes HPRODUCE request (produce with headers)
func (s *streamSession) handleHProduce(req *pb.HProduceRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hproduce{
				Hproduce: &pb.HProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	// Use transaction writer if in transaction
	s.mu.Lock()
	inTx := s.inTx
	if inTx {
		// Get or create transactional writer
		if s.currentTxWriter == nil {
			w, err := s.cman.GetWriter(req.Topic, s.streamID)
			if err != nil {
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Hproduce{
						Hproduce: &pb.HProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         err.Error(),
						},
					},
				})
			}
			if err := w.BeginTx(s.ctx); err != nil {
				s.cman.PutWriter(w, req.Topic, s.streamID)
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Hproduce{
						Hproduce: &pb.HProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         err.Error(),
						},
					},
				})
			}
			s.currentTxWriter = w
			s.currentTxWriterTopic = req.Topic
		} else {
			// Check if the new topic has the same endpoint as current transactional writer
			if !s.cman.WriterMatchEndpoint(s.currentTxWriter, req.Topic) {
				s.mu.Unlock()
				return s.sendResponse(&pb.FujinResponse{
					Response: &pb.FujinResponse_Hproduce{
						Hproduce: &pb.HProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "transaction cannot span different broker endpoints",
						},
					},
				})
			}
		}
	}
	s.mu.Unlock()

	var w writer.Writer
	var err error
	if inTx {
		s.mu.RLock()
		w = s.currentTxWriter
		s.mu.RUnlock()
	} else {
		w, err = s.getWriter(req.Topic)
		if err != nil {
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hproduce{
					Hproduce: &pb.HProduceResponse{
						CorrelationId: req.CorrelationId,
						Error:         err.Error(),
					},
				},
			})
		}
	}

	// Convert proto headers to [][]byte format (alternating key, value, key, value, ...)
	var headersKV [][]byte
	for _, h := range req.Headers {
		headersKV = append(headersKV, h.Key, h.Value)
	}

	correlationID := req.CorrelationId

	w.HProduce(
		s.ctx, req.Message, headersKV,
		func(writeErr error) {
			var errMsg string
			if writeErr != nil {
				errMsg = writeErr.Error()
			}

			err := s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hproduce{
					Hproduce: &pb.HProduceResponse{
						CorrelationId: correlationID,
						Error:         errMsg,
					},
				},
			})
			if err != nil {
				s.l.Error("send hproduce response", "err", err)
			}
		})

	return nil
}

// handleBeginTx processes BEGIN TX request
func (s *streamSession) handleBeginTx(req *pb.BeginTxRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_BeginTx{
				BeginTx: &pb.BeginTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inTx {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_BeginTx{
				BeginTx: &pb.BeginTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "transaction already in progress",
				},
			},
		})
	}

	// Flush all non-transactional writers
	for topic, w := range s.writers {
		w.Flush(s.ctx)
		s.cman.PutWriter(w, topic, s.streamID)
	}
	s.writers = make(map[string]writer.Writer)

	s.inTx = true

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_BeginTx{
			BeginTx: &pb.BeginTxResponse{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	})
}

// handleCommitTx processes COMMIT TX request
func (s *streamSession) handleCommitTx(req *pb.CommitTxRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_CommitTx{
				CommitTx: &pb.CommitTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.inTx {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_CommitTx{
				CommitTx: &pb.CommitTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "no transaction in progress",
				},
			},
		})
	}

	var errMsg string
	if s.currentTxWriter != nil {
		if err := s.currentTxWriter.CommitTx(s.ctx); err != nil {
			errMsg = err.Error()
		}
		s.cman.PutWriter(s.currentTxWriter, s.currentTxWriterTopic, s.streamID)
		s.currentTxWriter = nil
		s.currentTxWriterTopic = ""
	}

	s.inTx = false

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_CommitTx{
			CommitTx: &pb.CommitTxResponse{
				CorrelationId: req.CorrelationId,
				Error:         errMsg,
			},
		},
	})
}

// handleRollbackTx processes ROLLBACK TX request
func (s *streamSession) handleRollbackTx(req *pb.RollbackTxRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_RollbackTx{
				RollbackTx: &pb.RollbackTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected",
				},
			},
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.inTx {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_RollbackTx{
				RollbackTx: &pb.RollbackTxResponse{
					CorrelationId: req.CorrelationId,
					Error:         "no transaction in progress",
				},
			},
		})
	}

	var errMsg string
	if s.currentTxWriter != nil {
		if err := s.currentTxWriter.RollbackTx(s.ctx); err != nil {
			errMsg = err.Error()
		}
		s.cman.PutWriter(s.currentTxWriter, s.currentTxWriterTopic, s.streamID)
		s.currentTxWriter = nil
		s.currentTxWriterTopic = ""
	}

	s.inTx = false

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_RollbackTx{
			RollbackTx: &pb.RollbackTxResponse{
				CorrelationId: req.CorrelationId,
				Error:         errMsg,
			},
		},
	})
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
		reader:      r,
		topic:       req.Topic,
		autoCommit:  req.AutoCommit,
		withHeaders: false,
		cancel:      cancel,
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

// handleHSubscribe processes HSUBSCRIBE request (subscribe with headers)
func (s *streamSession) handleHSubscribe(req *pb.HSubscribeRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hsubscribe{
				Hsubscribe: &pb.HSubscribeResponse{
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
			Response: &pb.FujinResponse_Hsubscribe{
				Hsubscribe: &pb.HSubscribeResponse{
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
			Response: &pb.FujinResponse_Hsubscribe{
				Hsubscribe: &pb.HSubscribeResponse{
					CorrelationId:  req.CorrelationId,
					Error:          err.Error(),
					SubscriptionId: 0,
				},
			},
		})
	}

	ctx, cancel := context.WithCancel(s.ctx)

	state := &readerState{
		reader:      r,
		topic:       req.Topic,
		autoCommit:  req.AutoCommit,
		withHeaders: true,
		cancel:      cancel,
	}

	s.mu.Lock()
	s.readers[subID] = state
	s.mu.Unlock()

	if err := s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Hsubscribe{
			Hsubscribe: &pb.HSubscribeResponse{
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

// handleFetch processes FETCH request (pull-based message retrieval)
func (s *streamSession) handleFetch(req *pb.FetchRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Fetch{
				Fetch: &pb.FetchResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "not connected",
					SubscriptionId: 0,
				},
			},
		})
	}

	s.mu.Lock()
	subID, exists := s.fetchReaders[req.Topic]

	if !exists {
		if s.nextSubID == 255 {
			s.mu.Unlock()
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Fetch{
					Fetch: &pb.FetchResponse{
						CorrelationId:  req.CorrelationId,
						Error:          "maximum subscriptions reached (255)",
						SubscriptionId: 0,
					},
				},
			})
		}
		s.nextSubID++
		subID = s.nextSubID

		r, err := s.cman.GetReader(req.Topic, req.AutoCommit)
		if err != nil {
			s.mu.Unlock()
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Fetch{
					Fetch: &pb.FetchResponse{
						CorrelationId:  req.CorrelationId,
						Error:          err.Error(),
						SubscriptionId: 0,
					},
				},
			})
		}

		state := &readerState{
			reader:      r,
			topic:       req.Topic,
			autoCommit:  req.AutoCommit,
			withHeaders: false,
		}
		s.readers[subID] = state
		s.fetchReaders[req.Topic] = subID
	}
	s.mu.Unlock()

	s.mu.RLock()
	state, exists := s.readers[subID]
	s.mu.RUnlock()

	if !exists {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Fetch{
				Fetch: &pb.FetchResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "reader not found",
					SubscriptionId: uint32(subID),
				},
			},
		})
	}

	var messages []*pb.FetchMessage
	var msgIDBuf []byte
	if !req.AutoCommit {
		msgIDBuf = make([]byte, state.reader.MsgIDStaticArgsLen())
	}

	fetchErr := make(chan error, 1)

	msgHandler := func(message []byte, topic string, args ...any) {
		var msgID []byte
		if !req.AutoCommit && len(args) > 0 {
			msgID = state.reader.EncodeMsgID(msgIDBuf, topic, args...)
		}

		messages = append(messages, &pb.FetchMessage{
			MessageId: msgID,
			Payload:   message,
		})
	}

	fetchResponseHandler := func(n uint32, err error) {
		fetchErr <- err
	}

	state.reader.Fetch(s.ctx, req.BatchSize, fetchResponseHandler, msgHandler)
	err := <-fetchErr

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Fetch{
			Fetch: &pb.FetchResponse{
				CorrelationId:  req.CorrelationId,
				Error:          errMsg,
				SubscriptionId: uint32(subID),
				Messages:       messages,
			},
		},
	})
}

// handleHFetch processes HFETCH request (pull-based message retrieval with headers)
func (s *streamSession) handleHFetch(req *pb.HFetchRequest) error {
	if !s.connected {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hfetch{
				Hfetch: &pb.HFetchResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "not connected",
					SubscriptionId: 0,
				},
			},
		})
	}

	s.mu.Lock()
	subID, exists := s.fetchReaders[req.Topic]

	if !exists {
		if s.nextSubID == 255 {
			s.mu.Unlock()
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hfetch{
					Hfetch: &pb.HFetchResponse{
						CorrelationId:  req.CorrelationId,
						Error:          "maximum subscriptions reached (255)",
						SubscriptionId: 0,
					},
				},
			})
		}
		s.nextSubID++
		subID = s.nextSubID

		r, err := s.cman.GetReader(req.Topic, req.AutoCommit)
		if err != nil {
			s.mu.Unlock()
			return s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hfetch{
					Hfetch: &pb.HFetchResponse{
						CorrelationId:  req.CorrelationId,
						Error:          err.Error(),
						SubscriptionId: 0,
					},
				},
			})
		}

		state := &readerState{
			reader:      r,
			topic:       req.Topic,
			autoCommit:  req.AutoCommit,
			withHeaders: true,
		}
		s.readers[subID] = state
		s.fetchReaders[req.Topic] = subID
	}
	s.mu.Unlock()

	// Get reader
	s.mu.RLock()
	state, exists := s.readers[subID]
	s.mu.RUnlock()

	if !exists {
		return s.sendResponse(&pb.FujinResponse{
			Response: &pb.FujinResponse_Hfetch{
				Hfetch: &pb.HFetchResponse{
					CorrelationId:  req.CorrelationId,
					Error:          "reader not found",
					SubscriptionId: uint32(subID),
				},
			},
		})
	}

	var messages []*pb.HFetchMessage
	var msgIDBuf []byte
	if !req.AutoCommit {
		msgIDBuf = make([]byte, state.reader.MsgIDStaticArgsLen())
	}

	fetchErr := make(chan error, 1)

	hmsgHandler := func(message []byte, topic string, hs [][]byte, args ...any) {
		var msgID []byte
		if !req.AutoCommit && len(args) > 0 {
			msgID = state.reader.EncodeMsgID(msgIDBuf, topic, args...)
		}

		// Convert headers from [][]byte to []*pb.Header
		var protoHeaders []*pb.Header
		for i := 0; i < len(hs); i += 2 {
			key := hs[i]
			var val []byte
			if i+1 < len(hs) {
				val = hs[i+1]
			}
			protoHeaders = append(protoHeaders, &pb.Header{
				Key:   key,
				Value: val,
			})
		}

		messages = append(messages, &pb.HFetchMessage{
			MessageId: msgID,
			Payload:   message,
			Headers:   protoHeaders,
		})
	}

	fetchResponseHandler := func(n uint32, err error) {
		fetchErr <- err
	}

	state.reader.HFetch(s.ctx, req.BatchSize, fetchResponseHandler, hmsgHandler)
	err := <-fetchErr

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	return s.sendResponse(&pb.FujinResponse{
		Response: &pb.FujinResponse_Hfetch{
			Hfetch: &pb.HFetchResponse{
				CorrelationId:  req.CorrelationId,
				Error:          errMsg,
				SubscriptionId: uint32(subID),
				Messages:       messages,
			},
		},
	})
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

	// Remove from fetchReaders if this is a fetch subscription
	topic := state.topic
	if fetchSubID, isFetch := s.fetchReaders[topic]; isFetch && fetchSubID == subID {
		delete(s.fetchReaders, topic)
	}

	if state.cancel != nil {
		state.cancel()
	}
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

	if state.withHeaders {
		hmsgHandler := func(message []byte, topic string, hs [][]byte, args ...any) {
			var msgID []byte
			if !state.autoCommit && len(args) > 0 {
				msgID = state.reader.EncodeMsgID(msgIDBuf, topic, args...)
			}

			var protoHeaders []*pb.Header
			for i := 0; i < len(hs); i += 2 {
				key := hs[i]
				var val []byte
				if i+1 < len(hs) {
					val = hs[i+1]
				}
				protoHeaders = append(protoHeaders, &pb.Header{
					Key:   key,
					Value: val,
				})
			}

			if err := s.sendResponse(&pb.FujinResponse{
				Response: &pb.FujinResponse_Hmessage{
					Hmessage: &pb.HMessage{
						SubscriptionId: uint32(subID),
						MessageId:      msgID,
						Payload:        message,
						Headers:        protoHeaders,
					},
				},
			}); err != nil {
				s.l.Error("failed to send hmessage", "sub_id", subID, "err", err)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := state.reader.HSubscribe(ctx, hmsgHandler)
				if err != nil && ctx.Err() == nil {
					s.l.Error("hsubscribe error", "sub_id", subID, "err", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	} else {
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

	msgIDs := req.MessageIds

	results := make([]*pb.AckMessageResult, 0, len(msgIDs))
	var resultsMu sync.Mutex

	state.reader.Ack(
		s.ctx,
		msgIDs,
		func(err error) {
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

	msgIDs := req.MessageIds

	results := make([]*pb.NackMessageResult, 0, len(msgIDs))
	var resultsMu sync.Mutex

	state.reader.Nack(
		s.ctx,
		msgIDs,
		func(err error) {
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
		if state.cancel != nil {
			state.cancel()
		}
		state.reader.Close()
	}
	s.readers = make(map[byte]*readerState)

	// Clear fetch readers mapping
	s.fetchReaders = make(map[string]byte)
}
