package server

import (
	"context"
	"log/slog"
	"sync"

	fujinv1 "github.com/ValerySidorin/fujin/internal/api/grpc/v1"
	iconn "github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

// GRPCServer implements the FujinService gRPC interface
type GRPCServer struct {
	fujinv1.UnimplementedFujinServiceServer

	cman *iconn.Manager
	l    *slog.Logger
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(cman *iconn.Manager, l *slog.Logger) *GRPCServer {
	return &GRPCServer{
		cman: cman,
		l:    l,
	}
}

// Stream handles bidirectional streaming for all Fujin operations
func (s *GRPCServer) Stream(stream fujinv1.FujinService_StreamServer) error {
	ctx := stream.Context()

	// Create a session handler similar to native protocol
	session := newGRPCSession(ctx, s.cman, s.l)
	defer session.close()

	// Start response sender goroutine
	responseCh := make(chan *fujinv1.FujinResponse, 100)
	defer close(responseCh)

	var wg sync.WaitGroup
	wg.Go(func() {
		for resp := range responseCh {
			if err := stream.Send(resp); err != nil {
				s.l.Error("failed to send response", "error", err)
				return
			}
		}
	})

	// Process incoming requests
	for {
		req, err := stream.Recv()
		if err != nil {
			s.l.Error("failed to receive request", "error", err)
			break
		}

		// Handle request asynchronously
		go func(request *fujinv1.FujinRequest) {
			session.handleRequest(request, responseCh)
		}(req)
	}

	// Wait for response sender to finish
	wg.Wait()
	return nil
}

// GRPCSession represents a gRPC session, similar to native protocol handler
type GRPCSession struct {
	ctx  context.Context
	cman *iconn.Manager
	l    *slog.Logger

	// Stream ID (transactional ID for Kafka)
	streamID  string
	connected bool

	// Writers for different topics (similar to native protocol)
	writers map[string]writer.Writer
	wMu     sync.RWMutex

	// Subscribers (similar to native protocol)
	subscribers map[uint32]reader.Reader
	subMu       sync.RWMutex
}

func newGRPCSession(ctx context.Context, cman *iconn.Manager, l *slog.Logger) *GRPCSession {
	return &GRPCSession{
		ctx:         ctx,
		cman:        cman,
		l:           l,
		writers:     make(map[string]writer.Writer),
		subscribers: make(map[uint32]reader.Reader),
	}
}

func (s *GRPCSession) close() {
	s.wMu.Lock()
	defer s.wMu.Unlock()

	for topic, w := range s.writers {
		s.cman.PutWriter(w, topic, s.streamID)
	}
	s.writers = nil

	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.subscribers = nil
}

func (s *GRPCSession) handleRequest(req *fujinv1.FujinRequest, responseCh chan<- *fujinv1.FujinResponse) {
	switch r := req.Request.(type) {
	case *fujinv1.FujinRequest_Connect:
		s.handleConnect(r.Connect, responseCh)
	case *fujinv1.FujinRequest_Produce:
		s.handleProduce(r.Produce, responseCh)
	case *fujinv1.FujinRequest_Subscribe:
		s.handleSubscribe(r.Subscribe, responseCh)
	case *fujinv1.FujinRequest_Ack:
		s.handleAck(r.Ack, responseCh)
	case *fujinv1.FujinRequest_Nack:
		s.handleNack(r.Nack, responseCh)
	default:
		s.l.Error("unknown request type")
	}
}

func (s *GRPCSession) handleConnect(req *fujinv1.ConnectRequest, responseCh chan<- *fujinv1.FujinResponse) {
	if s.connected {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Connect{
				Connect: &fujinv1.ConnectResponse{
					CorrelationId: req.CorrelationId,
					Error:         "already connected",
				},
			},
		}
		return
	}

	// Set stream ID and mark as connected
	s.streamID = req.StreamId
	s.connected = true

	s.l.Info("gRPC stream connected", "stream_id", s.streamID)

	responseCh <- &fujinv1.FujinResponse{
		Response: &fujinv1.FujinResponse_Connect{
			Connect: &fujinv1.ConnectResponse{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	}
}

func (s *GRPCSession) handleProduce(req *fujinv1.ProduceRequest, responseCh chan<- *fujinv1.FujinResponse) {
	if !s.connected {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Produce{
				Produce: &fujinv1.ProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         "not connected, send Connect request first",
				},
			},
		}
		return
	}

	if req.Topic == "" {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Produce{
				Produce: &fujinv1.ProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         "topic is required",
				},
			},
		}
		return
	}

	// Get or create writer for topic
	writer := s.getWriter(req.Topic)
	if writer == nil {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Produce{
				Produce: &fujinv1.ProduceResponse{
					CorrelationId: req.CorrelationId,
					Error:         "failed to get writer for topic: " + req.Topic,
				},
			},
		}
		return
	}

	// Convert headers from proto to [][]byte
	var headers [][]byte
	if len(req.Headers) > 0 {
		headers = make([][]byte, 0, len(req.Headers)*2)
		for _, h := range req.Headers {
			headers = append(headers, h.Key, h.Value)
		}
	}

	// Produce message asynchronously
	if len(headers) > 0 {
		writer.HProduce(s.ctx, req.Message, headers, func(err error) {
			if err != nil {
				s.l.Error("failed to produce message with headers", "error", err, "topic", req.Topic, "stream_id", s.streamID)
				responseCh <- &fujinv1.FujinResponse{
					Response: &fujinv1.FujinResponse_Produce{
						Produce: &fujinv1.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "failed to produce message: " + err.Error(),
						},
					},
				}
			} else {
				responseCh <- &fujinv1.FujinResponse{
					Response: &fujinv1.FujinResponse_Produce{
						Produce: &fujinv1.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "",
						},
					},
				}
			}
		})
	} else {
		writer.Produce(s.ctx, req.Message, func(err error) {
			if err != nil {
				s.l.Error("failed to produce message", "error", err, "topic", req.Topic, "stream_id", s.streamID)
				responseCh <- &fujinv1.FujinResponse{
					Response: &fujinv1.FujinResponse_Produce{
						Produce: &fujinv1.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "failed to produce message: " + err.Error(),
						},
					},
				}
			} else {
				responseCh <- &fujinv1.FujinResponse{
					Response: &fujinv1.FujinResponse_Produce{
						Produce: &fujinv1.ProduceResponse{
							CorrelationId: req.CorrelationId,
							Error:         "",
						},
					},
				}
			}
		})
	}
}

func (s *GRPCSession) handleSubscribe(req *fujinv1.SubscribeRequest, responseCh chan<- *fujinv1.FujinResponse) {
	if !s.connected {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Ack{
				Ack: &fujinv1.Empty{
					CorrelationId: req.CorrelationId,
					Error:         "not connected, send Connect request first",
				},
			},
		}
		return
	}

	// TODO: Implement subscribe logic similar to native protocol
	s.l.Info("subscribe request received", "topic", req.Topic, "correlation_id", req.CorrelationId, "stream_id", s.streamID)

	// For now, just acknowledge the request
	responseCh <- &fujinv1.FujinResponse{
		Response: &fujinv1.FujinResponse_Ack{
			Ack: &fujinv1.Empty{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	}
}

func (s *GRPCSession) handleAck(req *fujinv1.AckRequest, responseCh chan<- *fujinv1.FujinResponse) {
	if !s.connected {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Ack{
				Ack: &fujinv1.Empty{
					CorrelationId: req.CorrelationId,
					Error:         "not connected, send Connect request first",
				},
			},
		}
		return
	}

	// TODO: Implement ack logic
	s.l.Info("ack request received", "correlation_id", req.CorrelationId, "stream_id", s.streamID)

	responseCh <- &fujinv1.FujinResponse{
		Response: &fujinv1.FujinResponse_Ack{
			Ack: &fujinv1.Empty{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	}
}

func (s *GRPCSession) handleNack(req *fujinv1.NackRequest, responseCh chan<- *fujinv1.FujinResponse) {
	if !s.connected {
		responseCh <- &fujinv1.FujinResponse{
			Response: &fujinv1.FujinResponse_Nack{
				Nack: &fujinv1.Empty{
					CorrelationId: req.CorrelationId,
					Error:         "not connected, send Connect request first",
				},
			},
		}
		return
	}

	// TODO: Implement nack logic
	s.l.Info("nack request received", "correlation_id", req.CorrelationId, "stream_id", s.streamID)

	responseCh <- &fujinv1.FujinResponse{
		Response: &fujinv1.FujinResponse_Nack{
			Nack: &fujinv1.Empty{
				CorrelationId: req.CorrelationId,
				Error:         "",
			},
		},
	}
}

func (s *GRPCSession) getWriter(topic string) writer.Writer {
	s.wMu.RLock()
	if w, exists := s.writers[topic]; exists {
		s.wMu.RUnlock()
		return w
	}
	s.wMu.RUnlock()

	s.wMu.Lock()
	defer s.wMu.Unlock()

	// Double-check after acquiring write lock
	if w, exists := s.writers[topic]; exists {
		return w
	}

	// Create new writer using streamID as writerID (for Kafka transactional ID)
	w, err := s.cman.GetWriter(topic, s.streamID)
	if err != nil {
		s.l.Error("failed to get writer", "error", err, "topic", topic, "stream_id", s.streamID)
		return nil
	}

	s.writers[topic] = w
	return w
}
