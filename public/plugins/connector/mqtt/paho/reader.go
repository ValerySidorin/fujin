package paho

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

var ErrMsgNotFound = errors.New("msg not found")

// pendingMsg stores a message awaiting acknowledgment with TTL tracking
type pendingMsg struct {
	publish   *paho.Publish
	client    *paho.Client
	createdAt time.Time
}

// Reader implements connector.ReadCloser for MQTT using paho.golang
type Reader struct {
	conf    ConnectorConfig
	cm      *autopaho.ConnectionManager
	autoAck bool
	l       *slog.Logger

	// pending messages for manual ack
	msgs   map[uint16]pendingMsg
	msgsMu sync.Mutex

	// TTL cleanup
	cleanupStop chan struct{}
	cleanupDone chan struct{}

	// subscription handler
	msgHandler   func([]byte, string, ...any)
	hMsgHandler  func([]byte, string, [][]byte, ...any)
	handlerMu    sync.RWMutex
	isHSubscribe bool
}

// NewReader creates a new MQTT reader using paho.golang
func NewReader(conf ConnectorConfig, autoAck bool, l *slog.Logger) (connector.ReadCloser, error) {
	if err := conf.ValidateReader(); err != nil {
		return nil, fmt.Errorf("mqtt_paho: validate config: %w", err)
	}

	serverURL, err := url.Parse(conf.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("mqtt_paho: parse broker url: %w", err)
	}

	r := &Reader{
		conf:        conf,
		autoAck:     autoAck,
		l:           l.With("reader_type", "mqtt_paho"),
		msgs:        make(map[uint16]pendingMsg),
		cleanupStop: make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     conf.KeepAlive,
		CleanStartOnInitialConnection: conf.CleanStart,
		SessionExpiryInterval:         conf.SessionExpiry,
		ConnectTimeout:                conf.ConnectTimeout,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			r.l.Info("mqtt connection up", "session_present", connAck.SessionPresent)
		},
		OnConnectError: func(err error) {
			r.l.Error("mqtt connection error", "err", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: conf.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				r.handlePublish,
			},
			EnableManualAcknowledgment: !autoAck,
			SendAcksInterval:           conf.SendAcksInterval,
		},
	}

	ctx := context.Background()
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, fmt.Errorf("mqtt_paho: new connection: %w", err)
	}

	// Wait for connection
	if err := cm.AwaitConnection(ctx); err != nil {
		return nil, fmt.Errorf("mqtt_paho: await connection: %w", err)
	}

	r.cm = cm

	// Start TTL cleanup goroutine if manual ack is enabled
	if !autoAck {
		go r.cleanupExpiredMessages()
	}

	return r, nil
}

// handlePublish processes incoming messages
func (r *Reader) handlePublish(pr paho.PublishReceived) (bool, error) {
	r.handlerMu.RLock()
	msgHandler := r.msgHandler
	hMsgHandler := r.hMsgHandler
	isHSubscribe := r.isHSubscribe
	r.handlerMu.RUnlock()

	if msgHandler == nil && hMsgHandler == nil {
		return false, nil
	}

	pb := pr.Packet

	if r.autoAck {
		if isHSubscribe {
			headers := extractHeaders(pb)
			hMsgHandler(pb.Payload, pb.Topic, headers)
		} else {
			msgHandler(pb.Payload, pb.Topic)
		}
	} else {
		// Store for manual ack - include client reference for later ack
		r.msgsMu.Lock()
		r.msgs[pb.PacketID] = pendingMsg{
			publish:   pb,
			client:    pr.Client,
			createdAt: time.Now(),
		}
		r.msgsMu.Unlock()

		if isHSubscribe {
			headers := extractHeaders(pb)
			hMsgHandler(pb.Payload, pb.Topic, headers, pb.PacketID)
		} else {
			msgHandler(pb.Payload, pb.Topic, pb.PacketID)
		}
	}

	return true, nil
}

// extractHeaders extracts user properties as headers
func extractHeaders(pb *paho.Publish) [][]byte {
	if pb.Properties == nil || len(pb.Properties.User) == 0 {
		return nil
	}
	var hs [][]byte
	for _, prop := range pb.Properties.User {
		hs = append(hs, []byte(prop.Key), []byte(prop.Value))
	}
	return hs
}

// cleanupExpiredMessages periodically removes messages that exceed TTL
func (r *Reader) cleanupExpiredMessages() {
	defer close(r.cleanupDone)

	ticker := time.NewTicker(r.conf.AckTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-r.cleanupStop:
			return
		case <-ticker.C:
			r.msgsMu.Lock()
			now := time.Now()
			for id, msg := range r.msgs {
				if now.Sub(msg.createdAt) > r.conf.AckTTL {
					r.l.Warn("mqtt_paho: message TTL expired, removing from pending", "packet_id", id)
					delete(r.msgs, id)
				}
			}
			r.msgsMu.Unlock()
		}
	}
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	r.handlerMu.Lock()
	r.msgHandler = h
	r.isHSubscribe = false
	r.handlerMu.Unlock()

	_, err := r.cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: r.conf.Topic,
				QoS:   r.conf.QoS,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("mqtt_paho: subscribe: %w", err)
	}
	if err := ready(); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	r.handlerMu.Lock()
	r.hMsgHandler = h
	r.isHSubscribe = true
	r.handlerMu.Unlock()

	_, err := r.cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: r.conf.Topic,
				QoS:   r.conf.QoS,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("mqtt_paho: subscribe: %w", err)
	}
	if err := ready(); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	_ context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	if r.autoAck {
		for _, id := range msgIDs {
			ackMsgHandler(id, connector.ErrOperationUnsupported)
		}
		return
	}

	for _, id := range msgIDs {
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			ackMsgHandler(id, fmt.Errorf("mqtt: ack: %w", err))
			continue
		}
		packetID := binary.BigEndian.Uint16(id)

		r.msgsMu.Lock()
		msg, ok := r.msgs[packetID]
		if ok {
			delete(r.msgs, packetID)
		}
		r.msgsMu.Unlock()

		if !ok {
			ackMsgHandler(id, ErrMsgNotFound)
			continue
		}

		// Ack through the client that received this message
		if err := msg.client.Ack(msg.publish); err != nil {
			ackMsgHandler(id, err)
			continue
		}
		ackMsgHandler(id, nil)
	}
}

func (r *Reader) Nack(
	_ context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// MQTT has no explicit negative acknowledgement operation.
	nackHandler(nil)
	for _, id := range msgIDs {
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			nackMsgHandler(id, fmt.Errorf("mqtt: nack: %w", err))
			continue
		}
		nackMsgHandler(id, connector.ErrOperationUnsupported)
	}
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return binary.BigEndian.AppendUint16(buf, args[0].(uint16))
}

func (r *Reader) MsgIDArgsLen() int {
	return 2
}

func (r *Reader) AutoCommit() bool {
	return r.autoAck
}

func (r *Reader) Close() error {
	// Stop TTL cleanup
	if !r.autoAck {
		close(r.cleanupStop)
		<-r.cleanupDone
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.conf.DisconnectTimeout)
	defer cancel()

	return r.cm.Disconnect(ctx)
}
