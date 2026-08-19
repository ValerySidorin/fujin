package paho

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/panjf2000/ants/v2"
)

// Writer implements connector.WriteCloser for MQTT using paho.golang
type Writer struct {
	conf ConnectorConfig
	cm   *autopaho.ConnectionManager
	pool *ants.Pool
	l    *slog.Logger
	wg   sync.WaitGroup
}

// NewWriter creates a new MQTT writer using paho.golang
func NewWriter(conf ConnectorConfig, l *slog.Logger) (connector.WriteCloser, error) {
	if err := conf.ValidateWriter(); err != nil {
		return nil, fmt.Errorf("mqtt_paho: validate config: %w", err)
	}

	serverURL, err := url.Parse(conf.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("mqtt_paho: parse broker url: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     conf.KeepAlive,
		CleanStartOnInitialConnection: conf.CleanStart,
		SessionExpiryInterval:         conf.SessionExpiry,
		ConnectTimeout:                conf.ConnectTimeout,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			l.Info("mqtt writer connection up", "session_present", connAck.SessionPresent)
		},
		OnConnectError: func(err error) {
			l.Error("mqtt writer connection error", "err", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: conf.ClientID,
		},
	}

	ctx := context.Background()
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, fmt.Errorf("mqtt_paho: new connection: %w", err)
	}

	// Wait for connection
	if err := cm.AwaitConnection(ctx); err != nil {
		_ = cm.Disconnect(ctx)
		return nil, fmt.Errorf("mqtt_paho: await connection: %w", err)
	}

	pool, err := ants.NewPool(conf.Pool.Size, ants.WithPreAlloc(conf.Pool.PreAlloc))
	if err != nil {
		_ = cm.Disconnect(ctx)
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return &Writer{
		conf: conf,
		cm:   cm,
		pool: pool,
		l:    l,
	}, nil
}

func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	w.wg.Add(1)

	publish := &paho.Publish{
		Topic:   w.conf.Topic,
		QoS:     w.conf.QoS,
		Retain:  w.conf.Retain,
		Payload: msg,
	}

	err := w.pool.Submit(func() {
		defer w.wg.Done()
		_, err := w.cm.Publish(ctx, publish)
		callback(err)
	})

	if err != nil {
		callback(err)
		w.wg.Done()
	}
}

func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	w.wg.Add(1)

	publish := &paho.Publish{
		Topic:   w.conf.Topic,
		QoS:     w.conf.QoS,
		Retain:  w.conf.Retain,
		Payload: msg,
	}

	// Add headers as user properties
	if len(headers) > 0 {
		props := &paho.PublishProperties{}
		for i := 0; i < len(headers); i += 2 {
			key := string(headers[i])
			var val string
			if i+1 < len(headers) {
				val = string(headers[i+1])
			}
			props.User = append(props.User, paho.UserProperty{Key: key, Value: val})
		}
		publish.Properties = props
	}

	err := w.pool.Submit(func() {
		defer w.wg.Done()
		_, err := w.cm.Publish(ctx, publish)
		callback(err)
	})

	if err != nil {
		callback(err)
		w.wg.Done()
	}
}

func (w *Writer) Flush(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) CommitTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) Close() error {
	w.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), w.conf.DisconnectTimeout)
	defer cancel()

	if err := w.cm.Disconnect(ctx); err != nil {
		w.l.Error("mqtt_paho: disconnect error", "err", err)
	}

	if w.pool != nil {
		if w.conf.Pool.ReleaseTimeout != 0 {
			if err := w.pool.ReleaseTimeout(w.conf.Pool.ReleaseTimeout); err != nil {
				return fmt.Errorf("release pool: %w", err)
			}
		} else {
			w.pool.Release()
		}
	}
	return nil
}
