//go:build zeromq_pebbe && cgo

package pebbe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fujin-io/fujin/public/plugins/connector"
	zmq "github.com/pebbe/zmq4"
)

var monitorSequence atomic.Uint64

type runtime struct {
	ctx       *zmq.Context
	routes    map[string]*routeRuntime
	zap       *zapActor
	closeOnce sync.Once
	closeErr  error
}

type routeRuntime struct {
	config routeConfig
	writer *writerActor
	reader *readerActor
}

func openRuntime(config Config, l *slog.Logger) (_ connector.Runtime, err error) {
	if l == nil {
		l = slog.Default()
	}
	ctx, err := zmq.NewContext()
	if err != nil {
		return nil, fmt.Errorf("zeromq: create context: %w", err)
	}
	ctx.SetRetryAfterEINTR(true)
	if err := ctx.SetIoThreads(config.Common.IOThreads); err != nil {
		_ = ctx.Term()
		return nil, fmt.Errorf("zeromq: set io_threads: %w", err)
	}
	if err := ctx.SetBlocky(false); err != nil {
		_ = ctx.Term()
		return nil, fmt.Errorf("zeromq: set non-blocking context termination: %w", err)
	}
	r := &runtime{ctx: ctx, routes: make(map[string]*routeRuntime, len(config.Routes))}
	defer func() {
		if err != nil {
			cleanupErr := r.Close(context.Background())
			err = errors.Join(err, cleanupErr)
		}
	}()

	routeNames := make([]string, 0, len(config.Routes))
	policies := make(map[string]map[string]struct{})
	prepared := make(map[string]routeConfig, len(config.Routes))
	for name, settings := range config.Routes {
		routeNames = append(routeNames, name)
		route := routeConfig{name: name, CommonSettings: config.Common, RouteSettings: settings, zapDomain: "fujin." + name}
		if settings.Security.Mechanism == SecurityCurve {
			secret, readErr := os.ReadFile(settings.Security.SecretKeyPath)
			if readErr != nil {
				return nil, fmt.Errorf("zeromq route %q: read secret key: %w", name, readErr)
			}
			route.secretKey = strings.TrimSpace(string(secret))
			if !validZ85Key(route.secretKey) {
				return nil, fmt.Errorf("zeromq route %q: secret key file must contain one valid Z85 key", name)
			}
			if settings.Mode == ModeBind {
				allowed := make(map[string]struct{}, len(settings.Security.AllowedClientPublicKeys))
				for _, key := range settings.Security.AllowedClientPublicKeys {
					allowed[key] = struct{}{}
				}
				policies[route.zapDomain] = allowed
			}
		}
		prepared[name] = route
	}
	sort.Strings(routeNames)
	if len(policies) > 0 {
		r.zap, err = startZAPActor(ctx, policies, config.Common.ReceivePollInterval, l)
		if err != nil {
			return nil, err
		}
	}
	for _, name := range routeNames {
		route := prepared[name]
		routeRuntime, openErr := openRoute(ctx, route, l)
		if openErr != nil {
			return nil, fmt.Errorf("zeromq route %q: %w", name, openErr)
		}
		r.routes[name] = routeRuntime
	}
	return r, nil
}

func openRoute(ctx *zmq.Context, route routeConfig, l *slog.Logger) (*routeRuntime, error) {
	socketType, err := routeSocketType(route.Pattern)
	if err != nil {
		return nil, err
	}
	socket, err := ctx.NewSocket(socketType)
	if err != nil {
		return nil, fmt.Errorf("create socket: %w", err)
	}
	closeSocket := true
	defer func() {
		if closeSocket {
			_ = socket.Close()
		}
	}()
	if err := configureSocket(socket, route); err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	var monitor *routeMonitor
	if route.Mode == ModeConnect {
		monitor, err = startRouteMonitor(ctx, socket, route, ready, l)
		if err != nil {
			return nil, err
		}
	}
	if route.Mode == ModeBind {
		err = socket.Bind(route.Endpoint)
	} else {
		err = socket.Connect(route.Endpoint)
	}
	if err != nil {
		if monitor != nil {
			monitor.close()
		}
		return nil, fmt.Errorf("%s %s: %w", route.Mode, route.Endpoint, err)
	}
	if route.Mode == ModeBind {
		close(ready)
	}
	result := &routeRuntime{config: route}
	switch route.Pattern {
	case PatternPub, PatternPush:
		result.writer = newWriterActor(socket, route, ready, monitor)
	case PatternSub, PatternPull:
		result.reader = newReaderActor(socket, route, ready, monitor, l)
	default:
		return nil, fmt.Errorf("unsupported pattern %q", route.Pattern)
	}
	closeSocket = false
	return result, nil
}

func configureSocket(socket *zmq.Socket, route routeConfig) error {
	setters := []struct {
		name string
		fn   func() error
	}{
		{"send_hwm", func() error { return socket.SetSndhwm(route.SendHWM) }},
		{"receive_hwm", func() error { return socket.SetRcvhwm(route.ReceiveHWM) }},
		{"send_timeout", func() error { return socket.SetSndtimeo(route.SendTimeout) }},
		{"receive_timeout", func() error { return socket.SetRcvtimeo(route.ReceivePollInterval) }},
		{"linger", func() error { return socket.SetLinger(route.Linger) }},
		{"reconnect_interval", func() error { return socket.SetReconnectIvl(route.ReconnectInterval) }},
		{"reconnect_interval_max", func() error { return socket.SetReconnectIvlMax(route.ReconnectIntervalMax) }},
	}
	for _, setter := range setters {
		if err := setter.fn(); err != nil {
			return fmt.Errorf("set %s: %w", setter.name, err)
		}
	}
	if route.Mode == ModeConnect && route.Pattern == PatternPush {
		if err := socket.SetImmediate(true); err != nil {
			return fmt.Errorf("set immediate delivery: %w", err)
		}
	}
	if route.Pattern == PatternSub {
		subscriptions := route.Subscriptions
		if len(subscriptions) == 0 {
			subscriptions = []string{""}
		}
		for _, subscription := range subscriptions {
			if err := socket.SetSubscribe(subscription); err != nil {
				return fmt.Errorf("subscribe %q: %w", subscription, err)
			}
		}
	}
	if route.Security.Mechanism == SecurityCurve {
		if err := socket.SetCurvePublickey(route.Security.PublicKey); err != nil {
			return fmt.Errorf("set CURVE public key: %w", err)
		}
		if err := socket.SetCurveSecretkey(route.secretKey); err != nil {
			return fmt.Errorf("set CURVE secret key: %w", err)
		}
		if route.Mode == ModeBind {
			if err := socket.SetCurveServer(1); err != nil {
				return fmt.Errorf("enable CURVE server: %w", err)
			}
			if err := socket.SetZapDomain(route.zapDomain); err != nil {
				return fmt.Errorf("set ZAP domain: %w", err)
			}
		} else if err := socket.SetCurveServerkey(route.Security.ServerPublicKey); err != nil {
			return fmt.Errorf("set CURVE server key: %w", err)
		}
	}
	return nil
}

func routeSocketType(pattern string) (zmq.Type, error) {
	switch pattern {
	case PatternPub:
		return zmq.PUB, nil
	case PatternSub:
		return zmq.SUB, nil
	case PatternPush:
		return zmq.PUSH, nil
	case PatternPull:
		return zmq.PULL, nil
	default:
		return 0, fmt.Errorf("unsupported pattern %q", pattern)
	}
}

func (r *runtime) NewReader(route string, autoSettle bool, _ *slog.Logger) (connector.ReadCloser, error) {
	routeRuntime := r.routes[route]
	if routeRuntime == nil {
		return nil, connector.ErrRouteNotFound
	}
	if routeRuntime.reader == nil {
		return nil, connector.ErrOperationUnsupported
	}
	return newReader(routeRuntime.reader, autoSettle), nil
}

func (r *runtime) NewWriter(route string, _ *slog.Logger) (connector.WriteCloser, error) {
	routeRuntime := r.routes[route]
	if routeRuntime == nil {
		return nil, connector.ErrRouteNotFound
	}
	if routeRuntime.writer == nil {
		return nil, connector.ErrOperationUnsupported
	}
	return newWriter(routeRuntime.writer), nil
}

func (r *runtime) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		names := make([]string, 0, len(r.routes))
		for name := range r.routes {
			names = append(names, name)
		}
		sort.Strings(names)
		var errs []error
		for _, name := range names {
			route := r.routes[name]
			if route.writer != nil {
				errs = append(errs, route.writer.close(ctx))
			}
			if route.reader != nil {
				errs = append(errs, route.reader.close(ctx))
			}
		}
		if r.zap != nil {
			errs = append(errs, r.zap.close(ctx))
		}
		if r.ctx != nil {
			errs = append(errs, r.ctx.Term())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

type routeMonitor struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func startRouteMonitor(ctx *zmq.Context, socket *zmq.Socket, route routeConfig, ready chan struct{}, l *slog.Logger) (*routeMonitor, error) {
	endpoint := fmt.Sprintf("inproc://fujin.zmq.monitor.%d", monitorSequence.Add(1))
	if err := socket.Monitor(endpoint, zmq.EVENT_HANDSHAKE_SUCCEEDED|zmq.EVENT_HANDSHAKE_FAILED_AUTH|zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL); err != nil {
		return nil, fmt.Errorf("start socket monitor: %w", err)
	}
	monitorSocket, err := ctx.NewSocket(zmq.PAIR)
	if err != nil {
		return nil, fmt.Errorf("create monitor socket: %w", err)
	}
	if err := monitorSocket.SetLinger(0); err != nil {
		_ = monitorSocket.Close()
		return nil, err
	}
	if err := monitorSocket.SetRcvtimeo(route.ReceivePollInterval); err != nil {
		_ = monitorSocket.Close()
		return nil, err
	}
	if err := monitorSocket.Connect(endpoint); err != nil {
		_ = monitorSocket.Close()
		return nil, err
	}
	monitor := &routeMonitor{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(monitor.done)
		defer monitorSocket.Close()
		readyOnce := sync.Once{}
		for {
			select {
			case <-monitor.stop:
				return
			default:
			}
			event, _, _, recvErr := monitorSocket.RecvEvent(0)
			if recvErr != nil {
				if zmq.AsErrno(recvErr) == zmq.ETERM {
					return
				}
				if zmq.AsErrno(recvErr) == zmq.Errno(syscall.EAGAIN) {
					continue
				}
				l.Warn("zeromq socket monitor", "route", route.name, "err", recvErr)
				continue
			}
			if event == zmq.EVENT_HANDSHAKE_SUCCEEDED {
				readyOnce.Do(func() { close(ready) })
			}
			if event == zmq.EVENT_HANDSHAKE_FAILED_AUTH || event == zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL {
				l.Warn("zeromq handshake failed", "route", route.name, "event", event.String())
			}
		}
	}()
	return monitor, nil
}

func (m *routeMonitor) close() {
	if m == nil {
		return
	}
	m.once.Do(func() { close(m.stop); <-m.done })
}
