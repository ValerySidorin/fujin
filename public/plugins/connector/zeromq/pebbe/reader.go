//go:build zeromq_pebbe && cgo

package pebbe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	zmq "github.com/pebbe/zmq4"
)

var (
	ErrSlowConsumer  = errors.New("zeromq slow consumer detached")
	errReaderClosed  = errors.New("zeromq reader closed")
	errRuntimeClosed = errors.New("zeromq runtime closed")
)

type readerSubscription struct {
	id       uint64
	messages chan decodedMessage
	terminal chan error
	once     sync.Once
}

func (s *readerSubscription) terminate(err error) {
	s.once.Do(func() { s.terminal <- err })
}

type readerControl struct {
	add    *readerSubscription
	remove *readerSubscription
	ack    chan struct{}
}

type readerActor struct {
	socket           *zmq.Socket
	route            routeConfig
	ready            <-chan struct{}
	monitor          *routeMonitor
	logger           *slog.Logger
	controls         chan readerControl
	stop             chan struct{}
	done             chan struct{}
	closeOnce        sync.Once
	nextSubscription atomic.Uint64
	malformed        atomic.Uint64
	oversized        atomic.Uint64
}

func newReaderActor(socket *zmq.Socket, route routeConfig, ready <-chan struct{}, monitor *routeMonitor, l *slog.Logger) *readerActor {
	a := &readerActor{socket: socket, route: route, ready: ready, monitor: monitor, logger: l, controls: make(chan readerControl, 64), stop: make(chan struct{}), done: make(chan struct{})}
	go a.run()
	return a
}

func (a *readerActor) run() {
	defer close(a.done)
	defer a.monitor.close()
	defer a.socket.Close()
	poller := zmq.NewPoller()
	poller.Add(a.socket, zmq.POLLIN)
	subscriptions := make(map[uint64]*readerSubscription)
	order := make([]uint64, 0)
	roundRobin := 0
	var lastMalformedLog time.Time
	for {
		if len(subscriptions) == 0 {
			select {
			case <-a.stop:
				return
			case control := <-a.controls:
				order, roundRobin = applyReaderControl(subscriptions, order, roundRobin, control)
			}
			continue
		}
		select {
		case <-a.stop:
			for _, subscription := range subscriptions {
				subscription.terminate(errRuntimeClosed)
			}
			return
		case control := <-a.controls:
			order, roundRobin = applyReaderControl(subscriptions, order, roundRobin, control)
			continue
		default:
		}
		polled, err := poller.Poll(a.route.ReceivePollInterval)
		if err != nil {
			if zmq.AsErrno(err) == zmq.ETERM {
				return
			}
			if zmq.AsErrno(err) == zmq.Errno(syscall.EINTR) {
				continue
			}
			a.logger.Warn("zeromq poll", "route", a.route.name, "err", err)
			continue
		}
		if len(polled) == 0 {
			continue
		}
		frames, err := a.socket.RecvMessageBytes(0)
		if err != nil {
			if zmq.AsErrno(err) == zmq.Errno(syscall.EAGAIN) {
				continue
			}
			a.logger.Warn("zeromq receive", "route", a.route.name, "err", err)
			continue
		}
		message, err := decodeMessage(a.route, frames)
		if err != nil {
			if errors.Is(err, errMessageTooLarge) {
				a.oversized.Add(1)
			} else {
				a.malformed.Add(1)
			}
			if time.Since(lastMalformedLog) >= time.Second {
				a.logger.Warn("zeromq dropped malformed message", "route", a.route.name, "err", err)
				lastMalformedLog = time.Now()
			}
			continue
		}
		if a.route.Pattern == PatternSub {
			order = broadcastMessage(subscriptions, order, message)
			if roundRobin >= len(order) {
				roundRobin = 0
			}
		} else {
			order, roundRobin = dispatchMessage(subscriptions, order, roundRobin, message)
		}
	}
}

func applyReaderControl(subscriptions map[uint64]*readerSubscription, order []uint64, roundRobin int, control readerControl) ([]uint64, int) {
	if control.add != nil {
		subscriptions[control.add.id] = control.add
		order = append(order, control.add.id)
	}
	if control.remove != nil {
		if _, exists := subscriptions[control.remove.id]; exists {
			delete(subscriptions, control.remove.id)
			order = removeSubscription(order, control.remove.id)
			control.remove.terminate(errReaderClosed)
		}
	}
	if roundRobin >= len(order) {
		roundRobin = 0
	}
	if control.ack != nil {
		close(control.ack)
	}
	return order, roundRobin
}

func broadcastMessage(subscriptions map[uint64]*readerSubscription, order []uint64, message decodedMessage) []uint64 {
	kept := order[:0]
	for _, id := range order {
		subscription := subscriptions[id]
		if subscription == nil {
			continue
		}
		select {
		case subscription.messages <- cloneDecodedMessage(message):
			kept = append(kept, id)
		default:
			delete(subscriptions, id)
			subscription.terminate(ErrSlowConsumer)
		}
	}
	return kept
}

func dispatchMessage(subscriptions map[uint64]*readerSubscription, order []uint64, roundRobin int, message decodedMessage) ([]uint64, int) {
	for len(order) > 0 {
		if roundRobin >= len(order) {
			roundRobin = 0
		}
		id := order[roundRobin]
		subscription := subscriptions[id]
		if subscription == nil {
			order = removeSubscription(order, id)
			continue
		}
		select {
		case subscription.messages <- message:
			return order, (roundRobin + 1) % len(order)
		default:
			delete(subscriptions, id)
			order = removeSubscription(order, id)
			subscription.terminate(ErrSlowConsumer)
		}
	}
	return order, 0
}

func removeSubscription(order []uint64, id uint64) []uint64 {
	for index, candidate := range order {
		if candidate == id {
			return append(order[:index], order[index+1:]...)
		}
	}
	return order
}

func cloneDecodedMessage(message decodedMessage) decodedMessage {
	cloned := decodedMessage{payload: append([]byte(nil), message.payload...), source: message.source, headers: make([][]byte, len(message.headers))}
	for index, header := range message.headers {
		cloned.headers[index] = append([]byte(nil), header...)
	}
	return cloned
}

func (a *readerActor) subscribe(ctx context.Context) (*readerSubscription, error) {
	subscription := &readerSubscription{id: a.nextSubscription.Add(1), messages: make(chan decodedMessage, a.route.SubscriberQueueCapacity), terminal: make(chan error, 1)}
	ack := make(chan struct{})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.done:
		return nil, errRuntimeClosed
	case a.controls <- readerControl{add: subscription, ack: ack}:
	}
	select {
	case <-ctx.Done():
		a.unsubscribe(subscription)
		return nil, ctx.Err()
	case <-a.done:
		return nil, errRuntimeClosed
	case <-ack:
		return subscription, nil
	}
}

func (a *readerActor) unsubscribe(subscription *readerSubscription) {
	if subscription == nil {
		return
	}
	ack := make(chan struct{})
	select {
	case <-a.done:
		return
	case a.controls <- readerControl{remove: subscription, ack: ack}:
	}
	select {
	case <-ack:
	case <-a.done:
	}
}

func (a *readerActor) awaitReady(ctx context.Context) error {
	timer := time.NewTimer(a.route.ReadyTimeout)
	defer timer.Stop()
	select {
	case <-a.ready:
		return nil
	case <-a.done:
		return errRuntimeClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("zeromq route %q readiness timeout after %s", a.route.name, a.route.ReadyTimeout)
	}
}

func (a *readerActor) close(ctx context.Context) error {
	a.closeOnce.Do(func() { close(a.stop) })
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Reader struct {
	actor        *readerActor
	autoSettle   bool
	mu           sync.Mutex
	subscription *readerSubscription
	closed       bool
}

func newReader(actor *readerActor, autoSettle bool) *Reader {
	return &Reader{actor: actor, autoSettle: autoSettle}
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, handler func([]byte, string, ...any)) error {
	return r.subscribe(ctx, ready, func(message decodedMessage) { handler(message.payload, message.source) })
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, handler func([]byte, string, [][]byte, ...any)) error {
	if r.actor.route.Framing != FramingFujinV1 {
		return connector.ErrOperationUnsupported
	}
	return r.subscribe(ctx, ready, func(message decodedMessage) { handler(message.payload, message.source, message.headers) })
}

func (r *Reader) subscribe(ctx context.Context, ready func() error, handler func(decodedMessage)) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errReaderClosed
	}
	if r.subscription != nil {
		r.mu.Unlock()
		return errors.New("zeromq reader already subscribed")
	}
	subscription, err := r.actor.subscribe(ctx)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.subscription = subscription
	r.mu.Unlock()
	defer func() {
		r.actor.unsubscribe(subscription)
		r.mu.Lock()
		if r.subscription == subscription {
			r.subscription = nil
		}
		r.mu.Unlock()
	}()
	if err := r.actor.awaitReady(ctx); err != nil {
		return err
	}
	if err := ready(); err != nil {
		return err
	}
	for {
		select {
		case err := <-subscription.terminal:
			return err
		default:
		}
		select {
		case message := <-subscription.messages:
			handler(message)
		case err := <-subscription.terminal:
			return err
		case <-ctx.Done():
			select {
			case err := <-subscription.terminal:
				return err
			default:
				return nil
			}
		}
	}
}

func (*Reader) Fetch(_ context.Context, _ uint32, handler func(uint32, error), _ func([]byte, string, ...any)) {
	handler(0, util.ErrNotSupported)
}
func (*Reader) FetchWithHeaders(_ context.Context, _ uint32, handler func(uint32, error), _ func([]byte, string, [][]byte, ...any)) {
	handler(0, util.ErrNotSupported)
}
func (*Reader) Ack(_ context.Context, _ [][]byte, handler func(error), _ func([]byte, error)) {
	handler(util.ErrNotSupported)
}
func (*Reader) Nack(_ context.Context, _ [][]byte, handler func(error), _ func([]byte, error)) {
	handler(util.ErrNotSupported)
}
func (*Reader) MsgIDArgsLen() int                                    { return 0 }
func (*Reader) EncodeMsgID(buffer []byte, _ string, _ ...any) []byte { return buffer }
func (r *Reader) AutoCommit() bool                                   { return r.autoSettle }

func (r *Reader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	subscription := r.subscription
	r.mu.Unlock()
	r.actor.unsubscribe(subscription)
	return nil
}

var _ connector.ReadCloser = (*Reader)(nil)
