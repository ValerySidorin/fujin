//go:build zeromq_pebbe && cgo

package pebbe

import (
	"context"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	zmq "github.com/pebbe/zmq4"
)

type sendRequest struct {
	ctx      context.Context
	frames   [][]byte
	callback func(error)
}

type writerCommand struct {
	send    *sendRequest
	barrier chan error
}

type writerActor struct {
	socket    *zmq.Socket
	route     routeConfig
	monitor   *routeMonitor
	ready     <-chan struct{}
	commands  chan writerCommand
	stop      chan struct{}
	done      chan struct{}
	stateMu   sync.RWMutex
	closing   bool
	closeOnce sync.Once
}

func newWriterActor(socket *zmq.Socket, route routeConfig, ready <-chan struct{}, monitor *routeMonitor) *writerActor {
	a := &writerActor{socket: socket, route: route, ready: ready, monitor: monitor, commands: make(chan writerCommand, route.SendHWM), stop: make(chan struct{}), done: make(chan struct{})}
	go a.run()
	return a
}

func (a *writerActor) run() {
	defer close(a.done)
	defer a.monitor.close()
	defer a.socket.Close()
	for {
		select {
		case <-a.stop:
			return
		case command := <-a.commands:
			if command.barrier != nil {
				command.barrier <- nil
				continue
			}
			request := command.send
			if request == nil {
				continue
			}
			if err := request.ctx.Err(); err != nil {
				request.callback(err)
				continue
			}
			err := sendFrames(a.socket, request.frames)
			if err != nil && request.ctx.Err() != nil {
				err = request.ctx.Err()
			}
			request.callback(err)
		}
	}
}

func sendFrames(socket *zmq.Socket, frames [][]byte) error {
	for index, frame := range frames {
		flags := zmq.Flag(0)
		if index+1 < len(frames) {
			flags = zmq.SNDMORE
		}
		if _, err := socket.SendBytes(frame, flags); err != nil {
			return err
		}
	}
	return nil
}

func (a *writerActor) submit(command writerCommand, ctx context.Context) error {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.closing {
		return connector.ErrWriterClosed
	}
	select {
	case <-a.stop:
		return connector.ErrWriterClosed
	case <-ctx.Done():
		return ctx.Err()
	case a.commands <- command:
		return nil
	}
}

func (a *writerActor) close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		a.stateMu.Lock()
		a.closing = true
		close(a.stop)
		a.stateMu.Unlock()
	})
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Writer struct {
	actor  *writerActor
	mu     sync.Mutex
	closed bool
}

func newWriter(actor *writerActor) *Writer { return &Writer{actor: actor} }

func (w *Writer) Produce(ctx context.Context, payload []byte, callback func(error)) {
	w.produce(ctx, payload, nil, false, callback)
}

func (w *Writer) HProduce(ctx context.Context, payload []byte, headers [][]byte, callback func(error)) {
	w.produce(ctx, payload, headers, true, callback)
}

func (w *Writer) produce(ctx context.Context, payload []byte, headers [][]byte, withHeaders bool, callback func(error)) {
	frames, err := encodeMessage(w.actor.route, payload, headers, withHeaders)
	if err != nil {
		callback(err)
		return
	}
	frames = cloneFrames(frames)
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		callback(connector.ErrWriterClosed)
		return
	}
	err = w.actor.submit(writerCommand{send: &sendRequest{ctx: ctx, frames: frames, callback: callback}}, ctx)
	w.mu.Unlock()
	if err != nil {
		callback(err)
	}
}

func (w *Writer) Flush(ctx context.Context) error {
	barrier := make(chan error, 1)
	w.mu.Lock()
	err := w.actor.submit(writerCommand{barrier: barrier}, ctx)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.actor.done:
		return connector.ErrWriterClosed
	}
}

func (*Writer) BeginTx(context.Context) error    { return util.ErrNotSupported }
func (*Writer) CommitTx(context.Context) error   { return util.ErrNotSupported }
func (*Writer) RollbackTx(context.Context) error { return util.ErrNotSupported }

func (w *Writer) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

func cloneFrames(frames [][]byte) [][]byte {
	cloned := make([][]byte, len(frames))
	for index, frame := range frames {
		cloned[index] = append([]byte(nil), frame...)
	}
	return cloned
}

var _ connector.WriteCloser = (*Writer)(nil)
