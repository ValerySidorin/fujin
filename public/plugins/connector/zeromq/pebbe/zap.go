//go:build zeromq_pebbe && cgo

package pebbe

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type zapActor struct {
	socket    *zmq.Socket
	policies  map[string]map[string]struct{}
	logger    *slog.Logger
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func startZAPActor(ctx *zmq.Context, policies map[string]map[string]struct{}, pollInterval time.Duration, l *slog.Logger) (*zapActor, error) {
	socket, err := ctx.NewSocket(zmq.REP)
	if err != nil {
		return nil, fmt.Errorf("zeromq ZAP: create socket: %w", err)
	}
	if err := socket.SetLinger(0); err != nil {
		_ = socket.Close()
		return nil, err
	}
	if err := socket.SetRcvtimeo(pollInterval); err != nil {
		_ = socket.Close()
		return nil, err
	}
	if err := socket.SetSndtimeo(pollInterval); err != nil {
		_ = socket.Close()
		return nil, err
	}
	if err := socket.Bind("inproc://zeromq.zap.01"); err != nil {
		_ = socket.Close()
		return nil, fmt.Errorf("zeromq ZAP: bind: %w", err)
	}
	a := &zapActor{socket: socket, policies: policies, logger: l, stop: make(chan struct{}), done: make(chan struct{})}
	go a.run()
	return a, nil
}

func (a *zapActor) run() {
	defer close(a.done)
	defer a.socket.Close()
	for {
		select {
		case <-a.stop:
			return
		default:
		}
		request, err := a.socket.RecvMessageBytes(0)
		if err != nil {
			if zmq.AsErrno(err) == zmq.ETERM {
				return
			}
			if zmq.AsErrno(err) == zmq.Errno(syscall.EAGAIN) {
				continue
			}
			a.logger.Warn("zeromq ZAP receive", "err", err)
			continue
		}
		response := zapResponse(request, a.policies)
		if err := sendFrames(a.socket, response); err != nil {
			a.logger.Warn("zeromq ZAP response", "err", err)
		}
	}
}

func zapResponse(request [][]byte, policies map[string]map[string]struct{}) [][]byte {
	version := []byte("1.0")
	sequence := []byte{}
	if len(request) > 0 {
		version = request[0]
	}
	if len(request) > 1 {
		sequence = request[1]
	}
	status, text, user := []byte("400"), []byte("Denied"), []byte{}
	if len(request) == 7 && string(request[0]) == "1.0" && string(request[5]) == "CURVE" && len(request[6]) == 32 {
		allowed := policies[string(request[2])]
		clientKey := zmq.Z85encode(string(request[6]))
		if _, ok := allowed[clientKey]; ok {
			status, text, user = []byte("200"), []byte("OK"), []byte(clientKey)
		}
	}
	return [][]byte{version, sequence, status, text, user, {}}
}

func (a *zapActor) close(ctx context.Context) error {
	a.closeOnce.Do(func() { close(a.stop) })
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
