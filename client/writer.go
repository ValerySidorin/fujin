package client

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/ValerySidorin/fujin/internal/common/fnet"
	"github.com/ValerySidorin/fujin/internal/server/fujin/pool"
	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
)

type parseState struct{}

type Writer struct {
	conn *Conn

	ps  *parseState
	r   *reader
	out *fnet.Outbound

	wcm *defaultCorrelationManager
	bcm *defaultCorrelationManager
	ccm *defaultCorrelationManager
	rcm *defaultCorrelationManager

	mu sync.Mutex
	wg sync.WaitGroup
}

func (c *Conn) ConnectWriter(id uint32) (*Writer, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}

	stream, err := c.qconn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("quic: open stream: %w", err)
	}

	buf := pool.Get(5)
	defer pool.Put(buf)

	buf = append(buf, byte(request.OP_CODE_CONNECT_WRITER))
	buf = binary.BigEndian.AppendUint32(buf, id)

	if _, err := stream.Write(buf); err != nil {
		stream.Close()
		return nil, fmt.Errorf("quic: write connect writer: %w", err)
	}

	r := &reader{
		r: stream,
	}

	out := fnet.NewOutbound(stream, 5*time.Second, c.l)

	w := &Writer{
		conn: c,
		out:  out,
		r:    r,
		wcm:  newDefaultCorrelationManager(),
		bcm:  newDefaultCorrelationManager(),
		ccm:  newDefaultCorrelationManager(),
		rcm:  newDefaultCorrelationManager(),
	}

	w.wg.Add(3)
	go out.WriteLoop()
	go w.readLoop()

	return w, nil
}

func (w *Writer) Write(topic string, p []byte) error {
	buf := pool.Get(len(topic) + len(p) + 13)
	defer pool.Put(buf)

	ch := make(chan error, 1)
	defer close(ch)

	id := w.wcm.next(ch)
	defer w.wcm.delete(id)

	buf = append(buf, byte(request.OP_CODE_WRITE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	w.out.EnqueueProto(buf)

	select {
	case <-time.After(w.conn.timeout):
		return ErrTimeout
	case err := <-ch:
		return err
	}
}

func (w *Writer) Close() error {
	// Send - receive disconnect
	w.out.Close()
	w.out.BroadcastCond()

	w.wg.Wait()
	return nil
}

func (w *Writer) readLoop() {
	defer w.wg.Done()

	// Crete a parseState if needed.
	w.mu.Lock()
	if w.ps == nil {
		w.ps = &parseState{}
	}
	w.mu.Unlock()

	if w.conn == nil {
		return
	}

	for {
		buf, err := w.r.Read()
		if err == nil {
			if len(buf) == 0 {
				continue
			}

			// parse
			// err = nc.parse(buf)
		}
		if err != nil {
			// handle error
			// if shouldClose := nc.processOpErr(err); shouldClose {
			// 	nc.close(CLOSED, true, nil)
			// }
			break
		}
	}
	// Clear the parseState here..
	w.mu.Lock()
	w.ps = nil
	w.mu.Unlock()
}
