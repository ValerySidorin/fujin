package fujin

import (
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/internal/api/fujin/pool"
	"github.com/quic-go/quic-go"
)

const (
	maxBufSize    = 65536
	MaxVectorSize = 1024
)

type outbound struct {
	v      net.Buffers   // vector
	wv     net.Buffers   // working vector
	wdl    time.Duration // write deadline
	c      *sync.Cond
	pb     int64 // pending bytes
	mu     sync.Mutex
	str    quic.SendStream // current quic stream
	closed atomic.Bool
	l      *slog.Logger
}

func newOutbound(
	str quic.SendStream, wdl time.Duration,
	l *slog.Logger) *outbound {
	o := &outbound{
		str: str,
		wdl: wdl,
		l:   l,
	}
	o.c = sync.NewCond(&(o.mu))

	return o
}

func (o *outbound) writeloop() {
	waitOK := true
	var closed bool

	for {
		o.mu.Lock()
		if closed = o.isClosed(); !closed {
			if waitOK && (o.pb == 0 || o.pb < maxBufSize) {
				o.c.Wait()
				closed = o.isClosed()
			}
		}

		if closed {
			o.flushOutbound()
			o.mu.Unlock()
			return
		}

		waitOK = o.flushOutbound()
		o.mu.Unlock()
	}
}

func (o *outbound) flushOutbound() bool {
	defer func() {
		if o.isClosed() {
			for i := range o.wv {
				pool.Put(o.wv[i])
			}
			o.wv = nil
		}
	}()

	if o.str == nil || o.pb == 0 {
		return true
	}

	detached, _ := o.getV()
	o.v = nil

	o.wv = append(o.wv, detached...)
	var _orig [MaxVectorSize][]byte
	orig := append(_orig[:0], o.wv...)

	startOfWv := o.wv[0:]

	start := time.Now()

	var n int64
	var wn int64
	var err error

	for len(o.wv) > 0 {
		wv := o.wv
		if len(wv) > MaxVectorSize {
			wv = wv[:MaxVectorSize]
		}
		consumed := len(wv)

		_ = o.str.SetWriteDeadline(start.Add(o.wdl))
		wn, err = wv.WriteTo(o.str)
		_ = o.str.SetWriteDeadline(time.Time{})

		n += wn
		o.wv = o.wv[consumed-len(wv):]
		if err != nil {
			o.l.Error("write buffers", "err", err)
			break
		}
	}

	for i := 0; i < len(orig)-len(o.wv); i++ {
		pool.Put(orig[i])
	}

	o.wv = append(startOfWv[:0], o.wv...)

	o.pb -= n
	if o.pb > 0 {
		o.signalFlush()
	}

	return true
}

func (o *outbound) getV() (net.Buffers, int64) {
	return o.v, o.pb
}

func (o *outbound) signalFlush() {
	o.c.Signal()
}

func (o *outbound) enqueueProto(proto []byte) {
	if o.isClosed() {
		return
	}

	o.queueOutbound(proto)
	o.signalFlush()
}

func (o *outbound) queueOutbound(data []byte) {
	if o.isClosed() {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.pb += int64(len(data))
	toBuffer := data
	if len(o.v) > 0 {
		last := &o.v[len(o.v)-1]
		if free := cap(*last) - len(*last); free > 0 {
			if l := len(toBuffer); l < free {
				free = l
			}
			*last = append(*last, toBuffer[:free]...)
			toBuffer = toBuffer[free:]
		}
	}

	for len(toBuffer) > 0 {
		new := pool.Get(len(toBuffer))
		n := copy(new[:cap(new)], toBuffer)
		o.v = append(o.v, new[:n])
		toBuffer = toBuffer[n:]
	}
}

func (o *outbound) isClosed() bool {
	return o.closed.Load()
}

func (o *outbound) close() {
	o.closed.Store(true)
	o.c.Broadcast()
}
