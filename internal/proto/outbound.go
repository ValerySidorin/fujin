package proto

import (
	"errors"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fujin-io/fujin/internal/proto/pool"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
)

const (
	MaxVectorSize                  = 1024
	fallbackWriteBatchLen          = 64 * 1024
	maximumPendingBytes            = 4 * 1024 * 1024
	maximumMediumFramePendingBytes = 64 * 1024 * 1024
	minimumWriteBatchLen           = 4 * 1024
)

type Outbound struct {
	sync.Mutex
	v            net.Buffers   // vector
	wv           net.Buffers   // working vector
	wvOffset     int           // bytes already written from wv[0]
	batchStorage net.Buffers   // allocated only after partial writes or ENOBUFS
	wdl          time.Duration // write deadline
	c            *sync.Cond
	pb           int64 // pending bytes
	str          session.Stream
	closed       atomic.Bool
	l            *slog.Logger
}

func NewOutbound(
	str session.Stream, wdl time.Duration,
	l *slog.Logger) *Outbound {
	o := &Outbound{
		str: str,
		wdl: wdl,
		l:   l,
	}
	o.c = sync.NewCond(&(o.Mutex))

	return o
}

func (o *Outbound) WriteLoop() {
	for {
		o.Lock()
		for o.pb == 0 && !o.IsClosed() {
			o.c.Wait()
		}

		if o.IsClosed() {
			// Detach remaining pending buffers without discarding either vector's capacity.
			o.detachPendingNoLock()
			o.Unlock()

			// Final write without lock.
			o.writeBuffers()

			o.Lock()
			for i := range o.v {
				pool.Put(o.v[i])
			}
			o.v = nil
			for i := range o.wv {
				pool.Put(o.wv[i])
			}
			o.wv = nil
			o.wvOffset = 0
			o.Unlock()
			return
		}

		// Swap empty working and pending vectors on the hot path. If a prior
		// write stopped partially, append preserves wire order.
		o.detachPendingNoLock()
		o.Unlock()

		// I/O without lock — wv is only accessed from WriteLoop.
		n := o.writeBuffers()

		// Update pending bytes under lock and wake blocked producers.
		o.Lock()
		o.pb -= n
		if n > 0 {
			o.c.Broadcast()
		}
		o.Unlock()
	}
}

func (o *Outbound) detachPendingNoLock() {
	if len(o.wv) == 0 {
		o.v, o.wv = o.wv[:0], o.v
		return
	}
	o.wv = append(o.wv, o.v...)
	clear(o.v)
	o.v = o.v[:0]
}

func (o *Outbound) EnqueueProto(proto []byte) {
	o.queueOutbound(proto)
}

// EnqueueOwned transfers ownership of a pool-backed buffer to the outbound queue.
// Pool-sized frames retain queue coalescing; larger frames avoid a full copy.
func (o *Outbound) EnqueueOwned(buffer []byte) {
	o.Lock()
	defer o.Unlock()
	if o.IsClosed() || !o.waitForQueueSpaceNoLock(len(buffer)) {
		pool.Put(buffer)
		return
	}
	if cap(buffer) <= pool.SIZE_LARGE {
		o.QueueOutboundNoLock(buffer)
		pool.Put(buffer)
	} else {
		o.pb += int64(len(buffer))
		o.v = append(o.v, buffer)
	}
	o.c.Signal()
}

func (o *Outbound) EnqueueProtoMulti(protos ...[]byte) {
	o.Lock()
	defer o.Unlock()
	if o.IsClosed() {
		return
	}
	total := 0
	for _, proto := range protos {
		total += len(proto)
	}
	if !o.waitForQueueSpaceNoLock(total) {
		return
	}
	for _, proto := range protos {
		o.QueueOutboundNoLock(proto)
	}
	o.c.Signal()
}

// writeBuffers writes wv to the stream without holding the lock.
// Returns the number of bytes written. Only called from WriteLoop.
func (o *Outbound) writeBuffers() int64 {
	if o.str == nil || len(o.wv) == 0 {
		return 0
	}

	startOfWv := o.wv
	start := time.Now()
	maxBatchLen := 0
	var n int64

	for len(o.wv) > 0 {
		batch, usedStorage := o.nextWriteBatch(maxBatchLen)
		if len(batch) == 0 {
			break
		}

		_ = o.str.SetWriteDeadline(start.Add(o.wdl))
		wn, err := batch.WriteTo(o.str)
		_ = o.str.SetWriteDeadline(time.Time{})

		n += wn
		o.consumeWritten(wn)
		if usedStorage {
			clear(o.batchStorage)
			o.batchStorage = o.batchStorage[:0]
		}
		if err == nil {
			continue
		}
		if errors.Is(err, syscall.ENOBUFS) && maxBatchLen != minimumWriteBatchLen {
			if maxBatchLen == 0 {
				maxBatchLen = fallbackWriteBatchLen
			} else {
				maxBatchLen = max(maxBatchLen/2, minimumWriteBatchLen)
			}
			runtime.Gosched()
			continue
		}
		o.l.Error("write buffers", "err", err)
		break
	}

	o.wv = append(startOfWv[:0], o.wv...)
	return n
}

func selectWriteBatch(dst, src net.Buffers, maxBytes int) net.Buffers {
	return selectWriteBatchAtOffset(dst, src, 0, maxBytes)
}

func (o *Outbound) nextWriteBatch(maxBytes int) (net.Buffers, bool) {
	if o.batchStorage == nil {
		o.batchStorage = make(net.Buffers, 0, MaxVectorSize)
	}
	if o.wvOffset == 0 && maxBytes == 0 {
		count := min(len(o.wv), MaxVectorSize)
		o.batchStorage = append(o.batchStorage[:0], o.wv[:count]...)
		return o.batchStorage, true
	}
	o.batchStorage = selectWriteBatchAtOffset(o.batchStorage[:0], o.wv, o.wvOffset, maxBytes)
	return o.batchStorage, true
}

func selectWriteBatchAtOffset(dst, src net.Buffers, firstOffset, maxBytes int) net.Buffers {
	total := 0
	for index, chunk := range src {
		if index == 0 && firstOffset > 0 {
			chunk = chunk[firstOffset:]
		}
		if len(chunk) == 0 {
			continue
		}
		if len(dst) == MaxVectorSize {
			break
		}
		if maxBytes > 0 {
			remaining := maxBytes - total
			if remaining <= 0 {
				break
			}
			if len(chunk) > remaining {
				chunk = chunk[:remaining]
			}
		}
		dst = append(dst, chunk)
		total += len(chunk)
		if maxBytes > 0 && total == maxBytes {
			break
		}
	}
	return dst
}

func (o *Outbound) consumeWritten(written int64) {
	for written > 0 && len(o.wv) > 0 {
		remaining := int64(len(o.wv[0]) - o.wvOffset)
		if written < remaining {
			o.wvOffset += int(written)
			return
		}
		written -= remaining
		pool.Put(o.wv[0])
		o.wv[0] = nil
		o.wv = o.wv[1:]
		o.wvOffset = 0
	}
}

func (o *Outbound) SignalFlush() {
	o.c.Signal()
}

func (o *Outbound) queueOutbound(data []byte) {
	o.Lock()
	defer o.Unlock()
	if o.IsClosed() || !o.waitForQueueSpaceNoLock(len(data)) {
		return
	}
	o.QueueOutboundNoLock(data)
	o.c.Signal()
}

func (o *Outbound) QueueOutboundNoLock(data []byte) {
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

func (o *Outbound) waitForQueueSpaceNoLock(size int) bool {
	limit := int64(maximumPendingBytes)
	if size > minimumWriteBatchLen && size <= pool.SIZE_LARGE {
		limit = maximumMediumFramePendingBytes
	}
	for !o.IsClosed() && o.pb > 0 && o.pb+int64(size) > limit {
		o.c.Wait()
	}
	return !o.IsClosed()
}

// QueueOutboundOwnedMultiNoLock transfers ownership of pool-backed buffers to
// the outbound queue as one frame. The caller must hold o's lock and must not
// retain or return any buffer after this call.
func (o *Outbound) QueueOutboundOwnedMultiNoLock(data ...[]byte) {
	total := 0
	for _, buffer := range data {
		total += len(buffer)
	}
	if !o.waitForQueueSpaceNoLock(total) {
		for _, buffer := range data {
			pool.Put(buffer)
		}
		return
	}
	for _, buffer := range data {
		o.pb += int64(len(buffer))
		o.v = append(o.v, buffer)
	}
}

func (o *Outbound) QueueOutboundByteNoLock(data byte) {
	o.pb++
	new := pool.Get(1)[:1]
	new[0] = data
	o.v = append(o.v, new)
}

func (o *Outbound) IsClosed() bool {
	return o.closed.Load()
}

func (o *Outbound) Close() {
	o.Lock()
	o.closed.Store(true)
	o.c.Broadcast()
	o.Unlock()
}

func (o *Outbound) BroadcastCond() {
	o.Lock()
	o.c.Broadcast()
	o.Unlock()
}
