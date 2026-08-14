package proto

import "sync"

var bufsPool = sync.Pool{
	New: func() any {
		return [][]byte{}
	},
}

const inlineBufs = 256

type bufsLease struct {
	bufs     [][]byte
	inline   [inlineBufs][]byte
	overflow [][]byte
}

var bufsLeasePool = sync.Pool{
	New: func() any {
		return new(bufsLease)
	},
}

func prepareBufsLease(lease *bufsLease, size int) {
	if size <= len(lease.inline) {
		lease.bufs = lease.inline[:0]
		return
	}
	if cap(lease.overflow) < size {
		lease.overflow = make([][]byte, 0, size)
	}
	lease.bufs = lease.overflow[:0]
}

func (h *Handler) getFetchBufs(size int) *bufsLease {
	h.fetchBufsMu.Lock()
	lease := h.fetchBufs
	h.fetchBufs = nil
	h.fetchBufsMu.Unlock()
	if lease == nil {
		lease = bufsLeasePool.Get().(*bufsLease)
	}
	prepareBufsLease(lease, size)
	return lease
}

func (h *Handler) putFetchBufs(lease *bufsLease) {
	clear(lease.bufs)
	lease.bufs = nil
	h.fetchBufsMu.Lock()
	if h.fetchBufs == nil {
		h.fetchBufs = lease
		lease = nil
	}
	h.fetchBufsMu.Unlock()
	if lease != nil {
		bufsLeasePool.Put(lease)
	}
}

func GetBufs() [][]byte {
	return bufsPool.Get().([][]byte)[:0]
}

func PutBufs(bufs [][]byte) {
	bufsPool.Put(bufs)
}
