package pool

import (
	"sync"
)

const SIZE_BYTE = 1 // for single byte op protocol allocs
const (
	SIZE_4_BYTE = 4 // for uint32 protocol allocs
	SIZE_TINY   = 256
	SIZE_SMALL  = 512
	SIZE_MEDIUM = 4 * 1024
	SIZE_LARGE  = 64 * 1024

	payloadSize128K = 128 * 1024
	payloadSize256K = 256 * 1024
	payloadSize512K = 512 * 1024
	payloadSize1M   = 1024 * 1024
)

var poolByte = &sync.Pool{New: func() any { b := [SIZE_BYTE]byte{}; return &b }}
var pool4Byte = &sync.Pool{New: func() any { b := [SIZE_4_BYTE]byte{}; return &b }}
var poolTiny = &sync.Pool{New: func() any { b := [SIZE_TINY]byte{}; return &b }}
var poolSmall = &sync.Pool{New: func() any { b := [SIZE_SMALL]byte{}; return &b }}
var poolMedium = &sync.Pool{New: func() any { b := [SIZE_MEDIUM]byte{}; return &b }}
var poolLarge = &sync.Pool{New: func() any { b := [SIZE_LARGE]byte{}; return &b }}
var poolPayload128K = &sync.Pool{New: func() any { b := [payloadSize128K]byte{}; return &b }}
var poolPayload256K = &sync.Pool{New: func() any { b := [payloadSize256K]byte{}; return &b }}
var poolPayload512K = &sync.Pool{New: func() any { b := [payloadSize512K]byte{}; return &b }}
var poolPayload1M = &sync.Pool{New: func() any { b := [payloadSize1M]byte{}; return &b }}

func Get(sz int) []byte {
	switch {
	case sz <= SIZE_BYTE:
		return poolByte.Get().(*[SIZE_BYTE]byte)[:0]
	case sz <= SIZE_4_BYTE:
		return pool4Byte.Get().(*[SIZE_4_BYTE]byte)[:0]
	case sz <= SIZE_TINY:
		return poolTiny.Get().(*[SIZE_TINY]byte)[:0]
	case sz <= SIZE_SMALL:
		return poolSmall.Get().(*[SIZE_SMALL]byte)[:0]
	case sz <= SIZE_MEDIUM:
		return poolMedium.Get().(*[SIZE_MEDIUM]byte)[:0]
	case sz <= SIZE_LARGE:
		return poolLarge.Get().(*[SIZE_LARGE]byte)[:0]
	default:
		return make([]byte, 0, sz)
	}
}

// GetPayload returns a buffer suitable for retaining an inbound produce payload.
// Buffers larger than 1 MiB are intentionally not pooled.
func GetPayload(sz int) []byte {
	switch {
	case sz <= SIZE_LARGE:
		return Get(sz)
	case sz <= payloadSize128K:
		return poolPayload128K.Get().(*[payloadSize128K]byte)[:0]
	case sz <= payloadSize256K:
		return poolPayload256K.Get().(*[payloadSize256K]byte)[:0]
	case sz <= payloadSize512K:
		return poolPayload512K.Get().(*[payloadSize512K]byte)[:0]
	case sz <= payloadSize1M:
		return poolPayload1M.Get().(*[payloadSize1M]byte)[:0]
	default:
		return make([]byte, 0, sz)
	}
}

func Put(b []byte) {
	switch cap(b) {
	case SIZE_BYTE:
		poolByte.Put((*[SIZE_BYTE]byte)(b[:SIZE_BYTE]))
	case SIZE_4_BYTE:
		pool4Byte.Put((*[SIZE_4_BYTE]byte)(b[:SIZE_4_BYTE]))
	case SIZE_TINY:
		poolTiny.Put((*[SIZE_TINY]byte)(b[:SIZE_TINY]))
	case SIZE_SMALL:
		poolSmall.Put((*[SIZE_SMALL]byte)(b[:SIZE_SMALL]))
	case SIZE_MEDIUM:
		poolMedium.Put((*[SIZE_MEDIUM]byte)(b[:SIZE_MEDIUM]))
	case SIZE_LARGE:
		poolLarge.Put((*[SIZE_LARGE]byte)(b[:SIZE_LARGE]))
	}
}

// PutPayload returns a buffer acquired with GetPayload after its owner releases it.
func PutPayload(b []byte) {
	switch cap(b) {
	case payloadSize128K:
		poolPayload128K.Put((*[payloadSize128K]byte)(b[:payloadSize128K]))
	case payloadSize256K:
		poolPayload256K.Put((*[payloadSize256K]byte)(b[:payloadSize256K]))
	case payloadSize512K:
		poolPayload512K.Put((*[payloadSize512K]byte)(b[:payloadSize512K]))
	case payloadSize1M:
		poolPayload1M.Put((*[payloadSize1M]byte)(b[:payloadSize1M]))
	default:
		Put(b)
	}
}
