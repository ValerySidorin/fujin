package pool

import (
	"sync"
)

const SIZE_BYTE = 1   // for single byte op protocol allocs
const SIZE_4_BYTE = 4 // for uint32 protocol allocs
const SIZE_TINY = 256
const SIZE_SMALL = 512
const SIZE_MEDIUM = 4096
const SIZE_LARGE = 65536

var poolByte = &sync.Pool{
	New: func() any {
		b := [SIZE_BYTE]byte{}
		return &b
	},
}

var pool4Byte = &sync.Pool{
	New: func() any {
		b := [SIZE_4_BYTE]byte{}
		return &b
	},
}

var poolTiny = &sync.Pool{
	New: func() any {
		b := [SIZE_TINY]byte{}
		return &b
	},
}

var poolSmall = &sync.Pool{
	New: func() any {
		b := [SIZE_SMALL]byte{}
		return &b
	},
}

var poolMedium = &sync.Pool{
	New: func() any {
		b := [SIZE_MEDIUM]byte{}
		return &b
	},
}

var poolLarge = &sync.Pool{
	New: func() any {
		b := [SIZE_LARGE]byte{}
		return &b
	},
}

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
	default:
		return poolLarge.Get().(*[SIZE_LARGE]byte)[:0]
	}
}

func Put(b []byte) {
	switch cap(b) {
	case SIZE_BYTE:
		b := (*[SIZE_BYTE]byte)(b[0:SIZE_BYTE])
		poolByte.Put(b)
	case SIZE_4_BYTE:
		b := (*[SIZE_4_BYTE]byte)(b[0:SIZE_4_BYTE])
		pool4Byte.Put(b)
	case SIZE_TINY:
		b := (*[SIZE_TINY]byte)(b[0:SIZE_TINY])
		poolTiny.Put(b)
	case SIZE_SMALL:
		b := (*[SIZE_SMALL]byte)(b[0:SIZE_SMALL])
		poolSmall.Put(b)
	case SIZE_MEDIUM:
		b := (*[SIZE_MEDIUM]byte)(b[0:SIZE_MEDIUM])
		poolMedium.Put(b)
	case SIZE_LARGE:
		b := (*[SIZE_LARGE]byte)(b[0:SIZE_LARGE])
		poolLarge.Put(b)
	default:
	}
}
