package server

import "sync"

var bufsPool = sync.Pool{
	New: func() any {
		return [][]byte{}
	},
}

func GetBufs() [][]byte {
	return bufsPool.Get().([][]byte)[:0]
}

func PutBufs(bufs [][]byte) {
	bufsPool.Put(bufs)
}
