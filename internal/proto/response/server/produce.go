package server

import (
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
)

func ProduceResponseSuccess(buf []byte, cID []byte) []byte {
	buf = append(buf, byte(v1.RESP_CODE_PRODUCE))
	buf = append(buf, cID...)
	return append(buf, byte(v1.STATUS_OK))
}
