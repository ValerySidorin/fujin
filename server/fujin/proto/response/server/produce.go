package server

import (
	"github.com/ValerySidorin/fujin/server/fujin/proto/response"
)

func ProduceResponseSuccess(buf []byte, rID []byte) []byte {
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, rID...)
	return append(buf, response.ERR_CODE_NO)
}
