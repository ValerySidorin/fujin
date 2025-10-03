package server

import (
	"github.com/ValerySidorin/fujin/internal/api/fujin/proto/response"
)

func ProduceResponseSuccess(buf []byte, cID []byte) []byte {
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, cID...)
	return append(buf, response.ERR_CODE_NO)
}
