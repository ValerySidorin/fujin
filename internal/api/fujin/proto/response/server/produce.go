package server

import (
	"github.com/ValerySidorin/fujin/internal/api/fujin/proto/response"
)

func ProduceResponse(buf []byte, rID []byte, success bool) []byte {
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, rID...)
	if success {
		buf = append(buf, response.ERR_CODE_NO)
		return buf
	}

	buf = append(buf, response.ERR_CODE_PRODUCE)
	return buf
}
