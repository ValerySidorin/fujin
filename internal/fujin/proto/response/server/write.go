package server

import (
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
)

func WriteResponseSuccess(buf []byte, cID []byte) []byte {
	buf = append(buf, byte(response.RESP_CODE_WRITE))
	buf = append(buf, cID...)
	return append(buf, response.ERR_CODE_NO)
}
