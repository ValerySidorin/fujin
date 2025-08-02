package client

import (
	"encoding/binary"

	"github.com/ValerySidorin/fujin/internal/fujin"
)

func (s *Stream) parseErrLenArg() error {
	s.ps.ea.errLen = binary.BigEndian.Uint32(s.ps.argBuf[0:fujin.Uint32Len])
	if s.ps.ea.errLen == 0 {
		return ErrParseProto
	}

	return nil
}

func (s *Stream) parseMsgLenArg() error {
	s.ps.ma.len = binary.BigEndian.Uint32(s.ps.argBuf[0:fujin.Uint32Len])
	if s.ps.ma.len == 0 {
		return ErrParseProto
	}

	return nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func byteToBool(b byte) bool {
	if b <= 0 {
		return false
	}

	return true
}
