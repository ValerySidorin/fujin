package fujin

import "errors"

var (
	ErrConnClosed = errors.New("connection closed")
	ErrParseProto = errors.New("parse protocol")
)
