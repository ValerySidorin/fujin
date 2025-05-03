package client

import "errors"

var (
	ErrConnClosed = errors.New("connection closed")
	ErrTimeout    = errors.New("timeout")

	ErrParseProto    = errors.New("parse proto")
	ErrEmptyTopic    = errors.New("empty topic")
	ErrEmptyPoolSize = errors.New("empty pool size")

	ErrInvalidAckMsgResponseNum = errors.New("invalid number of ack msg responses")
)
