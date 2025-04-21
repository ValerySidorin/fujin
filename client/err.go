package client

import "errors"

var (
	ErrParseProto    = errors.New("parse proto")
	ErrEmptyTopic    = errors.New("empty topic")
	ErrEmptyPoolSize = errors.New("empty pool size")
)
