package config

import "time"

type GRPCStreamConfig struct {
	RPCWait time.Duration
	Backoff ReconnectBackoff
}

type ReconnectBackoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

type StreamConfig struct {
	GRPC *GRPCStreamConfig
}
