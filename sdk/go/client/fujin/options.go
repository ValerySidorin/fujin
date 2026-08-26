package fujin

import (
	"log/slog"
	"time"
)

type Option func(c *Conn)

func WithLogger(l *slog.Logger) Option {
	return func(c *Conn) {
		c.l = l
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *Conn) {
		c.timeout = timeout
	}
}

func WithWriteDeadline(wdl time.Duration) Option {
	return func(c *Conn) {
		c.wdl = wdl
	}
}
