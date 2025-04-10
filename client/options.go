package client

import "log/slog"

type Option func(c *Conn)

func WithLogger(l *slog.Logger) Option {
	return func(c *Conn) {
		c.l = l
	}
}
