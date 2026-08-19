// Package session provides the protocol session API for Fujin transport plugins.
// Transport plugins accept connections, obtain a Stream (e.g. net.Conn or quic.Stream),
// and call HandleStream to run the Fujin binary protocol.
package session

import (
	"io"
	"time"
)

// Stream is a transport-agnostic bidirectional byte stream.
type Stream interface {
	io.ReadWriteCloser
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}
