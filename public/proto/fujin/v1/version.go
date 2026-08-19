package v1

import "strconv"

// WireVersion identifies one immutable native wire contract.
type WireVersion byte

const (
	// Protocol is the version-independent application protocol identifier used by
	// transports such as QUIC ALPN. Wire compatibility is negotiated by HELLO.
	Protocol = "fujin"
	// Version is the exact wire contract implemented by this package.
	Version WireVersion = 1
	// HelloFormat is the framing version of the version-negotiation exchange.
	HelloFormat byte = 1
)

// String returns the human-readable protocol label for diagnostics.
func (v WireVersion) String() string {
	return Protocol + "/" + strconv.Itoa(int(v))
}
