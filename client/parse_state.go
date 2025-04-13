package client

const (
	OP_START int = iota

	OP_WRITE
	OP_WRITE_CORRELATION_ID_ARG

	OP_ERROR_CODE_ARG
	OP_ERROR_PAYLOAD_ARG
	OP_ERROR_PAYLOAD

	OP_DISCONNECT
)

type parseState struct {
	state      int
	argBuf     []byte
	payloadBuf []byte

	wma writeMessageArg

	ca correlationIDArg
}

type correlationIDArg struct {
	cID []byte
}

type writeMessageArg struct {
	errLen uint32
}
