package client

const (
	OP_START int = iota

	// Writer
	OP_WRITE
	OP_CORRELATION_ID_ARG

	OP_TX_BEGIN
	OP_TX_COMMIT
	OP_TX_ROLLBACK

	// Reader
	OP_CONNECT_READER
	OP_MSG
	OP_MSG_META_ARG
	OP_MSG_ARG
	OP_MSG_PAYLOAD

	// Common
	OP_ERROR_CODE_ARG
	OP_ERROR_PAYLOAD_ARG
	OP_ERROR_PAYLOAD

	OP_DISCONNECT
)

type parseState struct {
	state      int
	argBuf     []byte
	metaBuf    []byte
	payloadBuf []byte

	ea  errArg
	cra connectReaderArg
	ma  msgArg
	ca  correlationIDArg
}

type correlationIDArg struct {
	cID []byte
}

type connectReaderArg struct {
	msgMetaLen byte
}

type msgArg struct {
	len uint32
}

type errArg struct {
	errLen uint32
}
