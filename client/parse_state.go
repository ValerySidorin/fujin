package client

const (
	OP_START int = iota

	// Writer
	OP_WRITE

	OP_TX_BEGIN
	OP_TX_COMMIT
	OP_TX_ROLLBACK

	// Reader
	OP_CONNECT_READER
	OP_MSG
	OP_MSG_META_ARG
	OP_MSG_ARG
	OP_MSG_PAYLOAD

	OP_ACK
	OP_NACK

	OP_FETCH
	OP_FETCH_BATCH_NUM_ARG

	OP_FETCH_CORRELATION_ID_ARG
	OP_FETCH_ERROR_CODE_ARG
	OP_FETCH_ERROR_PAYLOAD_ARG
	OP_FETCH_ERROR_PAYLOAD

	// Common
	OP_CORRELATION_ID_ARG
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
	fa  fetchArg
	ca  correlationIDArg
}

type correlationIDArg struct {
	cID       []byte
	cIDUint32 uint32
}

type connectReaderArg struct {
	msgMetaLen byte
}

type msgArg struct {
	len uint32
}

type fetchArg struct {
	n       uint32
	handled uint32
	msgs    []Msg
	err     chan error
}

type errArg struct {
	errLen uint32
}
