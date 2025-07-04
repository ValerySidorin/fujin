package request

type OpCode byte

const (
	OP_CODE_UNKNOWN OpCode = iota

	// Client request opcodes
	OP_CODE_CONNECT
	OP_CODE_PRODUCE
	OP_CODE_TX_BEGIN
	OP_CODE_TX_COMMIT
	OP_CODE_TX_ROLLBACK
	OP_CODE_FETCH
	OP_CODE_ACK
	OP_CODE_NACK
	OP_CODE_SUBSCRIBE
	OP_CODE_UNSUBSCRIBE
	OP_CODE_DISCONNECT

	// Server request opcodes
	OP_CODE_PING
	OP_CODE_STOP
)

var (
	STOP_REQ = []byte{
		byte(OP_CODE_STOP),
	}

	PING_REQ = []byte{
		byte(OP_CODE_PING),
	}
)
