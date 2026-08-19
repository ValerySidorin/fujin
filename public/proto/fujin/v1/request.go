package v1

type OpCode byte

const (
	OP_CODE_BIND        OpCode = 1
	OP_CODE_PRODUCE     OpCode = 2
	OP_CODE_HPRODUCE    OpCode = 3
	OP_CODE_TX_BEGIN    OpCode = 4
	OP_CODE_TX_COMMIT   OpCode = 5
	OP_CODE_TX_ROLLBACK OpCode = 6
	OP_CODE_FETCH       OpCode = 7
	OP_CODE_HFETCH      OpCode = 8
	OP_CODE_ACK         OpCode = 9
	OP_CODE_NACK        OpCode = 10
	OP_CODE_SUBSCRIBE   OpCode = 11
	OP_CODE_HSUBSCRIBE  OpCode = 12
	OP_CODE_UNSUBSCRIBE OpCode = 13
	OP_CODE_DISCONNECT  OpCode = 14
	OP_CODE_TX_PRODUCE  OpCode = 15
	OP_CODE_TX_HPRODUCE OpCode = 16

	// Server request opcodes
	OP_CODE_STOP OpCode = 98
	OP_CODE_PING OpCode = 99
)

var (
	STOP_REQ = []byte{
		byte(OP_CODE_STOP),
	}

	PING_REQ = []byte{
		byte(OP_CODE_PING),
	}
)
