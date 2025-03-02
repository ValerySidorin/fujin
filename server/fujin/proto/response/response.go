package response

type ErrCode byte

const (
	ERR_CODE_NO = iota
	ERR_CODE_PARSE_PROTO
	ERR_CODE_PRODUCE
	ERR_CODE_SUBSCRIBE
	ERR_CODE_CONSUME
)

type RespCode byte

const (
	RESP_CODE_UNKNOWN RespCode = iota
	// Client response opcodes
	RESP_CODE_PONG

	// Server response opcodes
	RESP_CODE_OK
	RESP_CODE_ERR
	RESP_CODE_CONNECTED
	RESP_CODE_PRODUCE
	RESP_CODE_MSG
	RESP_CODE_ACK
	RESP_CODE_NACK
	RESP_CODE_TX_BEGIN
	RESP_CODE_TX_COMMIT
	RESP_CODE_TX_ROLLBACK

	RESP_CODE_DISCONNECT
	RESP_CODE_STOP
)

var (
	ERR_RESP = []byte{
		byte(RESP_CODE_ERR), // cmd (4)
	}
	DISCONNECT_RESP = []byte{
		byte(RESP_CODE_DISCONNECT), // cmd (8)
	}
	STOP_RESP = []byte{
		byte(RESP_CODE_STOP), // cmd (9)
	}
)
