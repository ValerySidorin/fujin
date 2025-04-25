package response

type ErrCode byte

const (
	ERR_CODE_NO = iota
	ERR_CODE_YES
)

type RespCode byte

const (
	RESP_CODE_UNKNOWN RespCode = iota

	// Server response opcodes
	RESP_CODE_CONNECT_READER
	RESP_CODE_WRITE
	RESP_CODE_TX_BEGIN
	RESP_CODE_TX_COMMIT
	RESP_CODE_TX_ROLLBACK
	RESP_CODE_MSG
	RESP_CODE_FETCH
	RESP_CODE_ACK
	RESP_CODE_NACK

	RESP_CODE_DISCONNECT

	// Client response opcodes
	RESP_CODE_PONG
)

var (
	DISCONNECT_RESP = []byte{
		byte(RESP_CODE_DISCONNECT),
	}
)
