package response

type ErrCode byte

const (
	ERR_CODE_NO = iota
	ERR_CODE_YES
)

type RespCode byte

const (

	// Server response opcodes
	RESP_CODE_SUBSCRIBE   RespCode = 1
	RESP_CODE_HSUBSCRIBE  RespCode = 2
	RESP_CODE_PRODUCE     RespCode = 3
	RESP_CODE_HPRODUCE    RespCode = 4
	RESP_CODE_TX_BEGIN    RespCode = 5
	RESP_CODE_TX_COMMIT   RespCode = 6
	RESP_CODE_TX_ROLLBACK RespCode = 7
	RESP_CODE_MSG         RespCode = 8
	RESP_CODE_HMSG        RespCode = 9
	RESP_CODE_FETCH       RespCode = 10
	RESP_CODE_HFETCH      RespCode = 11
	RESP_CODE_ACK         RespCode = 12
	RESP_CODE_NACK        RespCode = 13
	RESP_CODE_UNSUBSCRIBE RespCode = 14

	RESP_CODE_DISCONNECT RespCode = 15

	// Client response opcodes
	RESP_CODE_PONG RespCode = 99
)

var (
	DISCONNECT_RESP = []byte{
		byte(RESP_CODE_DISCONNECT),
	}
)
