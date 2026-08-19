package v1

// StatusCode classifies operation results. Values match gRPC status codes.
type StatusCode byte

const (
	STATUS_OK                  StatusCode = 0
	STATUS_CANCELED            StatusCode = 1
	STATUS_UNKNOWN             StatusCode = 2
	STATUS_INVALID_ARGUMENT    StatusCode = 3
	STATUS_DEADLINE_EXCEEDED   StatusCode = 4
	STATUS_NOT_FOUND           StatusCode = 5
	STATUS_ALREADY_EXISTS      StatusCode = 6
	STATUS_PERMISSION_DENIED   StatusCode = 7
	STATUS_RESOURCE_EXHAUSTED  StatusCode = 8
	STATUS_FAILED_PRECONDITION StatusCode = 9
	STATUS_ABORTED             StatusCode = 10
	STATUS_OUT_OF_RANGE        StatusCode = 11
	STATUS_UNIMPLEMENTED       StatusCode = 12
	STATUS_INTERNAL            StatusCode = 13
	STATUS_UNAVAILABLE         StatusCode = 14
	STATUS_DATA_LOSS           StatusCode = 15
	STATUS_UNAUTHENTICATED     StatusCode = 16
)

type OperationOutcome byte

const (
	OUTCOME_UNSPECIFIED OperationOutcome = iota
	OUTCOME_NOT_APPLIED
	OUTCOME_APPLIED
	OUTCOME_UNKNOWN
)

// OperationError is the structured failure envelope carried by native responses.
type OperationError struct {
	Code    StatusCode
	Outcome OperationOutcome
	Reason  string
	Message string
	Details map[string]string
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Reason != "" {
		return e.Reason
	}
	return "operation failed"
}

// Route capability bits carried by a successful BIND response.
const (
	ROUTE_CAP_PRODUCE byte = 1 << iota
	ROUTE_CAP_HEADERS
	ROUTE_CAP_TRANSACTIONS
	ROUTE_CAP_SUBSCRIBE
	ROUTE_CAP_FETCH
	ROUTE_CAP_MANUAL_SETTLEMENT
)

type ProduceGuarantee byte

const (
	PRODUCE_GUARANTEE_UNSPECIFIED ProduceGuarantee = iota
	PRODUCE_GUARANTEE_LOCAL_ACCEPT
	PRODUCE_GUARANTEE_PEER_ACCEPT
	PRODUCE_GUARANTEE_DURABLE_ACCEPT
)

type AckGranularity byte

const (
	ACK_GRANULARITY_UNSUPPORTED AckGranularity = iota
	ACK_GRANULARITY_SINGLE
	ACK_GRANULARITY_CUMULATIVE
)

type NackEffect byte

const (
	NACK_EFFECT_UNSUPPORTED NackEffect = iota
	NACK_EFFECT_REQUEUE
	NACK_EFFECT_RELEASE
	NACK_EFFECT_DROP
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
	RESP_CODE_DISCONNECT  RespCode = 15
	RESP_CODE_BIND        RespCode = 16
	RESP_CODE_TX_PRODUCE  RespCode = 17
	RESP_CODE_TX_HPRODUCE RespCode = 18
	RESP_CODE_HELLO       RespCode = 19

	// Client response opcodes
	RESP_CODE_PONG RespCode = 99
)

var (
	DISCONNECT_RESP = []byte{
		byte(RESP_CODE_DISCONNECT),
	}
)
