package client

import (
	"context"
	"fmt"
)

type StatusCode byte

const (
	StatusOK StatusCode = iota
	StatusCanceled
	StatusUnknown
	StatusInvalidArgument
	StatusDeadlineExceeded
	StatusNotFound
	StatusAlreadyExists
	StatusPermissionDenied
	StatusResourceExhausted
	StatusFailedPrecondition
	StatusAborted
	StatusOutOfRange
	StatusUnimplemented
	StatusInternal
	StatusUnavailable
	StatusDataLoss
	StatusUnauthenticated
)

type OperationOutcome byte

const (
	OutcomeUnspecified OperationOutcome = iota
	OutcomeNotApplied
	OutcomeApplied
	OutcomeUnknown
)

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
	return fmt.Sprintf("fujin operation failed with status %d", e.Code)
}

type Header struct {
	Key   []byte
	Value []byte
}

type ProduceGuarantee byte

const (
	ProduceGuaranteeUnspecified ProduceGuarantee = iota
	ProduceGuaranteeLocalAccept
	ProduceGuaranteePeerAccept
	ProduceGuaranteeDurableAccept
)

type AckGranularity byte

const (
	AckUnsupported AckGranularity = iota
	AckSingle
	AckCumulative
)

type NackEffect byte

const (
	NackUnsupported NackEffect = iota
	NackRequeue
	NackRelease
	NackDrop
)

type RouteCapabilities struct {
	Produce          bool
	Headers          bool
	Transactions     bool
	Subscribe        bool
	Fetch            bool
	ManualSettlement bool
	ProduceGuarantee ProduceGuarantee
	AckGranularity   AckGranularity
	NackEffect       NackEffect
}

type Message struct {
	SubscriptionID uint32
	MessageID      []byte
	Payload        []byte
	Headers        []Header
}

type SettlementResult struct {
	MessageID []byte
	Error     *OperationError
}

type FetchResult struct {
	SubscriptionID uint32
	Messages       []Message
}

type Conn interface {
	Bind(context.Context, string, ...BindOption) (Stream, error)
	Close() error
}

type Stream interface {
	Routes() map[string]RouteCapabilities
	Produce(context.Context, string, []byte) error
	HProduce(context.Context, string, []byte, []Header) error
	BeginTx(context.Context, string) error
	TxProduce(context.Context, []byte) error
	TxHProduce(context.Context, []byte, []Header) error
	CommitTx(context.Context) error
	RollbackTx(context.Context) error
	Subscribe(context.Context, string, bool, func(Message)) (Subscription, error)
	HSubscribe(context.Context, string, bool, func(Message)) (Subscription, error)
	Fetch(context.Context, string, bool, uint32) (FetchResult, error)
	HFetch(context.Context, string, bool, uint32) (FetchResult, error)
	Unsubscribe(context.Context, uint32) error
	Ack(context.Context, uint32, ...[]byte) ([]SettlementResult, error)
	Nack(context.Context, uint32, ...[]byte) ([]SettlementResult, error)
	Close(context.Context) error
}

type Subscription interface {
	ID() uint32
	Close(context.Context) error
}
