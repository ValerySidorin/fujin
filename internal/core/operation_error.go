package core

import (
	"context"
	"errors"
	"io"
	"net"

	commonpool "github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/public/plugins/connector"
	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
)

// StatusCode classifies an operation failure independently of its transport.
// Numeric values match the canonical gRPC status codes and the native protocol.
type StatusCode byte

const (
	StatusOK                 StatusCode = 0
	StatusCanceled           StatusCode = 1
	StatusUnknown            StatusCode = 2
	StatusInvalidArgument    StatusCode = 3
	StatusDeadlineExceeded   StatusCode = 4
	StatusNotFound           StatusCode = 5
	StatusAlreadyExists      StatusCode = 6
	StatusPermissionDenied   StatusCode = 7
	StatusResourceExhausted  StatusCode = 8
	StatusFailedPrecondition StatusCode = 9
	StatusAborted            StatusCode = 10
	StatusOutOfRange         StatusCode = 11
	StatusUnimplemented      StatusCode = 12
	StatusInternal           StatusCode = 13
	StatusUnavailable        StatusCode = 14
	StatusDataLoss           StatusCode = 15
	StatusUnauthenticated    StatusCode = 16
)

// OperationOutcome describes whether a failed state-changing operation took effect.
type OperationOutcome byte

const (
	OutcomeUnspecified OperationOutcome = iota
	OutcomeNotApplied
	OutcomeApplied
	OutcomeUnknown
)

// OperationError is the transport-neutral client-visible failure envelope.
type OperationError struct {
	Code    StatusCode
	Outcome OperationOutcome
	Reason  string
	Message string
	Details map[string]string
}

// ClassifyError converts domain and connector failures to the shared wire contract.
func ClassifyError(err error) OperationError {
	if err == nil {
		return OperationError{Code: StatusOK}
	}

	classified := OperationError{
		Code:    StatusUnknown,
		Outcome: OutcomeUnknown,
		Reason:  "UNKNOWN",
		Message: err.Error(),
	}

	switch {
	case errors.Is(err, context.Canceled):
		classified.Code = StatusCanceled
		classified.Reason = "CANCELED"
	case errors.Is(err, context.DeadlineExceeded):
		classified.Code = StatusDeadlineExceeded
		classified.Reason = "DEADLINE_EXCEEDED"
	case errors.Is(err, bmw.ErrUnauthenticated):
		classified.Code = StatusUnauthenticated
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "UNAUTHENTICATED"
	case errors.Is(err, bmw.ErrPermissionDenied):
		classified.Code = StatusPermissionDenied
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "PERMISSION_DENIED"
	case errors.Is(err, ErrConnectorNotFound):
		classified.Code = StatusNotFound
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "CONNECTOR_NOT_FOUND"
	case errors.Is(err, connector.ErrRouteNotFound):
		classified.Code = StatusNotFound
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "ROUTE_NOT_FOUND"
	case errors.Is(err, ErrSubscriptionNotFound):
		classified.Code = StatusNotFound
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "SUBSCRIPTION_NOT_FOUND"
	case errors.Is(err, ErrInvalidBatchSize):
		classified.Code = StatusInvalidArgument
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "INVALID_BATCH_SIZE"
	case errors.Is(err, ErrInvalidMessageID):
		classified.Code = StatusInvalidArgument
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "INVALID_MESSAGE_ID"
	case errors.Is(err, connector.ErrInvalidHeaders):
		classified.Code = StatusInvalidArgument
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "INVALID_HEADERS"
	case errors.Is(err, connector.ErrOperationUnsupported):
		classified.Code = StatusUnimplemented
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "OPERATION_UNSUPPORTED"
	case errors.Is(err, ErrFetchBusy):
		classified.Code = StatusAborted
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "FETCH_BUSY"
	case errors.Is(err, commonpool.ErrBytePoolExhausted):
		classified.Code = StatusResourceExhausted
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "SUBSCRIPTION_IDS_EXHAUSTED"
	case errors.Is(err, ErrCommitOutcomeUnknown):
		classified.Code = StatusUnknown
		classified.Outcome = OutcomeUnknown
		classified.Reason = "COMMIT_OUTCOME_UNKNOWN"
	case errors.Is(err, ErrTransactionAborted):
		classified.Code = StatusAborted
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "TRANSACTION_ABORTED"
	case errors.Is(err, ErrSubscriptionEnded):
		classified.Code = StatusUnavailable
		classified.Outcome = OutcomeUnknown
		classified.Reason = "SUBSCRIPTION_ENDED"
	case errors.Is(err, ErrNotBound):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "NOT_BOUND"
	case errors.Is(err, ErrAlreadyBound):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "ALREADY_BOUND"
	case errors.Is(err, ErrTransactionActive):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "TRANSACTION_ACTIVE"
	case errors.Is(err, ErrNoTransaction):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "NO_TRANSACTION"
	case errors.Is(err, ErrNormalProduceInTransaction):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "TRANSACTION_COMMAND_REQUIRED"
	case errors.Is(err, ErrClosed), errors.Is(err, connector.ErrWriterClosed):
		classified.Code = StatusFailedPrecondition
		classified.Outcome = OutcomeNotApplied
		classified.Reason = "SESSION_CLOSED"
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		classified.Code = StatusUnavailable
		classified.Reason = "UNAVAILABLE"
	default:
		var networkError net.Error
		if errors.As(err, &networkError) {
			if networkError.Timeout() {
				classified.Code = StatusDeadlineExceeded
				classified.Reason = "DEADLINE_EXCEEDED"
			} else {
				classified.Code = StatusUnavailable
				classified.Reason = "UNAVAILABLE"
			}
		}
	}
	return classified
}
