use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

/// Canonical status values shared by native v1 and gRPC.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum StatusCode {
    #[default]
    Ok = 0,
    Canceled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OperationOutcome {
    #[default]
    Unspecified = 0,
    NotApplied = 1,
    Applied = 2,
    Unknown = 3,
}

/// Transport-neutral client-visible failure envelope.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationError {
    pub code: StatusCode,
    pub outcome: OperationOutcome,
    pub reason: String,
    pub message: String,
    pub details: BTreeMap<String, String>,
}

impl OperationError {
    pub fn new(
        code: StatusCode,
        outcome: OperationOutcome,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            outcome,
            reason: reason.into(),
            message: message.into(),
            details: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoreError {
    #[error("not bound")]
    NotBound,
    #[error("already bound")]
    AlreadyBound,
    #[error("connector not found: {0}")]
    ConnectorNotFound(String),
    #[error("route not found: {0}")]
    RouteNotFound(String),
    #[error("operation unsupported")]
    OperationUnsupported,
    #[error("fetch already active")]
    FetchBusy,
    #[error("fetch batch size must be positive")]
    InvalidBatchSize,
    #[error("subscription IDs exhausted")]
    SubscriptionIdsExhausted,
    #[error("invalid message ID: {0}")]
    InvalidMessageId(String),
    #[error("invalid headers: {0}")]
    InvalidHeaders(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("transaction already active")]
    TransactionActive,
    #[error("no transaction active")]
    NoTransaction,
    #[error("normal produce is not allowed in a transaction")]
    TransactionCommandRequired,
    #[error("transaction aborted: {0}")]
    TransactionAborted(String),
    #[error("transaction commit outcome unknown: {0}")]
    CommitOutcomeUnknown(String),
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(u8),
    #[error("subscription ended: {0}")]
    SubscriptionEnded(String),
    #[error("session closed")]
    Closed,
    #[error("authentication required: {0}")]
    Unauthenticated(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("connector unavailable: {0}")]
    Unavailable(String),
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("internal error: {0}")]
    Internal(String),
}

fn classify(error: &CoreError) -> (StatusCode, OperationOutcome, &'static str) {
    use OperationOutcome::{NotApplied, Unknown};
    use StatusCode::{
        Aborted, FailedPrecondition, Internal, InvalidArgument, NotFound, PermissionDenied,
        ResourceExhausted, Unauthenticated, Unavailable, Unimplemented,
    };

    match error {
        CoreError::NotBound => (FailedPrecondition, NotApplied, "NOT_BOUND"),
        CoreError::AlreadyBound => (FailedPrecondition, NotApplied, "ALREADY_BOUND"),
        CoreError::ConnectorNotFound(_) => (NotFound, NotApplied, "CONNECTOR_NOT_FOUND"),
        CoreError::RouteNotFound(_) => (NotFound, NotApplied, "ROUTE_NOT_FOUND"),
        CoreError::OperationUnsupported => (Unimplemented, NotApplied, "OPERATION_UNSUPPORTED"),
        CoreError::FetchBusy => (Aborted, NotApplied, "FETCH_BUSY"),
        CoreError::InvalidBatchSize => (InvalidArgument, NotApplied, "INVALID_BATCH_SIZE"),
        CoreError::SubscriptionIdsExhausted => {
            (ResourceExhausted, NotApplied, "SUBSCRIPTION_IDS_EXHAUSTED")
        }
        CoreError::InvalidMessageId(_) => (InvalidArgument, NotApplied, "INVALID_MESSAGE_ID"),
        CoreError::InvalidHeaders(_) => (InvalidArgument, NotApplied, "INVALID_HEADERS"),
        CoreError::InvalidConfig(_) => (InvalidArgument, NotApplied, "INVALID_CONFIGURATION"),
        CoreError::TransactionActive => (FailedPrecondition, NotApplied, "TRANSACTION_ACTIVE"),
        CoreError::NoTransaction => (FailedPrecondition, NotApplied, "NO_TRANSACTION"),
        CoreError::TransactionCommandRequired => (
            FailedPrecondition,
            NotApplied,
            "TRANSACTION_COMMAND_REQUIRED",
        ),
        CoreError::TransactionAborted(_) => (Aborted, NotApplied, "TRANSACTION_ABORTED"),
        CoreError::CommitOutcomeUnknown(_) => {
            (StatusCode::Unknown, Unknown, "COMMIT_OUTCOME_UNKNOWN")
        }
        CoreError::SubscriptionNotFound(_) => (NotFound, NotApplied, "SUBSCRIPTION_NOT_FOUND"),
        CoreError::Closed => (FailedPrecondition, NotApplied, "SESSION_CLOSED"),
        CoreError::Unauthenticated(_) => (Unauthenticated, NotApplied, "UNAUTHENTICATED"),
        CoreError::PermissionDenied(_) => (PermissionDenied, NotApplied, "PERMISSION_DENIED"),
        CoreError::SubscriptionEnded(_) => (Unavailable, Unknown, "SUBSCRIPTION_ENDED"),
        CoreError::ResourceExhausted(_) => (ResourceExhausted, Unknown, "RESOURCE_EXHAUSTED"),
        CoreError::Unavailable(_) => (Unavailable, Unknown, "UNAVAILABLE"),
        CoreError::Internal(_) => (Internal, Unknown, "INTERNAL"),
    }
}

impl From<&CoreError> for OperationError {
    fn from(error: &CoreError) -> Self {
        let (code, outcome, reason) = classify(error);
        Self::new(code, outcome, reason, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_matches_wire_contract() {
        let classified = OperationError::from(&CoreError::RouteNotFound("orders".into()));
        assert_eq!(classified.code, StatusCode::NotFound);
        assert_eq!(classified.outcome, OperationOutcome::NotApplied);
        assert_eq!(classified.reason, "ROUTE_NOT_FOUND");
    }
}
