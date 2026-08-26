//! Transport-neutral contracts and session semantics for Fujin.

pub mod session;

pub(crate) use fujin_connector::{
    AckGranularity, Binding, Capabilities, Catalog, Completion, CompletionSink, Delivery, Message,
    NackEffect, OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback, RouteProfile,
    SettlementKind, SettlementResult, Writer, validate_headers,
};
pub(crate) use fujin_error::{CoreError, Result};
pub(crate) use fujin_middleware::BindMiddlewareRunner;
pub use session::{
    BindResult, FetchResult, NoSessionEvents, SessionCore, SessionEventSink, SessionState,
};
