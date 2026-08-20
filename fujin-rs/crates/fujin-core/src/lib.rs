//! Transport-neutral contracts and session semantics for Fujin.

pub mod config;
pub mod connector;
pub mod error;
pub mod generation;
pub mod middleware;
pub mod overrides;
pub mod session;
mod writer_contract;

pub use config::{ConnectorConfig, ConnectorsConfig, MiddlewareConfig};
pub use connector::{
    AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, CompiledConnector, Completion,
    CompletionSink, ConnectorDescriptor, ConnectorRuntime, Header, Headers, Message, NackEffect,
    OperationToken, Reader, ReaderEvent, ReaderEventSink, ReaderMessage, ReadyCallback,
    RouteProfile, SettlementKind, SettlementProfile, Writer,
};
pub use error::{CoreError, OperationError, OperationOutcome, Result, StatusCode};
pub use generation::{Binding, Catalog, DescriptorRegistry, Generation, GenerationCompiler};
pub use middleware::{
    BindContext, BindMiddleware, BindMiddlewareRunner, CompiledConnectorMiddleware,
    ConnectorMiddlewareCompiler, NoBindMiddleware, NoConnectorMiddleware,
};
pub use overrides::{apply_overrides, validate_override_path};
pub use session::{
    BindResult, Delivery, FetchResult, NoSessionEvents, SessionCore, SessionEventSink,
    SessionState, SettlementResult,
};
