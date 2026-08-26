//! Public connector contracts, configuration, and immutable connector generations.

mod config;
mod contract;
mod generation;
mod middleware;
mod overrides;
mod writer_contract;

pub use config::{ConnectorConfig, ConnectorsConfig, MiddlewareConfig};
pub use contract::{
    AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, CompiledConnector, Completion,
    CompletionSink, ConnectorDescriptor, ConnectorRuntime, Delivery, Header, Headers, Message,
    NackEffect, OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback, RouteProfile,
    SettlementKind, SettlementProfile, SettlementResult, Writer, validate_headers,
};
pub use fujin_error::{CoreError, Result};
pub use generation::{
    Binding, Catalog, CatalogStatus, ConnectorPlugin, ConnectorRegistry, Generation,
    GenerationCompiler, GenerationState, GenerationStatus, GenerationTransition,
};
pub use middleware::{
    CompiledConnectorMiddleware, ConnectorMiddlewareCompiler, NoConnectorMiddleware,
};
pub use overrides::{apply_overrides, validate_override_path};
