//! Stable public contracts for statically linked Fujin plugins.
//!
//! Third-party plugins are ordinary Rust crates. They implement one of these contracts, expose a
//! small `plugin()` constructor, and are registered explicitly with `fujin::ApplicationBuilder`.
//! No process-global registration or Rust trait objects cross a dynamic-library boundary.

/// Connector descriptors, runtimes, readers, writers, and route capabilities.
pub mod connector {
    pub use fujin_core::{
        AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, CompiledConnector,
        Completion, CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorPlugin,
        ConnectorRuntime, CoreError, Delivery, Header, Headers, Message, NackEffect,
        OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback, Result, RouteProfile,
        SettlementKind, SettlementProfile, SettlementResult, Writer,
    };
}

/// Bootstrap configuration and runtime connector snapshot contracts.
pub mod configurator {
    pub use fujin_runtime::configurator::{
        ApplyResult, ApplyState, Configurator, ConfiguratorPlugin, ConnectorRuntime,
        ConnectorSnapshot,
    };
    pub use fujin_runtime::{RuntimeConfig, RuntimeError};
}

/// Native-protocol transport contracts.
pub mod transport {
    pub use fujin_transport::{
        CompiledTransport, TransportConfig, TransportContext, TransportPlugin,
        TransportRegistration,
    };
    pub use fujin_upgrade::{InheritedListeners, ListenerMetadata, ListenerRegistry};
}

/// Middleware plugin contracts, separated by lifecycle and direction.
pub mod middleware {
    /// Middleware invoked while establishing a BIND.
    pub mod bind {
        pub use fujin_core::{
            BindContext, BindMiddleware, BindMiddlewarePlugin, BindMiddlewareRegistration,
        };
    }

    /// Generation-scoped middleware wrapping connector reader and writer leases.
    pub mod connector {
        pub use fujin_core::{
            CompiledConnectorMiddleware, ConnectorMiddlewarePlugin, ConnectorMiddlewareRegistration,
        };
    }
}

pub use configurator::ConfiguratorPlugin;
pub use connector::ConnectorPlugin;
pub use fujin_transport::TransportRegistration;
