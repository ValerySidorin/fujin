//! Fujin embedding facade and public plugin development surface.

mod application;

pub use application::{Application, ApplicationBuilder, ApplicationHandle, RunningApplication};

pub mod connector {
    pub use fujin_connector::{
        AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, CompiledConnector,
        Completion, CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorPlugin,
        ConnectorRuntime, Delivery, Header, Headers, Message, NackEffect, OperationToken, Reader,
        ReaderEvent, ReaderEventSink, ReadyCallback, RouteProfile, SettlementKind,
        SettlementProfile, SettlementResult, Writer,
    };
    pub use fujin_error::{CoreError, Result};
}

pub mod configurator {
    pub use fujin_configurator::{
        ApplyResult, ApplyState, Configurator, ConfiguratorPlugin, ConnectorRuntime,
        ConnectorSnapshot, RuntimeConfig, RuntimeError,
    };
}

pub mod transport {
    pub use fujin_transport::{
        CompiledTransport, InheritedListeners, ListenerMetadata, ListenerRegistry, TransportConfig,
        TransportContext, TransportPlugin, TransportRegistration,
    };
}

pub mod middleware {
    pub mod bind {
        pub use fujin_middleware::{
            BindContext, BindMiddleware, BindMiddlewarePlugin, BindMiddlewareRegistration,
        };
    }

    pub mod connector {
        pub use fujin_connector::CompiledConnectorMiddleware;
        pub use fujin_middleware::{ConnectorMiddlewarePlugin, ConnectorMiddlewareRegistration};
    }
}

pub use fujin_configurator::{
    FujinConfig, GrpcConfig, HealthConfig, RuntimeConfig, RuntimeError, TransportConfig,
};
pub use fujin_runtime::Endpoint;
