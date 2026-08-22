//! Statically linked first-party plugins selected by Cargo features.

/// Bootstrap configurators.
pub mod configurator {
    #[cfg(feature = "configurator-env")]
    pub use fujin_runtime::configurator::env_plugin as env;
    #[cfg(feature = "configurator-yaml")]
    pub use fujin_runtime::configurator::yaml_plugin as yaml;
}

/// Connector implementations.
pub mod connector {
    #[cfg(feature = "kafka")]
    pub use fujin_kafka::plugin as kafka;
}

/// Native protocol transports.
pub mod transport {
    #[cfg(feature = "quic")]
    pub use fujin_server::transport::quic_plugin as quic;
    #[cfg(feature = "tcp")]
    pub use fujin_server::transport::tcp_plugin as tcp;
    #[cfg(all(feature = "unix", unix))]
    pub use fujin_server::transport::unix_plugin as unix;
    #[cfg(feature = "websocket")]
    pub use fujin_server::transport::websocket_plugin as websocket;
}

/// Registers every first-party plugin enabled in this build.
#[must_use]
pub fn full(builder: crate::ApplicationBuilder) -> crate::ApplicationBuilder {
    let builder = builder;
    #[cfg(feature = "configurator-env")]
    let builder = builder.configurator(configurator::env());
    #[cfg(feature = "configurator-yaml")]
    let builder = builder.configurator(configurator::yaml());
    #[cfg(feature = "kafka")]
    let builder = builder.connector(connector::kafka());
    #[cfg(feature = "tcp")]
    let builder = builder.transport(transport::tcp());
    #[cfg(all(feature = "unix", unix))]
    let builder = builder.transport(transport::unix());
    #[cfg(feature = "websocket")]
    let builder = builder.transport(transport::websocket());
    #[cfg(feature = "quic")]
    let builder = builder.transport(transport::quic());
    builder
}
