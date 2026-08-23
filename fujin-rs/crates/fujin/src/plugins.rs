//! Optional plugin dependencies selected by the final application build.

/// Bootstrap configurators.
pub mod configurator {
    #[cfg(feature = "configurator-env")]
    pub use fujin_configurator_env::{EnvConfigurator, plugin as env};
    #[cfg(feature = "configurator-yaml")]
    pub use fujin_configurator_yaml::{YamlConfigurator, plugin as yaml};
}

/// Connector implementations.
pub mod connector {
    #[cfg(feature = "kafka")]
    pub use fujin_connector_kafka::plugin as kafka;
}

/// Native protocol transports.
pub mod transport {
    #[cfg(feature = "quic")]
    pub use fujin_transport_quic::plugin as quic;
    #[cfg(feature = "tcp")]
    pub use fujin_transport_tcp::plugin as tcp;
    #[cfg(all(feature = "unix", unix))]
    pub use fujin_transport_unix::plugin as unix;
    #[cfg(feature = "websocket")]
    pub use fujin_transport_websocket::plugin as websocket;
}

/// Registers every plugin dependency enabled in this build.
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
