//! Optional plugin dependencies selected by the final application build.

/// Bootstrap configurators.
pub mod configurator {
    #[cfg(feature = "configurator-env")]
    pub use fujin_configurator_env::plugin as env;
    #[cfg(feature = "configurator-file")]
    pub use fujin_configurator_file::plugin as file;
}

/// Connector implementations.
pub mod connector {
    #[cfg(feature = "connector-kafka")]
    pub use fujin_connector_kafka::plugin as kafka;
}

/// Native protocol transports.
pub mod transport {
    #[cfg(feature = "transport-quic")]
    pub use fujin_transport_quic::plugin as quic;
    #[cfg(feature = "transport-tcp")]
    pub use fujin_transport_tcp::plugin as tcp;
    #[cfg(all(feature = "transport-unix", unix))]
    pub use fujin_transport_unix::plugin as unix;
    #[cfg(feature = "transport-websocket")]
    pub use fujin_transport_websocket::plugin as websocket;
}

/// Registers every plugin dependency enabled in this build.
#[must_use]
pub fn full(builder: fujin::ApplicationBuilder) -> fujin::ApplicationBuilder {
    let builder = builder;
    #[cfg(feature = "configurator-env")]
    let builder = builder.configurator(configurator::env());
    #[cfg(feature = "configurator-file")]
    let builder = builder.configurator(configurator::file());
    #[cfg(feature = "connector-kafka")]
    let builder = builder.connector(connector::kafka());
    #[cfg(feature = "transport-tcp")]
    let builder = builder.transport(transport::tcp());
    #[cfg(all(feature = "transport-unix", unix))]
    let builder = builder.transport(transport::unix());
    #[cfg(feature = "transport-websocket")]
    let builder = builder.transport(transport::websocket());
    #[cfg(feature = "transport-quic")]
    let builder = builder.transport(transport::quic());
    builder
}
