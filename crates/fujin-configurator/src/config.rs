//! Fujin runtime configuration and connector catalog compilation.

use std::sync::Arc;

use fujin_connector::{
    Catalog, ConnectorMiddlewareCompiler, ConnectorRegistry, ConnectorsConfig, GenerationCompiler,
};
pub use fujin_transport::{TransportConfig, settings::TlsSettings};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub fujin: FujinConfig,
    #[serde(default)]
    pub grpc: GrpcConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub connectors: ConnectorsConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct FujinConfig {
    #[serde(default)]
    pub transports: Vec<TransportConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub addr: String,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub max_decoding_message_size: Option<usize>,
    #[serde(default)]
    pub max_encoding_message_size: Option<usize>,
    #[serde(default)]
    pub initial_stream_window_size: Option<u32>,
    #[serde(default)]
    pub initial_connection_window_size: Option<u32>,
    #[serde(default)]
    pub http2_keepalive_interval: Option<String>,
    #[serde(default)]
    pub http2_keepalive_timeout: Option<String>,
    #[serde(default)]
    pub http2_adaptive_window: Option<bool>,
    #[serde(default)]
    pub max_connection_age: Option<String>,
    #[serde(default)]
    pub max_connection_age_grace: Option<String>,
    #[serde(default)]
    pub tls: TlsSettings,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            addr: String::new(),
            timeout: None,
            max_concurrent_streams: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
            initial_stream_window_size: None,
            initial_connection_window_size: None,
            http2_keepalive_interval: None,
            http2_keepalive_timeout: None,
            http2_adaptive_window: None,
            max_connection_age: None,
            max_connection_age_grace: None,
            tls: TlsSettings::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct HealthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub addr: String,
}

const fn default_enabled() -> bool {
    true
}

pub mod server_config {
    use std::time::Duration;

    #[derive(Clone, Debug, Default)]
    pub struct ControlPlaneConfig {
        pub build: String,
        pub grpc: Option<GrpcListenerConfig>,
        pub health: Option<SocketListenerConfig>,
    }

    #[derive(Clone, Debug)]
    pub struct SocketListenerConfig {
        pub listen: String,
    }

    pub use fujin_transport::tls::TlsConfig;

    #[derive(Clone, Debug)]
    pub struct GrpcListenerConfig {
        pub listen: String,
        pub timeout: Option<Duration>,
        pub max_concurrent_streams: Option<u32>,
        pub max_decoding_message_size: Option<usize>,
        pub max_encoding_message_size: Option<usize>,
        pub initial_stream_window_size: Option<u32>,
        pub initial_connection_window_size: Option<u32>,
        pub http2_keepalive_interval: Option<Duration>,
        pub http2_keepalive_timeout: Option<Duration>,
        pub http2_adaptive_window: Option<bool>,
        pub max_connection_age: Option<Duration>,
        pub max_connection_age_grace: Option<Duration>,
        pub tls: Option<TlsConfig>,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("read configuration {path:?}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parse configuration {path:?}: {message}")]
    Parse { path: String, message: String },
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Core(#[from] fujin_error::CoreError),
}

impl RuntimeConfig {
    /// Validates listeners owned by the application control plane.
    ///
    /// Native transport entries are compiled independently through the registered transport
    /// plugins.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for incomplete gRPC or health listeners.
    pub fn control_plane_config(
        &self,
        build: impl Into<String>,
    ) -> Result<server_config::ControlPlaneConfig, RuntimeError> {
        let mut output = server_config::ControlPlaneConfig {
            build: build.into(),
            ..Default::default()
        };
        output.grpc = self.grpc.listener_config()?;
        if self.health.enabled {
            require_non_empty(&self.health.addr, "health addr")?;
            output.health = Some(server_config::SocketListenerConfig {
                listen: self.health.addr.clone(),
            });
        }
        Ok(output)
    }
}

impl GrpcConfig {
    fn listener_config(&self) -> Result<Option<server_config::GrpcListenerConfig>, RuntimeError> {
        if !self.enabled {
            return Ok(None);
        }
        require_non_empty(&self.addr, "gRPC addr")?;
        Ok(Some(server_config::GrpcListenerConfig {
            listen: self.addr.clone(),
            timeout: parse_duration("gRPC timeout", self.timeout.as_deref())?,
            max_concurrent_streams: self.max_concurrent_streams,
            max_decoding_message_size: positive_size(
                "gRPC max_decoding_message_size",
                self.max_decoding_message_size,
            )?,
            max_encoding_message_size: positive_size(
                "gRPC max_encoding_message_size",
                self.max_encoding_message_size,
            )?,
            initial_stream_window_size: self.initial_stream_window_size,
            initial_connection_window_size: self.initial_connection_window_size,
            http2_keepalive_interval: parse_duration(
                "gRPC http2_keepalive_interval",
                self.http2_keepalive_interval.as_deref(),
            )?,
            http2_keepalive_timeout: parse_duration(
                "gRPC http2_keepalive_timeout",
                self.http2_keepalive_timeout.as_deref(),
            )?,
            http2_adaptive_window: self.http2_adaptive_window,
            max_connection_age: parse_duration(
                "gRPC max_connection_age",
                self.max_connection_age.as_deref(),
            )?,
            max_connection_age_grace: parse_duration(
                "gRPC max_connection_age_grace",
                self.max_connection_age_grace.as_deref(),
            )?,
            tls: self
                .tls
                .listener_config("gRPC")
                .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?,
        }))
    }
}

fn parse_duration(
    field: &str,
    value: Option<&str>,
) -> Result<Option<std::time::Duration>, RuntimeError> {
    value
        .map(|value| {
            humantime::parse_duration(value)
                .map_err(|error| RuntimeError::InvalidConfig(format!("{field} {value:?}: {error}")))
        })
        .transpose()
}

fn positive_size(field: &str, value: Option<usize>) -> Result<Option<usize>, RuntimeError> {
    match value {
        Some(0) => Err(RuntimeError::InvalidConfig(format!(
            "{field} must be positive"
        ))),
        value => Ok(value),
    }
}

fn require_non_empty(value: &str, name: &str) -> Result<(), RuntimeError> {
    if value.is_empty() {
        Err(RuntimeError::InvalidConfig(format!("{name} is empty")))
    } else {
        Ok(())
    }
}

/// Compiles and publishes the initial connector generation without broker I/O unless a connector
/// explicitly requests eager runtime preflight.
///
/// # Errors
///
/// Returns [`RuntimeError::Core`] when connector registration, configuration, middleware, or
/// generation preflight fails.
pub async fn compile_catalog(
    config: &RuntimeConfig,
    registry: Arc<ConnectorRegistry>,
    middleware: Arc<dyn ConnectorMiddlewareCompiler>,
) -> Result<Arc<Catalog>, RuntimeError> {
    let compiler = Arc::new(GenerationCompiler::new(registry, middleware));
    Ok(Arc::new(
        Catalog::compile(&config.connectors, compiler).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_transport_entries_and_connector_snapshot() {
        let config: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "fujin": {
                "transports": [
                    {"type": "tcp", "settings": {"addr": "127.0.0.1:4848"}},
                    {"type": "quic", "enabled": true, "settings": {"max_concurrent_bidi_streams": 2048}}
                ]
            },
            "grpc": {"enabled": false},
            "connectors": {
                "primary": {"type": "kafka", "settings": {}}
            }
        }))
        .expect("parse runtime config");
        let control_plane = config
            .control_plane_config("test")
            .expect("convert control plane");

        assert_eq!(config.connectors["primary"].connector_type, "kafka");
        assert_eq!(config.fujin.transports.len(), 2);
        assert_eq!(config.fujin.transports[0].transport_type, "tcp");
        assert_eq!(config.fujin.transports[1].transport_type, "quic");
        assert!(control_plane.grpc.is_none());
    }

    #[test]
    fn compiles_tonic_named_grpc_controls() {
        let config: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "grpc": {
                "enabled": true,
                "addr": "127.0.0.1:4849",
                "timeout": "30s",
                "max_concurrent_streams": 128,
                "max_decoding_message_size": 1_048_576,
                "initial_stream_window_size": 65535,
                "http2_keepalive_interval": "2h",
                "http2_keepalive_timeout": "20s",
                "http2_adaptive_window": true,
                "max_connection_age": "30m",
                "max_connection_age_grace": "5s"
            }
        }))
        .expect("parse Tonic controls");

        let grpc = config
            .control_plane_config("test")
            .expect("compile control plane")
            .grpc
            .expect("gRPC listener");
        assert_eq!(grpc.timeout, Some(std::time::Duration::from_secs(30)));
        assert_eq!(grpc.max_concurrent_streams, Some(128));
        assert_eq!(grpc.http2_adaptive_window, Some(true));
        assert_eq!(
            grpc.max_connection_age,
            Some(std::time::Duration::from_mins(30))
        );
    }
}
