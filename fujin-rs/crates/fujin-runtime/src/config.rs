//! Fujin runtime configuration and connector catalog compilation.

use std::sync::Arc;

use fujin_core::{
    Catalog, ConnectorMiddlewareCompiler, ConnectorRegistry, ConnectorsConfig, GenerationCompiler,
};
pub use fujin_transport::TransportConfig;
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
    pub connection_timeout: Option<String>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub max_recv_msg_size: Option<usize>,
    #[serde(default)]
    pub max_send_msg_size: Option<usize>,
    #[serde(default)]
    pub initial_window_size: Option<u32>,
    #[serde(default)]
    pub initial_conn_window_size: Option<u32>,
    #[serde(default)]
    pub server_keepalive: GrpcServerKeepAliveSettings,
    #[serde(default)]
    pub client_keepalive: GrpcClientKeepAliveSettings,
    #[serde(default)]
    pub tls: TlsSettings,
    #[serde(default)]
    pub observability_enabled: bool,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            addr: String::new(),
            connection_timeout: None,
            max_concurrent_streams: None,
            max_recv_msg_size: None,
            max_send_msg_size: None,
            initial_window_size: None,
            initial_conn_window_size: None,
            server_keepalive: GrpcServerKeepAliveSettings::default(),
            client_keepalive: GrpcClientKeepAliveSettings::default(),
            tls: TlsSettings::default(),
            observability_enabled: false,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcServerKeepAliveSettings {
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub max_connection_idle: Option<String>,
    #[serde(default)]
    pub max_connection_age: Option<String>,
    #[serde(default)]
    pub max_connection_age_grace: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcClientKeepAliveSettings {
    #[serde(default)]
    pub min_time: Option<String>,
    #[serde(default)]
    pub permit_without_stream: bool,
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

    pub use fujin_tls::TlsConfig;

    #[derive(Clone, Debug, Default)]
    pub struct ServerKeepAliveConfig {
        pub time: Option<Duration>,
        pub timeout: Option<Duration>,
        pub max_connection_age: Option<Duration>,
        pub max_connection_age_grace: Option<Duration>,
    }

    #[derive(Clone, Debug)]
    pub struct GrpcListenerConfig {
        pub listen: String,
        pub max_concurrent_streams: Option<u32>,
        pub max_recv_message_size: Option<usize>,
        pub max_send_message_size: Option<usize>,
        pub initial_window_size: Option<u32>,
        pub initial_connection_window_size: Option<u32>,
        pub server_keepalive: ServerKeepAliveConfig,
        pub tls: Option<TlsConfig>,
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub client_certs_dir: String,
    #[serde(default)]
    pub server_cert_pem_path: String,
    #[serde(default)]
    pub server_key_pem_path: String,
    #[serde(default)]
    pub require_and_verify_client_cert: bool,
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
    Core(#[from] fujin_core::CoreError),
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
        if self.observability_enabled {
            return Err(RuntimeError::InvalidConfig(
                "gRPC observability_enabled is unavailable in the base Rust build".into(),
            ));
        }
        if self.connection_timeout.is_some()
            || self.server_keepalive.max_connection_idle.is_some()
            || self.client_keepalive.min_time.is_some()
            || self.client_keepalive.permit_without_stream
        {
            return Err(RuntimeError::InvalidConfig(
                "gRPC connection_timeout, server_keepalive.max_connection_idle, and client_keepalive enforcement are unavailable in the base Rust build".into(),
            ));
        }
        Ok(Some(server_config::GrpcListenerConfig {
            listen: self.addr.clone(),
            max_concurrent_streams: self.max_concurrent_streams,
            max_recv_message_size: positive_size("gRPC max_recv_msg_size", self.max_recv_msg_size)?,
            max_send_message_size: positive_size("gRPC max_send_msg_size", self.max_send_msg_size)?,
            initial_window_size: self.initial_window_size,
            initial_connection_window_size: self.initial_conn_window_size,
            server_keepalive: self.server_keepalive.config()?,
            tls: self.tls.listener_config("gRPC")?,
        }))
    }
}

impl GrpcServerKeepAliveSettings {
    fn config(&self) -> Result<server_config::ServerKeepAliveConfig, RuntimeError> {
        Ok(server_config::ServerKeepAliveConfig {
            time: parse_duration("gRPC server_keepalive.time", self.time.as_deref())?,
            timeout: parse_duration("gRPC server_keepalive.timeout", self.timeout.as_deref())?,
            max_connection_age: parse_duration(
                "gRPC server_keepalive.max_connection_age",
                self.max_connection_age.as_deref(),
            )?,
            max_connection_age_grace: parse_duration(
                "gRPC server_keepalive.max_connection_age_grace",
                self.max_connection_age_grace.as_deref(),
            )?,
        })
    }
}

impl TlsSettings {
    /// Compiles validated listener TLS settings without reading certificate files.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for incomplete or contradictory TLS settings.
    pub fn listener_config(
        &self,
        listener: &str,
    ) -> Result<Option<server_config::TlsConfig>, RuntimeError> {
        if !self.enabled {
            if self.require_and_verify_client_cert {
                return Err(RuntimeError::InvalidConfig(format!(
                    "{listener} cannot require client certificates while TLS is disabled"
                )));
            }
            return Ok(None);
        }
        require_non_empty(
            &self.server_cert_pem_path,
            &format!("{listener} certificate"),
        )?;
        require_non_empty(
            &self.server_key_pem_path,
            &format!("{listener} private key"),
        )?;
        if self.require_and_verify_client_cert {
            require_non_empty(
                &self.client_certs_dir,
                &format!("{listener} client certificates directory"),
            )?;
        }
        Ok(Some(server_config::TlsConfig {
            certificate: self.server_cert_pem_path.clone(),
            private_key: self.server_key_pem_path.clone(),
            client_certificates: (!self.client_certs_dir.is_empty())
                .then(|| self.client_certs_dir.clone()),
            require_client_certificate: self.require_and_verify_client_cert,
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
                    {"type": "quic", "enabled": true, "settings": {"max_incoming_streams": 2048}}
                ]
            },
            "grpc": {"enabled": false},
            "connectors": {
                "primary": {"type": "kafka_franz", "settings": {}}
            }
        }))
        .expect("parse runtime config");
        let control_plane = config
            .control_plane_config("test")
            .expect("convert control plane");

        assert_eq!(config.connectors["primary"].connector_type, "kafka_franz");
        assert_eq!(config.fujin.transports.len(), 2);
        assert_eq!(config.fujin.transports[0].transport_type, "tcp");
        assert_eq!(config.fujin.transports[1].transport_type, "quic");
        assert!(control_plane.grpc.is_none());
    }

    #[test]
    fn rejects_unsupported_grpc_controls() {
        let config: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "grpc": {
                "enabled": true,
                "addr": "127.0.0.1:4849",
                "client_keepalive": {"min_time": "10s"}
            }
        }))
        .expect("parse gRPC controls");

        assert!(matches!(
            config.control_plane_config("test"),
            Err(RuntimeError::InvalidConfig(message)) if message.contains("unavailable")
        ));
    }
}
