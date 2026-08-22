//! Fujin process lifecycle, connector catalog, and runtime configuration.

pub mod configurator;

use std::{path::Path, sync::Arc};

use fujin_core::{
    Catalog, ConnectorMiddlewareCompiler, ConnectorRegistry, ConnectorsConfig, GenerationCompiler,
};
use serde::Deserialize;
use serde_json::Value;

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
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub settings: Value,
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

pub mod fujin_server_config {
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

    #[derive(Clone, Debug)]
    pub struct TlsConfig {
        pub certificate: String,
        pub private_key: String,
        pub client_certificates: Option<String>,
        pub require_client_certificate: bool,
    }

    #[derive(Clone, Debug)]
    pub struct TcpListenerConfig {
        pub listen: String,
        pub tls: Option<TlsConfig>,
    }

    #[derive(Clone, Debug)]
    pub struct UnixListenerConfig {
        pub path: String,
    }

    #[derive(Clone, Debug)]
    pub struct WebSocketListenerConfig {
        pub listen: String,
        pub path: String,
        pub allowed_origins: Vec<String>,
        pub max_message_bytes: usize,
        pub tls: Option<TlsConfig>,
    }

    #[derive(Clone, Debug)]
    pub struct QuicListenerConfig {
        pub listen: String,
        pub tls: TlsConfig,
        pub max_incoming_streams: u32,
        pub max_idle_timeout: Option<Duration>,
        pub keepalive_period: Option<Duration>,
    }

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
    #[error("parse configuration {path:?}: {source}")]
    Parse {
        path: String,
        source: yaml_serde::Error,
    },
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
    ) -> Result<fujin_server_config::ControlPlaneConfig, RuntimeError> {
        let mut output = fujin_server_config::ControlPlaneConfig {
            build: build.into(),
            ..Default::default()
        };
        output.grpc = self.grpc.listener_config()?;
        if self.health.enabled {
            require_non_empty(&self.health.addr, "health addr")?;
            output.health = Some(fujin_server_config::SocketListenerConfig {
                listen: self.health.addr.clone(),
            });
        }
        Ok(output)
    }
}

impl GrpcConfig {
    fn listener_config(
        &self,
    ) -> Result<Option<fujin_server_config::GrpcListenerConfig>, RuntimeError> {
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
        Ok(Some(fujin_server_config::GrpcListenerConfig {
            listen: self.addr.clone(),
            max_concurrent_streams: self.max_concurrent_streams,
            max_recv_message_size: positive_size("gRPC max_recv_msg_size", self.max_recv_msg_size)?,
            max_send_message_size: positive_size("gRPC max_send_msg_size", self.max_send_msg_size)?,
            initial_window_size: self.initial_window_size,
            initial_connection_window_size: self.initial_conn_window_size,
            server_keepalive: self.server_keepalive.config()?,
            tls: tls_config(&self.tls, "gRPC")?,
        }))
    }
}

impl GrpcServerKeepAliveSettings {
    fn config(&self) -> Result<fujin_server_config::ServerKeepAliveConfig, RuntimeError> {
        Ok(fujin_server_config::ServerKeepAliveConfig {
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

fn tls_config(
    settings: &TlsSettings,
    listener: &str,
) -> Result<Option<fujin_server_config::TlsConfig>, RuntimeError> {
    if !settings.enabled {
        if settings.require_and_verify_client_cert {
            return Err(RuntimeError::InvalidConfig(format!(
                "{listener} cannot require client certificates while TLS is disabled"
            )));
        }
        return Ok(None);
    }
    require_non_empty(
        &settings.server_cert_pem_path,
        &format!("{listener} certificate"),
    )?;
    require_non_empty(
        &settings.server_key_pem_path,
        &format!("{listener} private key"),
    )?;
    if settings.require_and_verify_client_cert {
        require_non_empty(
            &settings.client_certs_dir,
            &format!("{listener} client certificates directory"),
        )?;
    }
    Ok(Some(fujin_server_config::TlsConfig {
        certificate: settings.server_cert_pem_path.clone(),
        private_key: settings.server_key_pem_path.clone(),
        client_certificates: (!settings.client_certs_dir.is_empty())
            .then(|| settings.client_certs_dir.clone()),
        require_client_certificate: settings.require_and_verify_client_cert,
    }))
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

/// Loads one complete YAML runtime snapshot from disk.
///
/// # Errors
///
/// Returns [`RuntimeError::Read`] when the file cannot be read or [`RuntimeError::Parse`] when
/// its contents do not match the runtime schema.
pub async fn load(path: impl AsRef<Path>) -> Result<RuntimeConfig, RuntimeError> {
    let path = path.as_ref();
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|source| RuntimeError::Read {
            path: path.display().to_string(),
            source,
        })?;
    yaml_serde::from_slice(&bytes).map_err(|source| RuntimeError::Parse {
        path: path.display().to_string(),
        source,
    })
}

/// Reloads only the complete connector snapshot from a Go-compatible bootstrap file.
///
/// # Errors
///
/// Returns the load, compilation, preflight, or publication error. Listener settings are not
/// changed by runtime reload.
pub async fn reload_connectors(
    path: impl AsRef<Path>,
    catalog: &Catalog,
) -> Result<(), RuntimeError> {
    let config = load(path).await?;
    catalog.reload(&config.connectors).await?;
    Ok(())
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
    fn parses_go_compatible_transport_entries_and_connector_snapshot() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r"
fujin:
  transports:
    - type: tcp
      settings:
        addr: 127.0.0.1:4848
    - type: quic
      enabled: true
      settings:
        addr: 127.0.0.1:4849
        max_incoming_streams: 2048
        tls:
          enabled: true
          server_cert_pem_path: cert.pem
          server_key_pem_path: key.pem
grpc:
  enabled: false
connectors:
  primary:
    type: kafka_franz
    settings:
      common:
        brokers: [localhost:9092]
      routes:
        events:
          produce_topic: events
",
        )
        .expect("parse runtime config");
        let control_plane = config
            .control_plane_config("test")
            .expect("convert control-plane config");

        assert_eq!(config.connectors["primary"].connector_type, "kafka_franz");
        assert_eq!(config.fujin.transports.len(), 2);
        assert_eq!(config.fujin.transports[0].transport_type, "tcp");
        assert_eq!(
            config.fujin.transports[0].settings["addr"],
            "127.0.0.1:4848"
        );
        assert_eq!(config.fujin.transports[1].transport_type, "quic");
        assert_eq!(
            config.fujin.transports[1].settings["max_incoming_streams"],
            2048
        );
        assert!(control_plane.grpc.is_none());
    }

    #[test]
    fn rejects_grpc_controls_not_supported_by_base_build() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r#"
grpc:
  enabled: true
  addr: "127.0.0.1:4849"
  client_keepalive:
    min_time: 10s
"#,
        )
        .expect("parse gRPC controls");

        assert!(matches!(
            config.control_plane_config("test"),
            Err(RuntimeError::InvalidConfig(message)) if message.contains("unavailable")
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn reloads_complete_connector_snapshot_and_preserves_rejected_generation() {
        use std::collections::BTreeMap;

        use fujin_core::{
            AcceptanceGuarantee, BoxFuture, Capabilities, CompiledConnector, CompletionSink,
            ConnectorDescriptor, ConnectorRuntime, CoreError, Reader, ReaderEventSink, Result,
            RouteProfile, Writer,
        };

        #[derive(Debug)]
        struct Descriptor;

        impl ConnectorDescriptor for Descriptor {
            fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledConnector>> {
                let version = settings
                    .get("version")
                    .and_then(Value::as_str)
                    .ok_or_else(|| CoreError::InvalidConfig("missing test version".into()))?;
                Ok(Arc::new(Compiled {
                    version: version.to_owned(),
                    routes: BTreeMap::from([(
                        "route".into(),
                        RouteProfile {
                            capabilities: Capabilities::PRODUCE,
                            produce_guarantee: AcceptanceGuarantee::Local,
                            ..RouteProfile::default()
                        },
                    )]),
                }))
            }
        }

        #[derive(Debug)]
        struct Compiled {
            version: String,
            routes: BTreeMap<String, RouteProfile>,
        }

        impl CompiledConnector for Compiled {
            fn routes(&self) -> &BTreeMap<String, RouteProfile> {
                &self.routes
            }

            fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
                Ok(Arc::new(TestRuntime {
                    _version: self.version.clone(),
                }))
            }
        }

        #[derive(Debug)]
        struct TestRuntime {
            _version: String,
        }

        impl ConnectorRuntime for TestRuntime {
            fn open_reader(
                &self,
                _route: &str,
                _auto_settle: bool,
                _events: Arc<dyn ReaderEventSink>,
            ) -> Result<Arc<dyn Reader>> {
                Err(CoreError::OperationUnsupported)
            }

            fn open_writer(
                &self,
                _route: &str,
                _completions: Arc<dyn CompletionSink>,
            ) -> Result<Arc<dyn Writer>> {
                Err(CoreError::OperationUnsupported)
            }

            fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
                Box::pin(async { Ok(()) })
            }
        }

        let registry = Arc::new(ConnectorRegistry::default());
        registry
            .register("test", Arc::new(Descriptor))
            .expect("register test descriptor");
        let path = std::env::temp_dir().join(format!(
            "fujin-rust-reload-{}-{}.yaml",
            std::process::id(),
            std::thread::current().name().unwrap_or("runtime")
        ));
        let initial = r"
grpc: { enabled: false }
connectors:
  connector:
    type: test
    settings: { version: v1 }
";
        tokio::fs::write(&path, initial)
            .await
            .expect("write initial config");
        let config = load(&path).await.expect("load initial config");
        let catalog = compile_catalog(
            &config,
            registry,
            Arc::new(fujin_core::NoConnectorMiddleware),
        )
        .await
        .expect("compile initial catalog");
        let initial_generation = catalog.current().expect("initial generation");

        tokio::fs::write(&path, initial.replace("v1", "v2"))
            .await
            .expect("write replacement config");
        reload_connectors(&path, &catalog)
            .await
            .expect("publish replacement");
        let replacement = catalog.current().expect("replacement generation");
        assert!(!Arc::ptr_eq(&initial_generation, &replacement));

        tokio::fs::write(&path, initial.replace("version: v1", "invalid: true"))
            .await
            .expect("write rejected config");
        assert!(reload_connectors(&path, &catalog).await.is_err());
        assert!(Arc::ptr_eq(
            &replacement,
            &catalog.current().expect("replacement remains active")
        ));

        catalog.close().await.expect("close catalog");
        tokio::fs::remove_file(path)
            .await
            .expect("remove test config");
    }
}
