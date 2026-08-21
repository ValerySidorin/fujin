//! Fujin process lifecycle, connector catalog, and runtime configuration.

use std::{path::Path, sync::Arc};

use fujin_core::{
    Catalog, ConnectorsConfig, DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware,
};
use serde::{Deserialize, de::DeserializeOwned};
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
    pub struct ServerConfig {
        pub build: String,
        pub tcp: Option<TcpListenerConfig>,
        pub unix: Option<UnixListenerConfig>,
        pub websocket: Option<WebSocketListenerConfig>,
        pub quic: Option<QuicListenerConfig>,
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
struct NativeProtocolSettings {
    #[serde(default)]
    ping_interval: Option<String>,
    #[serde(default)]
    ping_timeout: Option<String>,
    #[serde(default)]
    ping_max_retries: u32,
    #[serde(default)]
    write_buffer_size: Option<usize>,
    #[serde(default)]
    write_deadline: Option<String>,
    #[serde(default)]
    force_terminate_timeout: Option<String>,
    #[serde(default)]
    ping_stream: bool,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TcpSettings {
    addr: String,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnixSettings {
    path: String,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebSocketSettings {
    addr: String,
    #[serde(default = "default_websocket_path")]
    path: String,
    #[serde(default)]
    allowed_origins: Vec<String>,
    #[serde(default = "default_max_message_bytes")]
    max_message_bytes: usize,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuicSettings {
    addr: String,
    #[serde(default = "default_max_incoming_streams")]
    max_incoming_streams: u32,
    #[serde(default)]
    max_idle_timeout: Option<String>,
    #[serde(default)]
    keepalive_period: Option<String>,
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

fn default_max_incoming_streams() -> u32 {
    1024
}

fn default_websocket_path() -> String {
    "/fujin".into()
}

fn default_max_message_bytes() -> usize {
    4 * 1024 * 1024
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
    /// Validates the Go-compatible bootstrap shape and resolves enabled listeners.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for duplicate, unknown, or incomplete listeners.
    pub fn server_config(
        &self,
        build: impl Into<String>,
    ) -> Result<fujin_server_config::ServerConfig, RuntimeError> {
        let mut output = fujin_server_config::ServerConfig {
            build: build.into(),
            ..Default::default()
        };
        for transport in self.fujin.transports.iter().filter(|entry| entry.enabled) {
            apply_transport(&mut output, transport)?;
        }
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

fn apply_transport(
    output: &mut fujin_server_config::ServerConfig,
    transport: &TransportConfig,
) -> Result<(), RuntimeError> {
    match transport.transport_type.as_str() {
        "tcp" => {
            ensure_absent(output.tcp.as_ref(), "tcp")?;
            let settings: TcpSettings = decode_settings(transport)?;
            protocol_config(&settings.fujin)?;
            require_non_empty(&settings.addr, "TCP addr")?;
            output.tcp = Some(fujin_server_config::TcpListenerConfig {
                listen: settings.addr,
                tls: tls_config(&settings.tls, "TCP")?,
            });
        }
        "unix" => {
            ensure_absent(output.unix.as_ref(), "unix")?;
            let settings: UnixSettings = decode_settings(transport)?;
            protocol_config(&settings.fujin)?;
            require_non_empty(&settings.path, "Unix path")?;
            output.unix = Some(fujin_server_config::UnixListenerConfig {
                path: settings.path,
            });
        }
        "websocket" => apply_websocket(output, transport)?,
        "quic" => apply_quic(output, transport)?,
        name => {
            return Err(RuntimeError::InvalidConfig(format!(
                "unsupported transport type {name:?}"
            )));
        }
    }
    Ok(())
}

fn apply_websocket(
    output: &mut fujin_server_config::ServerConfig,
    transport: &TransportConfig,
) -> Result<(), RuntimeError> {
    ensure_absent(output.websocket.as_ref(), "websocket")?;
    let settings: WebSocketSettings = decode_settings(transport)?;
    protocol_config(&settings.fujin)?;
    require_non_empty(&settings.addr, "WebSocket addr")?;
    if !settings.path.starts_with('/') {
        return Err(RuntimeError::InvalidConfig(
            "WebSocket path must start with '/'".into(),
        ));
    }
    if settings.max_message_bytes == 0 {
        return Err(RuntimeError::InvalidConfig(
            "WebSocket max_message_bytes must be positive".into(),
        ));
    }
    output.websocket = Some(fujin_server_config::WebSocketListenerConfig {
        listen: settings.addr,
        path: settings.path,
        allowed_origins: settings.allowed_origins,
        max_message_bytes: settings.max_message_bytes,
        tls: tls_config(&settings.tls, "WebSocket")?,
    });
    Ok(())
}

fn apply_quic(
    output: &mut fujin_server_config::ServerConfig,
    transport: &TransportConfig,
) -> Result<(), RuntimeError> {
    ensure_absent(output.quic.as_ref(), "quic")?;
    let settings: QuicSettings = decode_settings(transport)?;
    require_non_empty(&settings.addr, "QUIC addr")?;
    protocol_config(&settings.fujin)?;
    let tls = tls_config(&settings.tls, "QUIC")?.ok_or_else(|| {
        RuntimeError::InvalidConfig("QUIC requires settings.tls.enabled=true".into())
    })?;
    output.quic = Some(fujin_server_config::QuicListenerConfig {
        listen: settings.addr,
        tls,
        max_incoming_streams: settings.max_incoming_streams,
        max_idle_timeout: parse_duration(
            "QUIC max_idle_timeout",
            settings.max_idle_timeout.as_deref(),
        )?,
        keepalive_period: parse_duration(
            "QUIC keepalive_period",
            settings.keepalive_period.as_deref(),
        )?,
    });
    Ok(())
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

fn decode_settings<T: DeserializeOwned>(entry: &TransportConfig) -> Result<T, RuntimeError> {
    serde_json::from_value(entry.settings.clone()).map_err(|error| {
        RuntimeError::InvalidConfig(format!(
            "transport {:?} settings: {error}",
            entry.transport_type
        ))
    })
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

fn protocol_config(settings: &NativeProtocolSettings) -> Result<(), RuntimeError> {
    if settings.ping_interval.is_some()
        || settings.ping_timeout.is_some()
        || settings.ping_max_retries != 0
        || settings.write_buffer_size.is_some()
        || settings.write_deadline.is_some()
        || settings.force_terminate_timeout.is_some()
        || settings.ping_stream
    {
        return Err(RuntimeError::InvalidConfig(
            "native fujin ping and write tuning controls are unavailable in the base Rust build"
                .into(),
        ));
    }
    Ok(())
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

fn ensure_absent<T>(value: Option<&T>, name: &str) -> Result<(), RuntimeError> {
    if value.is_some() {
        Err(RuntimeError::InvalidConfig(format!(
            "duplicate enabled {name} transport"
        )))
    } else {
        Ok(())
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
    registry: Arc<DescriptorRegistry>,
) -> Result<Arc<Catalog>, RuntimeError> {
    let compiler = Arc::new(GenerationCompiler::new(
        registry,
        Arc::new(NoConnectorMiddleware),
    ));
    Ok(Arc::new(
        Catalog::compile(&config.connectors, compiler).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parses_go_compatible_listener_and_connector_snapshot() {
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
        let server = config.server_config("test").expect("convert server config");

        assert_eq!(config.connectors["primary"].connector_type, "kafka_franz");
        assert_eq!(
            server.tcp.as_ref().map(|value| value.listen.as_str()),
            Some("127.0.0.1:4848")
        );
        assert_eq!(
            server.quic.as_ref().map(|value| value.max_incoming_streams),
            Some(2048)
        );
        assert!(server.grpc.is_none());
    }

    #[test]
    fn maps_websocket_and_quic_operator_controls() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r#"
fujin:
  transports:
    - type: websocket
      settings:
        addr: "127.0.0.1:4851"
        path: /gateway
        allowed_origins: ["https://console.example"]
        max_message_bytes: 1048576
    - type: quic
      settings:
        addr: "127.0.0.1:4849"
        max_idle_timeout: 1m
        keepalive_period: 30s
        tls:
          enabled: true
          server_cert_pem_path: cert.pem
          server_key_pem_path: key.pem
grpc: { enabled: false }
"#,
        )
        .expect("parse operator controls");
        let server = config.server_config("test").expect("map operator controls");

        let websocket = server.websocket.expect("WebSocket listener");
        assert_eq!(websocket.path, "/gateway");
        assert_eq!(websocket.allowed_origins, ["https://console.example"]);
        assert_eq!(websocket.max_message_bytes, 1_048_576);
        let quic = server.quic.expect("QUIC listener");
        assert_eq!(quic.max_idle_timeout, Some(Duration::from_mins(1)));
        assert_eq!(quic.keepalive_period, Some(Duration::from_secs(30)));
    }

    #[test]
    fn rejects_native_tuning_controls_not_supported_by_base_build() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r#"
fujin:
  transports:
    - type: tcp
      settings:
        addr: "127.0.0.1:4848"
        fujin: { ping_interval: 20s }
grpc: { enabled: false }
"#,
        )
        .expect("parse native controls");

        assert!(matches!(
            config.server_config("test"),
            Err(RuntimeError::InvalidConfig(message)) if message.contains("unavailable")
        ));
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
            config.server_config("test"),
            Err(RuntimeError::InvalidConfig(message)) if message.contains("unavailable")
        ));
    }

    #[test]
    fn rejects_duplicate_enabled_transports() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r#"
fujin:
  transports:
    - type: tcp
      settings: { addr: "127.0.0.1:4848" }
    - type: tcp
      settings: { addr: "127.0.0.1:4849" }
grpc: { enabled: false }
"#,
        )
        .expect("parse duplicate transports");

        assert!(matches!(
            config.server_config("test"),
            Err(RuntimeError::InvalidConfig(message)) if message.contains("duplicate")
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

        let registry = Arc::new(DescriptorRegistry::default());
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
        let catalog = compile_catalog(&config, registry)
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
