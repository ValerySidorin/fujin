//! Fujin process lifecycle, connector catalog, and runtime configuration.

use std::{path::Path, sync::Arc};

use fujin_core::{
    Catalog, ConnectorsConfig, DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FujinConfig {
    #[serde(default)]
    pub transports: Vec<TransportConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub settings: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub addr: String,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            addr: String::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct ServerConfig {
        #[serde(default = "default_build")]
        pub build: String,
        #[serde(default)]
        pub tcp: Option<SocketListenerConfig>,
        #[serde(default)]
        pub unix: Option<UnixListenerConfig>,
        #[serde(default)]
        pub websocket: Option<SocketListenerConfig>,
        #[serde(default)]
        pub quic: Option<QuicListenerConfig>,
        #[serde(default)]
        pub grpc: Option<SocketListenerConfig>,
        #[serde(default)]
        pub health: Option<SocketListenerConfig>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct SocketListenerConfig {
        pub listen: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct UnixListenerConfig {
        pub path: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct QuicListenerConfig {
        pub listen: String,
        pub certificate: String,
        pub private_key: String,
        #[serde(default = "default_max_incoming_streams")]
        pub max_incoming_streams: u32,
    }

    fn default_max_incoming_streams() -> u32 {
        1024
    }

    fn default_build() -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }
}

#[derive(Clone, Debug, Deserialize)]
struct AddressSettings {
    addr: String,
}

#[derive(Clone, Debug, Deserialize)]
struct UnixSettings {
    path: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TlsSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    server_cert_pem_path: String,
    #[serde(default)]
    server_key_pem_path: String,
}

#[derive(Clone, Debug, Deserialize)]
struct QuicSettings {
    addr: String,
    #[serde(default = "default_max_incoming_streams")]
    max_incoming_streams: u32,
    tls: TlsSettings,
}

fn default_max_incoming_streams() -> u32 {
    1024
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
            match transport.transport_type.as_str() {
                "tcp" => {
                    ensure_absent(output.tcp.as_ref(), "tcp")?;
                    let settings: AddressSettings = decode_settings(transport)?;
                    require_non_empty(&settings.addr, "TCP addr")?;
                    output.tcp = Some(fujin_server_config::SocketListenerConfig {
                        listen: settings.addr,
                    });
                }
                "unix" => {
                    ensure_absent(output.unix.as_ref(), "unix")?;
                    let settings: UnixSettings = decode_settings(transport)?;
                    require_non_empty(&settings.path, "Unix path")?;
                    output.unix = Some(fujin_server_config::UnixListenerConfig {
                        path: settings.path,
                    });
                }
                "websocket" => {
                    ensure_absent(output.websocket.as_ref(), "websocket")?;
                    let settings: AddressSettings = decode_settings(transport)?;
                    require_non_empty(&settings.addr, "WebSocket addr")?;
                    output.websocket = Some(fujin_server_config::SocketListenerConfig {
                        listen: settings.addr,
                    });
                }
                "quic" => {
                    ensure_absent(output.quic.as_ref(), "quic")?;
                    let settings: QuicSettings = decode_settings(transport)?;
                    require_non_empty(&settings.addr, "QUIC addr")?;
                    if !settings.tls.enabled {
                        return Err(RuntimeError::InvalidConfig(
                            "QUIC requires settings.tls.enabled=true".into(),
                        ));
                    }
                    require_non_empty(&settings.tls.server_cert_pem_path, "QUIC certificate")?;
                    require_non_empty(&settings.tls.server_key_pem_path, "QUIC private key")?;
                    output.quic = Some(fujin_server_config::QuicListenerConfig {
                        listen: settings.addr,
                        certificate: settings.tls.server_cert_pem_path,
                        private_key: settings.tls.server_key_pem_path,
                        max_incoming_streams: settings.max_incoming_streams,
                    });
                }
                name => {
                    return Err(RuntimeError::InvalidConfig(format!(
                        "unsupported transport type {name:?}"
                    )));
                }
            }
        }
        if self.grpc.enabled {
            require_non_empty(&self.grpc.addr, "gRPC addr")?;
            output.grpc = Some(fujin_server_config::SocketListenerConfig {
                listen: self.grpc.addr.clone(),
            });
        }
        if self.health.enabled {
            require_non_empty(&self.health.addr, "health addr")?;
            output.health = Some(fujin_server_config::SocketListenerConfig {
                listen: self.health.addr.clone(),
            });
        }
        Ok(output)
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
