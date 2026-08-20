//! Fujin process lifecycle, connector catalog, and runtime configuration.

use std::{path::Path, sync::Arc};

use fujin_core::{
    Catalog, ConnectorsConfig, DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub connectors: ConnectorsConfig,
    #[serde(default)]
    pub server: fujin_server_config::ServerConfig,
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
    }

    fn default_build() -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }
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
    #[error(transparent)]
    Core(#[from] fujin_core::CoreError),
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
    fn parses_listener_and_connector_snapshot() {
        let config: RuntimeConfig = yaml_serde::from_str(
            r"
connectors:
  primary:
    type: kafka
    settings:
      brokers: localhost:9092
      routes:
        events:
          topic: events
server:
  build: test
  tcp:
    listen: 127.0.0.1:4848
",
        )
        .expect("parse runtime config");

        assert_eq!(config.connectors["primary"].connector_type, "kafka");
        assert_eq!(
            config
                .server
                .tcp
                .as_ref()
                .map(|value| value.listen.as_str()),
            Some("127.0.0.1:4848")
        );
    }
}
