use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use fujin_core::BoxFuture;
use fujin_runtime::{TlsSettings, fujin_server_config};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

use super::{CompiledTransport, TransportContext, TransportPlugin, TransportRegistration};

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

fn decode<T: DeserializeOwned>(transport: &str, settings: &Value) -> Result<T> {
    serde_json::from_value(settings.clone())
        .with_context(|| format!("parse {transport} transport settings"))
}

fn require_non_empty(value: &str, name: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{name} is required");
    }
    Ok(())
}

fn protocol_config(settings: &NativeProtocolSettings) -> Result<()> {
    if settings.ping_interval.is_some()
        || settings.ping_timeout.is_some()
        || settings.ping_max_retries != 0
        || settings.write_buffer_size.is_some()
        || settings.write_deadline.is_some()
        || settings.force_terminate_timeout.is_some()
        || settings.ping_stream
    {
        bail!("native fujin ping and write tuning controls are unavailable in the base Rust build");
    }
    Ok(())
}

fn tls_config(
    settings: &TlsSettings,
    listener: &str,
) -> Result<Option<fujin_server_config::TlsConfig>> {
    if !settings.enabled {
        if settings.require_and_verify_client_cert {
            bail!("{listener} cannot require client certificates while TLS is disabled");
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

fn parse_duration(name: &str, value: Option<&str>) -> Result<Option<Duration>> {
    value
        .map(|value| humantime::parse_duration(value).with_context(|| format!("parse {name}")))
        .transpose()
}

#[cfg(feature = "tcp")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TcpSettings {
    addr: String,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[cfg(feature = "tcp")]
#[derive(Debug)]
struct TcpPlugin;

#[cfg(feature = "tcp")]
impl TransportPlugin for TcpPlugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: TcpSettings = decode("tcp", settings)?;
        protocol_config(&settings.fujin)?;
        require_non_empty(&settings.addr, "TCP addr")?;
        Ok(Arc::new(TcpTransport {
            config: fujin_server_config::TcpListenerConfig {
                listen: settings.addr,
                tls: tls_config(&settings.tls, "TCP")?,
            },
        }))
    }
}

#[cfg(feature = "tcp")]
#[derive(Debug)]
struct TcpTransport {
    config: fujin_server_config::TcpListenerConfig,
}

#[cfg(feature = "tcp")]
impl CompiledTransport for TcpTransport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(crate::server::serve_tcp(self.config.clone(), context))
    }
}

#[cfg(feature = "tcp")]
#[must_use]
pub fn tcp_plugin() -> TransportRegistration {
    TransportRegistration::new("tcp", TcpPlugin)
}

#[cfg(all(feature = "unix", unix))]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnixSettings {
    path: String,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[cfg(all(feature = "unix", unix))]
#[derive(Debug)]
struct UnixPlugin;

#[cfg(all(feature = "unix", unix))]
impl TransportPlugin for UnixPlugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: UnixSettings = decode("unix", settings)?;
        protocol_config(&settings.fujin)?;
        require_non_empty(&settings.path, "Unix path")?;
        Ok(Arc::new(UnixTransport {
            path: settings.path,
        }))
    }
}

#[cfg(all(feature = "unix", unix))]
#[derive(Debug)]
struct UnixTransport {
    path: String,
}

#[cfg(all(feature = "unix", unix))]
impl CompiledTransport for UnixTransport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(crate::server::serve_unix(self.path.clone(), context))
    }
}

#[cfg(all(feature = "unix", unix))]
#[must_use]
pub fn unix_plugin() -> TransportRegistration {
    TransportRegistration::new("unix", UnixPlugin)
}

#[cfg(feature = "websocket")]
fn default_websocket_path() -> String {
    "/fujin".into()
}

#[cfg(feature = "websocket")]
fn default_max_message_bytes() -> usize {
    4 * 1024 * 1024
}

#[cfg(feature = "websocket")]
#[derive(Debug, Deserialize)]
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

#[cfg(feature = "websocket")]
#[derive(Debug)]
struct WebSocketPlugin;

#[cfg(feature = "websocket")]
impl TransportPlugin for WebSocketPlugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: WebSocketSettings = decode("websocket", settings)?;
        protocol_config(&settings.fujin)?;
        require_non_empty(&settings.addr, "WebSocket addr")?;
        if !settings.path.starts_with('/') {
            bail!("WebSocket path must start with '/'");
        }
        if settings.max_message_bytes == 0 {
            bail!("WebSocket max_message_bytes must be positive");
        }
        Ok(Arc::new(WebSocketTransport {
            config: fujin_server_config::WebSocketListenerConfig {
                listen: settings.addr,
                path: settings.path,
                allowed_origins: settings.allowed_origins,
                max_message_bytes: settings.max_message_bytes,
                tls: tls_config(&settings.tls, "WebSocket")?,
            },
        }))
    }
}

#[cfg(feature = "websocket")]
#[derive(Debug)]
struct WebSocketTransport {
    config: fujin_server_config::WebSocketListenerConfig,
}

#[cfg(feature = "websocket")]
impl CompiledTransport for WebSocketTransport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(crate::server::serve_websocket(self.config.clone(), context))
    }
}

#[cfg(feature = "websocket")]
#[must_use]
pub fn websocket_plugin() -> TransportRegistration {
    TransportRegistration::new("websocket", WebSocketPlugin)
}

#[cfg(feature = "quic")]
fn default_max_incoming_streams() -> u32 {
    1024
}

#[cfg(feature = "quic")]
#[derive(Debug, Deserialize)]
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

#[cfg(feature = "quic")]
#[derive(Debug)]
struct QuicPlugin;

#[cfg(feature = "quic")]
impl TransportPlugin for QuicPlugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: QuicSettings = decode("quic", settings)?;
        protocol_config(&settings.fujin)?;
        require_non_empty(&settings.addr, "QUIC addr")?;
        let tls = tls_config(&settings.tls, "QUIC")?
            .ok_or_else(|| anyhow::anyhow!("QUIC requires settings.tls.enabled=true"))?;
        Ok(Arc::new(QuicTransport {
            config: fujin_server_config::QuicListenerConfig {
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
            },
        }))
    }
}

#[cfg(feature = "quic")]
#[derive(Debug)]
struct QuicTransport {
    config: fujin_server_config::QuicListenerConfig,
}

#[cfg(feature = "quic")]
impl CompiledTransport for QuicTransport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(crate::server::serve_quic(self.config.clone(), context))
    }
}

#[cfg(feature = "quic")]
#[must_use]
pub fn quic_plugin() -> TransportRegistration {
    TransportRegistration::new("quic", QuicPlugin)
}
