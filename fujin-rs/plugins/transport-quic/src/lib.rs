//! Optional native Fujin v1 transport over QUIC bidirectional streams.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use fujin_core::BoxFuture;
use fujin_runtime::{TlsSettings, fujin_server_config::TlsConfig};
use fujin_transport::{
    CompiledTransport, Endpoint, TransportContext, TransportPlugin, TransportRegistration,
    listener::bind_udp, settings::NativeProtocolSettings,
};
use fujin_upgrade::ListenerMetadata;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

fn default_max_incoming_streams() -> u32 {
    1024
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
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

#[derive(Debug)]
struct Plugin;

impl TransportPlugin for Plugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: Settings =
            serde_json::from_value(settings.clone()).context("parse quic transport settings")?;
        settings.fujin.validate_supported()?;
        if settings.addr.is_empty() {
            bail!("QUIC addr is required");
        }
        let tls = settings
            .tls
            .listener_config("QUIC")?
            .ok_or_else(|| anyhow::anyhow!("QUIC requires settings.tls.enabled=true"))?;
        Ok(Arc::new(Transport {
            address: settings.addr,
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
        }))
    }
}

fn parse_duration(name: &str, value: Option<&str>) -> Result<Option<Duration>> {
    value
        .map(|value| humantime::parse_duration(value).with_context(|| format!("parse {name}")))
        .transpose()
}

#[derive(Debug)]
struct Transport {
    address: String,
    tls: TlsConfig,
    max_incoming_streams: u32,
    max_idle_timeout: Option<Duration>,
    keepalive_period: Option<Duration>,
}

impl CompiledTransport for Transport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { self.run(context).await })
    }
}

impl Transport {
    async fn run(&self, context: TransportContext) -> Result<()> {
        let address: std::net::SocketAddr =
            self.address.parse().context("parse QUIC listen address")?;
        let server_config = self.server_config().await?;
        let socket = bind_udp(
            address,
            ListenerMetadata::udp(self.address.clone()),
            context.listener_registry(),
            context.inherited_listeners(),
        )?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .context("start QUIC endpoint")?;
        context.signal_ready(Endpoint::native(
            "quic",
            "udp",
            endpoint
                .local_addr()
                .context("read QUIC listener address")?
                .to_string(),
            None,
            true,
        ));
        let shutdown = context.shutdown();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break; };
                    connections.spawn(serve_connection(incoming, context.clone()));
                }
            }
        }
        endpoint.close(quinn::VarInt::from_u32(0), b"server shutdown");
        endpoint.wait_idle().await;
        fujin_transport::listener::drain_tasks(&mut connections).await
    }

    async fn server_config(&self) -> Result<quinn::ServerConfig> {
        let (certificates, key) = fujin_tls::load_identity(&self.tls, "QUIC").await?;
        let mut server = quinn::ServerConfig::with_single_cert(certificates, key)
            .context("build QUIC server config")?;
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(self.max_incoming_streams));
        if let Some(timeout) = self.max_idle_timeout {
            transport.max_idle_timeout(Some(timeout.try_into().context("QUIC max idle timeout")?));
        }
        transport.keep_alive_interval(self.keepalive_period);
        server.transport_config(Arc::new(transport));
        Ok(server)
    }
}

async fn serve_connection(incoming: quinn::Incoming, context: TransportContext) -> Result<()> {
    let connection = incoming.await.context("accept QUIC connection")?;
    let shutdown: CancellationToken = context.shutdown();
    let mut streams = FuturesUnordered::new();
    let mut session_error = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            stream = connection.accept_bi() => match stream {
                Ok((send, recv)) => {
                    let session = context.clone();
                    streams.push(async move { session.serve_native_stream(QuicStream { recv, send }).await });
                }
                Err(quinn::ConnectionError::ApplicationClosed(_) | quinn::ConnectionError::LocallyClosed) => break,
                Err(error) => return Err(error).context("accept QUIC stream"),
            },
            result = streams.next(), if !streams.is_empty() => {
                if let Some(Err(error)) = result && session_error.is_none() { session_error = Some(error); }
            }
        }
    }
    connection.close(quinn::VarInt::from_u32(0), b"server shutdown");
    while let Some(result) = streams.next().await {
        if let Err(error) = result
            && session_error.is_none()
        {
            session_error = Some(error);
        }
    }
    session_error.map_or(Ok(()), Err)
}

struct QuicStream {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
}

impl tokio::io::AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(context, buffer)
    }
}

impl tokio::io::AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.send), context, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.send), context)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.send), context)
    }
}

#[must_use]
pub fn plugin() -> TransportRegistration {
    TransportRegistration::new("quic", Plugin)
}
