//! Optional native Fujin v1 transport over QUIC bidirectional streams.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use fujin_transport::{
    BoxFuture, CompiledTransport, Endpoint, ListenerMetadata, TransportContext, TransportPlugin,
    TransportRegistration,
    listener::bind_udp,
    settings::{NativeProtocolSettings, TlsSettings},
    tls::TlsConfig,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use serde_json::Value;

const FUJIN_ALPN: &[u8] = b"fujin";
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tokio_util::sync::CancellationToken;

fn default_max_concurrent_bidi_streams() -> u32 {
    1024
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    addr: String,
    #[serde(default = "default_max_concurrent_bidi_streams")]
    max_concurrent_bidi_streams: u32,
    #[serde(default)]
    max_idle_timeout: Option<String>,
    #[serde(default)]
    keep_alive_interval: Option<String>,
    #[serde(default)]
    stream_receive_window: Option<u64>,
    #[serde(default)]
    receive_window: Option<u64>,
    #[serde(default)]
    send_window: Option<u64>,
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
        let native = settings.fujin.compile()?;
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
            max_concurrent_bidi_streams: settings.max_concurrent_bidi_streams,
            max_idle_timeout: parse_duration(
                "QUIC max_idle_timeout",
                settings.max_idle_timeout.as_deref(),
            )?,
            keep_alive_interval: parse_duration(
                "QUIC keep_alive_interval",
                settings.keep_alive_interval.as_deref(),
            )?,
            stream_receive_window: settings.stream_receive_window,
            receive_window: settings.receive_window,
            send_window: settings.send_window,
            native,
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
    max_concurrent_bidi_streams: u32,
    max_idle_timeout: Option<Duration>,
    keep_alive_interval: Option<Duration>,
    stream_receive_window: Option<u64>,
    receive_window: Option<u64>,
    send_window: Option<u64>,
    native: fujin_transport::settings::NativeProtocolConfig,
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
                    connections.spawn(serve_connection(incoming, context.clone(), self.native.clone()));
                }
            }
        }
        let result = fujin_transport::listener::drain_tasks(&mut connections).await;
        endpoint.close(quinn::VarInt::from_u32(0), b"server shutdown");
        endpoint.wait_idle().await;
        result
    }

    async fn server_config(&self) -> Result<quinn::ServerConfig> {
        let (certificates, key) = fujin_transport::tls::load_identity(&self.tls, "QUIC").await?;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Self::server_crypto(
            certificates,
            key,
        )?)
        .context("build QUIC server crypto config")?;
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(quinn::VarInt::from_u32(self.max_concurrent_bidi_streams));
        if let Some(timeout) = self.max_idle_timeout {
            transport.max_idle_timeout(Some(timeout.try_into().context("QUIC max idle timeout")?));
        }
        transport.keep_alive_interval(self.keep_alive_interval);
        if let Some(window) = self.stream_receive_window {
            transport
                .stream_receive_window(window.try_into().context("QUIC stream receive window")?);
        }
        if let Some(window) = self.receive_window {
            transport.receive_window(window.try_into().context("QUIC receive window")?);
        }
        if let Some(window) = self.send_window {
            transport.send_window(window);
        }
        server.transport_config(Arc::new(transport));
        Ok(server)
    }
    fn server_crypto(
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<rustls::ServerConfig> {
        let mut crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .context("build QUIC server TLS config")?;
        crypto.alpn_protocols = vec![FUJIN_ALPN.to_vec()];
        Ok(crypto)
    }
}

async fn serve_connection(
    incoming: quinn::Incoming,
    context: TransportContext,
    native: fujin_transport::settings::NativeProtocolConfig,
) -> Result<()> {
    let connection = incoming.await.context("accept QUIC connection")?;
    let shutdown: CancellationToken = context.shutdown();
    let session_config = native.session_config(false);
    let mut streams = FuturesUnordered::new();
    let mut session_error = None;
    let mut ping = interval_at(Instant::now() + native.ping_interval, native.ping_interval);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping_failures = 0;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            stream = connection.accept_bi() => match stream {
                Ok((send, recv)) => {
                    let session = context.clone();
                    let config = session_config.clone();
                    streams.push(async move {
                        session.serve_native_stream_with_config(QuicStream { recv, send }, config).await
                    });
                }
                Err(quinn::ConnectionError::ApplicationClosed(_) | quinn::ConnectionError::LocallyClosed) => break,
                Err(error) => return Err(error).context("accept QUIC stream"),
            },
            result = streams.next(), if !streams.is_empty() => {
                if let Some(Err(error)) = result && session_error.is_none() { session_error = Some(error); }
            }
            _ = ping.tick() => {
                match ping_quic_connection(&connection, native.ping_timeout).await {
                    Ok(()) => ping_failures = 0,
                    Err(error) => {
                        ping_failures += 1;
                        if ping_failures >= native.ping_max_retries {
                            connection.close(quinn::VarInt::from_u32(1), b"PING timeout");
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
    while let Some(result) = streams.next().await {
        if let Err(error) = result
            && session_error.is_none()
        {
            session_error = Some(error);
        }
    }
    connection.close(quinn::VarInt::from_u32(0), b"server shutdown");
    session_error.map_or(Ok(()), Err)
}

async fn ping_quic_connection(connection: &quinn::Connection, deadline: Duration) -> Result<()> {
    timeout(deadline, async {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .context("open QUIC PING stream")?;
        send.write_all(&[fujin_transport::NATIVE_PING_OPCODE])
            .await
            .context("write QUIC PING")?;
        send.finish().context("finish QUIC PING request")?;
        let mut response = [0_u8; 1];
        recv.read_exact(&mut response)
            .await
            .context("read QUIC PONG")?;
        if response[0] != fujin_transport::NATIVE_PING_OPCODE {
            bail!("invalid QUIC PONG opcode {}", response[0]);
        }
        Result::<()>::Ok(())
    })
    .await
    .context("QUIC PING timeout")?
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

    #[test]
    fn server_crypto_advertises_fujin_alpn() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate test certificate");
        let certificate_der = CertificateDer::from(certificate.cert.der().to_vec());
        let private_key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());

        let crypto = Transport::server_crypto(vec![certificate_der], private_key.into())
            .expect("build server crypto");

        assert_eq!(crypto.alpn_protocols, vec![FUJIN_ALPN.to_vec()]);
    }

    async fn connected_pair() -> (
        quinn::Endpoint,
        quinn::Connection,
        quinn::Endpoint,
        quinn::Connection,
    ) {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate test certificate");
        let certificate_der = CertificateDer::from(certificate.cert.der().to_vec());
        let private_key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());
        let server_config = quinn::ServerConfig::with_single_cert(
            vec![certificate_der.clone()],
            private_key.into(),
        )
        .expect("build test server config");
        let server = quinn::Endpoint::server(
            server_config,
            "127.0.0.1:0".parse().expect("server address"),
        )
        .expect("start test server");

        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate_der).expect("trust test certificate");
        let rustls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(rustls)
            .expect("build QUIC client crypto");
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().expect("client address"))
            .expect("start test client");
        client.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        let server_address = server.local_addr().expect("server local address");
        let connecting = client
            .connect(server_address, "localhost")
            .expect("connect test client");
        let incoming = server.accept().await.expect("accept test connection");
        let (server_connection, client_connection) = tokio::join!(incoming, connecting);
        (
            server,
            server_connection.expect("accept test connection"),
            client,
            client_connection.expect("connect test client"),
        )
    }

    #[tokio::test]
    async fn dedicated_ping_accepts_pong_without_eof_dependency() {
        let (_server, server_connection, _client, client_connection) = connected_pair().await;
        let connection_guard = client_connection.clone();
        let responder = tokio::spawn(async move {
            let (mut send, mut recv) = client_connection
                .accept_bi()
                .await
                .expect("accept PING stream");
            let mut request = [0_u8; 1];
            recv.read_exact(&mut request).await.expect("read PING");
            assert_eq!(request[0], fujin_transport::NATIVE_PING_OPCODE);
            send.write_all(&[fujin_transport::NATIVE_PING_OPCODE])
                .await
                .expect("write PONG");
            send.finish().expect("finish PONG");
        });
        ping_quic_connection(&server_connection, Duration::from_secs(1))
            .await
            .expect("complete PING");
        drop(connection_guard);
        responder.await.expect("join PONG responder");
    }

    #[tokio::test]
    async fn dedicated_ping_times_out_without_pong() {
        let (_server, server_connection, _client, client_connection) = connected_pair().await;
        let responder = tokio::spawn(async move {
            let (_send, mut recv) = client_connection
                .accept_bi()
                .await
                .expect("accept PING stream");
            let mut request = [0_u8; 1];
            recv.read_exact(&mut request).await.expect("read PING");
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        assert!(
            ping_quic_connection(&server_connection, Duration::from_millis(10))
                .await
                .is_err()
        );
        responder.abort();
    }
}
