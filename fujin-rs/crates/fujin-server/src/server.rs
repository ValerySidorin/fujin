#[cfg(any(all(feature = "unix", unix), feature = "quic", feature = "websocket"))]
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[cfg(feature = "websocket")]
use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll, ready},
};

use anyhow::{Context, Result, bail};
#[cfg(feature = "websocket")]
use bytes::{Buf, Bytes, BytesMut};
use fujin_core::{BindMiddlewareRunner, Catalog};
use fujin_runtime::fujin_server_config::{ServerConfig, TlsConfig};
#[cfg(feature = "websocket")]
use futures_util::{Sink, Stream};
#[cfg(feature = "websocket")]
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
#[cfg(feature = "websocket")]
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;

/// Runs every configured listener until shutdown or a terminal listener failure.
///
/// # Errors
///
/// Returns an error when no listener is configured, a configured listener is unavailable in the
/// current feature set, listener startup or serving fails, or a listener task panics.
pub async fn serve(
    config: ServerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut listeners = JoinSet::new();
    let configured = configured_listener_count(&config);
    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let health_ready = Arc::new(AtomicBool::new(false));
    let _ = (&catalog, &bind_middlewares);

    if let Some(listener) = config.tcp {
        #[cfg(not(feature = "tcp"))]
        let _ = listener;
        #[cfg(feature = "tcp")]
        listeners.spawn(serve_tcp(
            listener,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
            ready_sender.clone(),
        ));
        #[cfg(not(feature = "tcp"))]
        bail!("TCP listener configured but fujin-server/tcp is disabled");
    }
    if let Some(listener) = config.unix {
        #[cfg(not(all(feature = "unix", unix)))]
        let _ = listener;
        #[cfg(all(feature = "unix", unix))]
        listeners.spawn(serve_unix(
            listener.path,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
            ready_sender.clone(),
        ));
        #[cfg(not(all(feature = "unix", unix)))]
        bail!("Unix listener configured but unavailable in this build");
    }
    if let Some(listener) = config.websocket {
        #[cfg(not(feature = "websocket"))]
        let _ = listener;
        #[cfg(feature = "websocket")]
        listeners.spawn(serve_websocket(
            listener,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
            ready_sender.clone(),
        ));
        #[cfg(not(feature = "websocket"))]
        bail!("WebSocket listener configured but fujin-server/websocket is disabled");
    }
    if let Some(listener) = config.quic {
        #[cfg(not(feature = "quic"))]
        let _ = listener;
        #[cfg(feature = "quic")]
        listeners.spawn(serve_quic(
            listener,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
            ready_sender.clone(),
        ));
        #[cfg(not(feature = "quic"))]
        bail!("QUIC listener configured but fujin-server/quic is disabled");
    }
    if let Some(listener) = config.grpc {
        #[cfg(not(feature = "grpc"))]
        let _ = listener;
        #[cfg(feature = "grpc")]
        listeners.spawn(serve_grpc(
            listener,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            shutdown.clone(),
            ready_sender.clone(),
        ));
        #[cfg(not(feature = "grpc"))]
        bail!("gRPC listener configured but fujin-server/grpc is disabled");
    }
    if let Some(listener) = config.health {
        listeners.spawn(serve_health(
            listener.listen,
            Arc::clone(&health_ready),
            shutdown.clone(),
            ready_sender.clone(),
        ));
    }
    if configured == 0 {
        bail!("no listeners configured");
    }
    drop(ready_sender);

    wait_for_readiness(&mut listeners, &mut ready_receiver, configured, &shutdown).await?;
    health_ready.store(true, Ordering::Release);
    let result = wait_for_listeners(&mut listeners, &shutdown).await;
    health_ready.store(false, Ordering::Release);
    result
}

fn configured_listener_count(config: &ServerConfig) -> usize {
    [
        config.tcp.is_some(),
        config.unix.is_some(),
        config.websocket.is_some(),
        config.quic.is_some(),
        config.grpc.is_some(),
        config.health.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count()
}

async fn wait_for_readiness(
    listeners: &mut JoinSet<Result<()>>,
    ready: &mut mpsc::UnboundedReceiver<()>,
    expected: usize,
    shutdown: &CancellationToken,
) -> Result<()> {
    let mut started = 0;
    while started < expected {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            signal = ready.recv() => match signal {
                Some(()) => started += 1,
                None => bail!("listener readiness channel closed after {started}/{expected}"),
            },
            result = listeners.join_next() => return listener_stopped(result, shutdown, true),
        }
    }
    Ok(())
}

async fn wait_for_listeners(
    listeners: &mut JoinSet<Result<()>>,
    shutdown: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        () = shutdown.cancelled() => {}
        result = listeners.join_next() => listener_stopped(result, shutdown, false)?,
    }

    shutdown.cancel();
    while let Some(result) = listeners.join_next().await {
        result.context("listener task failed")??;
    }
    Ok(())
}

fn listener_stopped(
    result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>,
    shutdown: &CancellationToken,
    starting: bool,
) -> Result<()> {
    let shutting_down = shutdown.is_cancelled();
    shutdown.cancel();
    match result {
        Some(Ok(Ok(()))) if shutting_down && !starting => Ok(()),
        Some(Ok(Ok(()))) => bail!("listener stopped unexpectedly"),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(error).context("listener task failed"),
        None => bail!("all listeners stopped"),
    }
}

#[cfg(feature = "tcp")]
async fn serve_tcp(
    config: fujin_runtime::fujin_server_config::TcpListenerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    let tls = match config.tls.as_ref() {
        Some(config) => Some(load_tls_acceptor(config).await?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("bind TCP listener {:?}", config.listen))?;
    let _ = ready.send(());
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept TCP connection")?;
                stream.set_nodelay(true).context("configure TCP_NODELAY")?;
                let catalog = Arc::clone(&catalog);
                let bind_middlewares = Arc::clone(&bind_middlewares);
                let build = build.clone();
                let session_shutdown = shutdown.clone();
                let tls = tls.clone();
                sessions.spawn(async move {
                    if let Some(tls) = tls {
                        let stream = tls.accept(stream).await.context("accept TCP TLS")?;
                        fujin_native::run_with_shutdown(
                            stream,
                            catalog,
                            bind_middlewares,
                            build,
                            session_shutdown.cancelled_owned(),
                        )
                        .await
                        .with_context(|| format!("TCP TLS session {peer}"))
                    } else {
                        fujin_native::run_with_shutdown(
                            stream,
                            catalog,
                            bind_middlewares,
                            build,
                            session_shutdown.cancelled_owned(),
                        )
                        .await
                        .with_context(|| format!("TCP session {peer}"))
                    }
                });
            }
        }
    }
    drain_sessions(&mut sessions).await
}

#[cfg(all(feature = "unix", unix))]
async fn serve_unix(
    path: String,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    let listener = tokio::net::UnixListener::bind(&path)
        .with_context(|| format!("bind Unix listener {path:?}"))?;
    let _ = ready.send(());
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept Unix connection")?;
                let catalog = Arc::clone(&catalog);
                let bind_middlewares = Arc::clone(&bind_middlewares);
                let build = build.clone();
                let session_shutdown = shutdown.clone();
                sessions.spawn(async move {
                    fujin_native::run_with_shutdown(
                        stream,
                        catalog,
                        bind_middlewares,
                        build,
                        session_shutdown.cancelled_owned(),
                    )
                    .await
                    .context("Unix session")
                });
            }
        }
    }
    drop(listener);
    let result = drain_sessions(&mut sessions).await;
    match tokio::fs::remove_file(&path).await {
        Ok(()) => result,
        Err(error) if error.kind() == io::ErrorKind::NotFound => result,
        Err(error) => Err(error).with_context(|| format!("remove Unix socket {path:?}")),
    }
}

async fn drain_sessions(sessions: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = sessions.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "session ended with error"),
            Err(error) => return Err(error).context("session task failed"),
        }
    }
    Ok(())
}

#[cfg(feature = "websocket")]
#[derive(Clone)]
struct WebSocketPolicy {
    path: Arc<str>,
    allowed_origins: Arc<[String]>,
    max_message_bytes: usize,
}

#[cfg(feature = "websocket")]
async fn serve_websocket(
    config: fujin_runtime::fujin_server_config::WebSocketListenerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    let tls = match config.tls.as_ref() {
        Some(config) => Some(load_tls_acceptor(config).await?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("bind WebSocket listener {:?}", config.listen))?;
    let policy = WebSocketPolicy {
        path: config.path.into(),
        allowed_origins: config.allowed_origins.into(),
        max_message_bytes: config.max_message_bytes,
    };
    let _ = ready.send(());
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept WebSocket connection")?;
                let catalog = Arc::clone(&catalog);
                let bind_middlewares = Arc::clone(&bind_middlewares);
                let build = build.clone();
                let session_shutdown = shutdown.clone();
                let tls = tls.clone();
                let policy = policy.clone();
                sessions.spawn(async move {
                    if let Some(tls) = tls {
                        let stream = tls.accept(stream).await.context("accept WebSocket TLS")?;
                        websocket_session(stream, catalog, bind_middlewares, build, session_shutdown, policy).await
                    } else {
                        websocket_session(stream, catalog, bind_middlewares, build, session_shutdown, policy).await
                    }
                    .with_context(|| format!("WebSocket session {peer}"))
                });
            }
        }
    }
    drain_sessions(&mut sessions).await
}

#[cfg(feature = "websocket")]
#[derive(Debug)]
pub struct NativeWebSocketStream<S> {
    websocket: WebSocketStream<S>,
    pending: Bytes,
    input_closed: bool,
    pending_write: bool,
}

#[cfg(feature = "websocket")]
impl<S> NativeWebSocketStream<S> {
    pub fn new(websocket: WebSocketStream<S>) -> Self {
        Self {
            websocket,
            pending: Bytes::new(),
            input_closed: false,
            pending_write: false,
        }
    }

    fn websocket_error(error: tokio_tungstenite::tungstenite::Error) -> io::Error {
        io::Error::other(error)
    }
}

#[cfg(feature = "websocket")]
impl<S> AsyncRead for NativeWebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pending_write {
            match Pin::new(&mut self.websocket).poll_flush(context) {
                Poll::Ready(Ok(())) => self.pending_write = false,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(Self::websocket_error(error)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        loop {
            if !self.pending.is_empty() {
                let read = self.pending.len().min(buffer.remaining());
                buffer.put_slice(&self.pending[..read]);
                self.pending.advance(read);
                return Poll::Ready(Ok(()));
            }
            if self.input_closed {
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut self.websocket).poll_next(context)) {
                Some(Ok(Message::Binary(bytes))) => self.pending = bytes,
                Some(Ok(Message::Close(_))) | None => {
                    self.input_closed = true;
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                    self.input_closed = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "only binary WebSocket messages are valid",
                    )));
                }
                Some(Err(error)) => return Poll::Ready(Err(Self::websocket_error(error))),
            }
        }
    }
}

#[cfg(feature = "websocket")]
impl<S> AsyncWrite for NativeWebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.pending_write {
            ready!(Pin::new(&mut self.websocket).poll_flush(context))
                .map_err(Self::websocket_error)?;
            self.pending_write = false;
        }
        ready!(Pin::new(&mut self.websocket).poll_ready(context)).map_err(Self::websocket_error)?;
        Pin::new(&mut self.websocket)
            .start_send(Message::Binary(Bytes::copy_from_slice(buffer)))
            .map_err(Self::websocket_error)?;
        self.pending_write = true;
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let length = buffers.iter().try_fold(0_usize, |length, buffer| {
            length.checked_add(buffer.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "WebSocket write overflow")
            })
        })?;
        if length == 0 {
            return Poll::Ready(Ok(0));
        }
        if self.pending_write {
            ready!(Pin::new(&mut self.websocket).poll_flush(context))
                .map_err(Self::websocket_error)?;
            self.pending_write = false;
        }
        ready!(Pin::new(&mut self.websocket).poll_ready(context)).map_err(Self::websocket_error)?;
        let mut message = BytesMut::with_capacity(length);
        for buffer in buffers {
            message.extend_from_slice(buffer);
        }
        Pin::new(&mut self.websocket)
            .start_send(Message::Binary(message.freeze()))
            .map_err(Self::websocket_error)?;
        self.pending_write = true;
        Poll::Ready(Ok(length))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.websocket).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                self.pending_write = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(Self::websocket_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(context))?;
        Pin::new(&mut self.websocket)
            .poll_close(context)
            .map_err(Self::websocket_error)
    }
}

#[cfg(feature = "websocket")]
#[allow(clippy::result_large_err)]
async fn websocket_session<S>(
    stream: S,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
    policy: WebSocketPolicy,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use tokio_tungstenite::tungstenite::{
        handshake::server::{Request, Response},
        http::StatusCode,
        protocol::WebSocketConfig,
    };

    let config = WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(8 * 1024)
        .max_write_buffer_size(policy.max_message_bytes)
        .max_message_size(Some(policy.max_message_bytes))
        .max_frame_size(Some(policy.max_message_bytes));
    let callback = move |request: &Request, response: Response| {
        if request.uri().path() != policy.path.as_ref() {
            return Err(websocket_rejection(
                StatusCode::NOT_FOUND,
                "WebSocket path not found",
            ));
        }
        if !websocket_origin_allowed(request, &policy.allowed_origins) {
            return Err(websocket_rejection(
                StatusCode::FORBIDDEN,
                "WebSocket origin denied",
            ));
        }
        Ok(response)
    };
    let websocket = tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(config))
        .await
        .context("upgrade WebSocket")?;
    fujin_native::run_with_shutdown(
        NativeWebSocketStream::new(websocket),
        catalog,
        bind_middlewares,
        build,
        shutdown.cancelled_owned(),
    )
    .await
    .map_err(anyhow::Error::from)
}

#[cfg(feature = "websocket")]
fn websocket_rejection(
    status: tokio_tungstenite::tungstenite::http::StatusCode,
    message: &str,
) -> tokio_tungstenite::tungstenite::handshake::server::ErrorResponse {
    let mut response = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some(
        message.to_owned(),
    ));
    *response.status_mut() = status;
    response
}

#[cfg(feature = "websocket")]
fn websocket_origin_allowed(
    request: &tokio_tungstenite::tungstenite::handshake::server::Request,
    allowed: &[String],
) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let Some(origin) = request
        .headers()
        .get("origin")
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    if allowed.iter().any(|value| value == "*") {
        return true;
    }
    let Ok(parsed) = origin.parse::<tokio_tungstenite::tungstenite::http::Uri>() else {
        return false;
    };
    if parsed.scheme().is_none() || parsed.authority().is_none() {
        return false;
    }
    let origin = origin.trim_end_matches('/');
    allowed
        .iter()
        .any(|value| value.trim_end_matches('/') == origin)
}

#[cfg(feature = "quic")]
async fn serve_quic(
    config: fujin_runtime::fujin_server_config::QuicListenerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    let address = config.listen.parse().context("parse QUIC listen address")?;
    let server_config = quic_server_config(
        &config.tls.certificate,
        &config.tls.private_key,
        config.max_incoming_streams,
        config.max_idle_timeout,
        config.keepalive_period,
    )
    .await?;
    let endpoint = quinn::Endpoint::server(server_config, address).context("bind QUIC endpoint")?;
    let _ = ready.send(());
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break; };
                let catalog = Arc::clone(&catalog);
                let bind_middlewares = Arc::clone(&bind_middlewares);
                let build = build.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let connection = incoming.await.context("accept QUIC connection")?;
                    let mut streams = JoinSet::new();
                    loop {
                        tokio::select! {
                            () = connection_shutdown.cancelled() => break,
                            stream = connection.accept_bi() => match stream {
                                Ok((send, recv)) => {
                                    let catalog = Arc::clone(&catalog);
                                    let bind_middlewares = Arc::clone(&bind_middlewares);
                                    let build = build.clone();
                                    let stream_shutdown = connection_shutdown.clone();
                                    streams.spawn(async move {
                                        fujin_native::run_with_shutdown(
                                            QuicStream { recv, send },
                                            catalog,
                                            bind_middlewares,
                                            build,
                                            stream_shutdown.cancelled_owned(),
                                        )
                                        .await
                                        .map_err(anyhow::Error::from)
                                    });
                                }
                                Err(
                                    quinn::ConnectionError::ApplicationClosed(_)
                                    | quinn::ConnectionError::LocallyClosed,
                                ) => break,
                                Err(error) => return Err(error).context("accept QUIC stream"),
                            }
                        }
                    }
                    connection.close(quinn::VarInt::from_u32(0), b"server shutdown");
                    drain_sessions(&mut streams).await
                });
            }
        }
    }
    endpoint.close(quinn::VarInt::from_u32(0), b"server shutdown");
    endpoint.wait_idle().await;
    drain_sessions(&mut connections).await
}

#[cfg(feature = "quic")]
async fn quic_server_config(
    certificate: &str,
    private_key: &str,
    max_incoming_streams: u32,
    max_idle_timeout: Option<std::time::Duration>,
    keepalive_period: Option<std::time::Duration>,
) -> Result<quinn::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    install_rustls_provider();

    let certificate_bytes = tokio::fs::read(certificate)
        .await
        .with_context(|| format!("read QUIC certificate {certificate:?}"))?;
    let private_key_bytes = tokio::fs::read(private_key)
        .await
        .with_context(|| format!("read QUIC private key {private_key:?}"))?;
    let certificates: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut certificate_bytes.as_slice())
            .collect::<std::result::Result<_, _>>()
            .context("parse QUIC certificate")?;
    if certificates.is_empty() {
        bail!("QUIC certificate contains no certificates");
    }
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut private_key_bytes.as_slice())
            .context("parse QUIC private key")?
            .context("QUIC private key file contains no key")?;
    let mut server = quinn::ServerConfig::with_single_cert(certificates, key)
        .context("build QUIC server config")?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(max_incoming_streams));
    if let Some(timeout) = max_idle_timeout {
        transport.max_idle_timeout(Some(timeout.try_into().context("QUIC max idle timeout")?));
    }
    transport.keep_alive_interval(keepalive_period);
    server.transport_config(Arc::new(transport));
    Ok(server)
}

#[cfg(feature = "quic")]
struct QuicStream {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
}

#[cfg(feature = "quic")]
impl tokio::io::AsyncRead for QuicStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.recv).poll_read(context, buffer)
    }
}

#[cfg(feature = "quic")]
impl tokio::io::AsyncWrite for QuicStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.send), context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.send), context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.send), context)
    }
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
async fn load_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor> {
    install_rustls_provider();
    let certificate = tokio::fs::read(&config.certificate)
        .await
        .with_context(|| format!("read TLS certificate {:?}", config.certificate))?;
    let private_key = tokio::fs::read(&config.private_key)
        .await
        .with_context(|| format!("read TLS private key {:?}", config.private_key))?;
    let certificates = rustls_pemfile::certs(&mut certificate.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parse TLS certificate")?;
    if certificates.is_empty() {
        bail!("TLS certificate contains no certificates");
    }
    let private_key = rustls_pemfile::private_key(&mut private_key.as_slice())
        .context("parse TLS private key")?
        .context("TLS private key file contains no key")?;
    let builder = rustls::ServerConfig::builder();
    let server = if config.require_client_certificate {
        let directory = config
            .client_certificates
            .as_ref()
            .context("client certificate directory missing")?;
        let roots_pem = load_pem_directory(directory).await?;
        let mut roots = rustls::RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut roots_pem.as_slice()) {
            roots
                .add(certificate.context("parse client CA certificate")?)
                .context("add client CA certificate")?;
        }
        if roots.is_empty() {
            bail!("client certificate directory contains no certificates");
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("build client certificate verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .context("configure TLS identity")?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .context("configure TLS identity")?
    };
    Ok(TlsAcceptor::from(Arc::new(server)))
}

async fn load_pem_directory(directory: &str) -> Result<Vec<u8>> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .with_context(|| format!("read certificate directory {directory:?}"))?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut output = Vec::new();
    for path in paths {
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read certificate {}", path.display()))?;
        output.extend_from_slice(&bytes);
        output.push(b'\n');
    }
    Ok(output)
}

#[cfg(feature = "grpc")]
async fn serve_grpc(
    config: fujin_runtime::fujin_server_config::GrpcListenerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    use fujin_proto::fujin::v1 as pb;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("bind gRPC listener {:?}", config.listen))?;
    let mut builder = Server::builder()
        .max_concurrent_streams(config.max_concurrent_streams)
        .initial_stream_window_size(config.initial_window_size)
        .initial_connection_window_size(config.initial_connection_window_size)
        .http2_keepalive_interval(config.server_keepalive.time)
        .http2_keepalive_timeout(config.server_keepalive.timeout);
    if let Some(age) = config.server_keepalive.max_connection_age {
        builder = builder.max_connection_age(age);
    }
    if let Some(grace) = config.server_keepalive.max_connection_age_grace {
        builder = builder.max_connection_age_grace(grace);
    }
    if let Some(tls) = config.tls.as_ref() {
        let certificate = tokio::fs::read(&tls.certificate)
            .await
            .with_context(|| format!("read gRPC certificate {:?}", tls.certificate))?;
        let private_key = tokio::fs::read(&tls.private_key)
            .await
            .with_context(|| format!("read gRPC private key {:?}", tls.private_key))?;
        let mut tls_config =
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key));
        if let Some(directory) = tls.client_certificates.as_ref() {
            tls_config = tls_config.client_ca_root(tonic::transport::Certificate::from_pem(
                load_pem_directory(directory).await?,
            ));
        }
        builder = builder
            .tls_config(tls_config)
            .context("configure gRPC TLS")?;
    }
    let mut service = pb::fujin_service_server::FujinServiceServer::new(crate::GrpcService::new(
        catalog,
        bind_middlewares,
    ));
    if let Some(limit) = config.max_recv_message_size {
        service = service.max_decoding_message_size(limit);
    }
    if let Some(limit) = config.max_send_message_size {
        service = service.max_encoding_message_size(limit);
    }
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<pb::fujin_service_server::FujinServiceServer<crate::GrpcService>>()
        .await;
    let _ = ready.send(());
    let shutdown_health = health_reporter.clone();
    builder
        .add_service(health_service)
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.cancelled().await;
            shutdown_health
                .set_not_serving::<
                    pb::fujin_service_server::FujinServiceServer<crate::GrpcService>,
                >()
                .await;
            shutdown_health
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        })
        .await
        .context("serve gRPC")
}

async fn serve_health(
    address: String,
    readiness: Arc<AtomicBool>,
    shutdown: CancellationToken,
    ready: mpsc::UnboundedSender<()>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("bind health listener {address:?}"))?;
    let _ = ready.send(());
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept health connection")?;
                let readiness = Arc::clone(&readiness);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    serve_health_connection(stream, readiness, connection_shutdown).await
                });
            }
        }
    }
    drain_sessions(&mut connections).await
}

async fn serve_health_connection(
    mut stream: tokio::net::TcpStream,
    readiness: Arc<AtomicBool>,
    shutdown: CancellationToken,
) -> Result<()> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut request = Vec::with_capacity(1024);
    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() == MAX_REQUEST_BYTES {
            stream.write_all(HTTP_TOO_LARGE).await?;
            return Ok(());
        }
        let mut buffer = [0_u8; 1024];
        let maximum = buffer.len().min(MAX_REQUEST_BYTES - request.len());
        let read = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            read = stream.read(&mut buffer[..maximum]) => read?,
        };
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let path = request
        .split(|byte| *byte == b' ')
        .nth(1)
        .unwrap_or_default();
    let response = health_response(path, readiness.load(Ordering::Acquire));
    stream.write_all(response).await?;
    stream.shutdown().await?;
    Ok(())
}

const HTTP_HEALTHY: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";
const HTTP_NOT_READY: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 10\r\nConnection: close\r\n\r\nnot ready\n";
const HTTP_NOT_FOUND: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 10\r\nConnection: close\r\n\r\nnot found\n";
const HTTP_TOO_LARGE: &[u8] = b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 18\r\nConnection: close\r\n\r\nrequest too large\n";

fn health_response(path: &[u8], ready: bool) -> &'static [u8] {
    match path {
        b"/healthz" => HTTP_HEALTHY,
        b"/readyz" if ready => HTTP_HEALTHY,
        b"/readyz" => HTTP_NOT_READY,
        _ => HTTP_NOT_FOUND,
    }
}

#[cfg(all(test, feature = "tcp"))]
mod tests {
    use super::*;
    use fujin_core::{DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware};
    use tokio::time::{Duration, timeout};

    async fn empty_catalog() -> Arc<Catalog> {
        let registry = Arc::new(DescriptorRegistry::default());
        let compiler = Arc::new(GenerationCompiler::new(
            registry,
            Arc::new(NoConnectorMiddleware),
        ));
        Arc::new(
            Catalog::compile(&std::collections::BTreeMap::default(), compiler)
                .await
                .expect("compile empty catalog"),
        )
    }

    #[test]
    fn health_response_tracks_readiness() {
        assert!(health_response(b"/healthz", false).starts_with(b"HTTP/1.1 200"));
        assert!(health_response(b"/readyz", false).starts_with(b"HTTP/1.1 503"));
        assert!(health_response(b"/readyz", true).starts_with(b"HTTP/1.1 200"));
        assert!(health_response(b"/missing", true).starts_with(b"HTTP/1.1 404"));
    }

    #[tokio::test]
    async fn cancelled_server_binds_and_drains_tcp_listener() {
        let catalog = empty_catalog().await;
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        serve(
            ServerConfig {
                build: "test".into(),
                tcp: Some(fujin_runtime::fujin_server_config::TcpListenerConfig {
                    listen: "127.0.0.1:0".into(),
                    tls: None,
                }),
                ..ServerConfig::default()
            },
            Arc::clone(&catalog),
            Arc::new(fujin_core::NoBindMiddleware),
            shutdown,
        )
        .await
        .expect("serve and drain TCP listener");
        catalog.close().await.expect("close catalog");
    }

    #[tokio::test]
    async fn readyz_becomes_healthy_after_all_listeners_bind() {
        let tcp_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve TCP address");
        let tcp_address = tcp_probe.local_addr().expect("TCP address");
        drop(tcp_probe);
        let health_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve health address");
        let health_address = health_probe.local_addr().expect("health address");
        drop(health_probe);
        let catalog = empty_catalog().await;
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_catalog = Arc::clone(&catalog);
        let server = tokio::spawn(async move {
            serve(
                ServerConfig {
                    build: "test".into(),
                    tcp: Some(fujin_runtime::fujin_server_config::TcpListenerConfig {
                        listen: tcp_address.to_string(),
                        tls: None,
                    }),
                    health: Some(fujin_runtime::fujin_server_config::SocketListenerConfig {
                        listen: health_address.to_string(),
                    }),
                    ..ServerConfig::default()
                },
                server_catalog,
                Arc::new(fujin_core::NoBindMiddleware),
                server_shutdown,
            )
            .await
        });

        let response = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(mut stream) = tokio::net::TcpStream::connect(health_address).await {
                    stream
                        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                        .await
                        .expect("write health request");
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .await
                        .expect("read health response");
                    break response;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("health listener readiness");
        assert!(response.starts_with(b"HTTP/1.1 200"));

        shutdown.cancel();
        server
            .await
            .expect("server task")
            .expect("serve and drain listeners");
        catalog.close().await.expect("close catalog");
    }
}
#[cfg(all(test, feature = "bench"))]
mod tls_tests {
    use std::sync::Arc;

    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn configured_tls_acceptor_completes_a_real_handshake() {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".into()]).expect("generate certificate");
        let prefix =
            std::env::temp_dir().join(format!("fujin-rust-tls-test-{}", std::process::id()));
        let certificate_path = prefix.with_extension("cert.pem");
        let private_key_path = prefix.with_extension("key.pem");
        tokio::fs::write(&certificate_path, cert.pem())
            .await
            .expect("write certificate");
        tokio::fs::write(&private_key_path, signing_key.serialize_pem())
            .await
            .expect("write private key");
        let acceptor = load_tls_acceptor(&TlsConfig {
            certificate: certificate_path.display().to_string(),
            private_key: private_key_path.display().to_string(),
            client_certificates: None,
            require_client_certificate: false,
        })
        .await
        .expect("load TLS acceptor");
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(cert.der().clone())
            .expect("trust test certificate");
        let connector = tokio_rustls::TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .expect("server name")
            .to_owned();
        let (client_io, server_io) = tokio::io::duplex(1024);
        let (client, server) = tokio::join!(
            connector.connect(server_name, client_io),
            acceptor.accept(server_io)
        );
        let mut client = client.expect("client TLS handshake");
        let mut server = server.expect("server TLS handshake");
        client.write_all(b"fujin").await.expect("write TLS bytes");
        client.flush().await.expect("flush TLS bytes");
        let mut received = [0_u8; 5];
        server
            .read_exact(&mut received)
            .await
            .expect("read TLS bytes");
        assert_eq!(&received, b"fujin");
        tokio::fs::remove_file(certificate_path)
            .await
            .expect("remove certificate");
        tokio::fs::remove_file(private_key_path)
            .await
            .expect("remove private key");
    }
}

#[cfg(all(test, feature = "grpc"))]
mod grpc_health_tests {
    use std::{collections::BTreeMap, sync::Arc};

    use fujin_core::{Catalog, DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware};
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
    };

    use super::*;

    #[tokio::test]
    async fn grpc_health_reports_fujin_service_serving() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve gRPC address");
        let address = probe.local_addr().expect("gRPC address");
        drop(probe);
        let registry = Arc::new(DescriptorRegistry::default());
        let compiler = Arc::new(GenerationCompiler::new(
            registry,
            Arc::new(NoConnectorMiddleware),
        ));
        let catalog = Arc::new(
            Catalog::compile(&BTreeMap::new(), compiler)
                .await
                .expect("compile empty catalog"),
        );
        let shutdown = CancellationToken::new();
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let server_catalog = Arc::clone(&catalog);
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            serve_grpc(
                fujin_runtime::fujin_server_config::GrpcListenerConfig {
                    listen: address.to_string(),
                    max_concurrent_streams: None,
                    max_recv_message_size: None,
                    max_send_message_size: None,
                    initial_window_size: None,
                    initial_connection_window_size: None,
                    server_keepalive:
                        fujin_runtime::fujin_server_config::ServerKeepAliveConfig::default(),
                    tls: None,
                },
                server_catalog,
                Arc::new(fujin_core::NoBindMiddleware),
                server_shutdown,
                ready_tx,
            )
            .await
        });
        timeout(Duration::from_secs(5), ready_rx.recv())
            .await
            .expect("gRPC listener readiness timeout")
            .expect("gRPC listener readiness");
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .expect("health endpoint")
            .connect()
            .await
            .expect("connect health channel");
        let mut client = HealthClient::new(channel);
        let response = client
            .check(HealthCheckRequest {
                service: "fujin.v1.FujinService".into(),
            })
            .await
            .expect("check Fujin gRPC health")
            .into_inner();
        assert_eq!(response.status, ServingStatus::Serving as i32);
        drop(client);
        shutdown.cancel();
        timeout(Duration::from_secs(5), server)
            .await
            .expect("gRPC shutdown timeout")
            .expect("gRPC server task")
            .expect("gRPC server");
        catalog.close().await.expect("close catalog");
    }
}

#[cfg(all(test, feature = "bench"))]
mod websocket_tests {
    use super::*;
    use crate::bench_support::nop_catalog;
    use fujin_native::{RequestCode, ResponseCode};
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        sync::Barrier,
        task::JoinSet,
        time::{Duration, timeout},
    };
    use tokio_tungstenite::{client_async, tungstenite::Message};

    #[test]
    fn websocket_origin_policy_allows_configured_origin_and_rejects_others() {
        let allowed = ["https://console.example".to_owned()];
        let allowed_request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .header("origin", "https://console.example/")
            .body(())
            .expect("allowed request");
        let denied_request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .header("origin", "https://attacker.example")
            .body(())
            .expect("denied request");

        assert!(websocket_origin_allowed(&allowed_request, &allowed));
        assert!(!websocket_origin_allowed(&denied_request, &allowed));
    }

    #[test]
    fn websocket_origin_policy_allows_non_browser_clients_without_origin() {
        let request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .body(())
            .expect("request without origin");

        assert!(websocket_origin_allowed(
            &request,
            &["https://console.example".to_owned()]
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_disconnects_deliver_final_response() {
        const CLIENTS: usize = 32;

        let catalog = nop_catalog().await.expect("compile nop catalog");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test WebSocket listener");
        let address = listener.local_addr().expect("test listener address");
        let start = Arc::new(Barrier::new(CLIENTS + 1));
        let shutdown = CancellationToken::new();
        let mut sessions = JoinSet::new();
        let mut clients = JoinSet::new();

        for _ in 0..CLIENTS {
            let connection = tokio::net::TcpStream::connect(address);
            let accepted = listener.accept();
            let (client_stream, server_stream) = tokio::join!(connection, accepted);
            let client_stream = client_stream.expect("connect WebSocket client");
            let (server_stream, _) = server_stream.expect("accept WebSocket client");
            let server_catalog = Arc::clone(&catalog);
            let server_shutdown = shutdown.clone();
            sessions.spawn(async move {
                websocket_session(
                    server_stream,
                    server_catalog,
                    Arc::new(fujin_core::NoBindMiddleware),
                    "test".into(),
                    server_shutdown,
                    WebSocketPolicy {
                        path: Arc::<str>::from("/"),
                        allowed_origins: Arc::<[String]>::from([]),
                        max_message_bytes: 4 * 1024 * 1024,
                    },
                )
                .await
            });
            let barrier = Arc::clone(&start);
            clients.spawn(async move {
                let (mut websocket, _) = client_async("ws://localhost/", client_stream)
                    .await
                    .expect("upgrade WebSocket client");
                websocket
                    .send(Message::Binary(hello_frame()))
                    .await
                    .expect("send HELLO");
                let hello = receive_binary_bytes(&mut websocket, 12).await;
                assert_eq!(hello.first(), Some(&(ResponseCode::Hello as u8)));
                websocket
                    .send(Message::Binary(bind_frame()))
                    .await
                    .expect("send BIND");
                let bind = receive_binary_bytes(&mut websocket, 17).await;
                assert_eq!(bind.first(), Some(&(ResponseCode::Bind as u8)));
                assert_eq!(bind.get(1), Some(&0));

                barrier.wait().await;
                websocket
                    .send(Message::Binary(bytes::Bytes::from_static(&[
                        RequestCode::Disconnect as u8,
                    ])))
                    .await
                    .expect("send DISCONNECT");
                let disconnect = receive_binary_bytes(&mut websocket, 1).await;
                assert_eq!(disconnect.first(), Some(&(ResponseCode::Disconnect as u8)));
            });
        }

        start.wait().await;
        while let Some(result) = clients.join_next().await {
            result.expect("client task");
        }
        shutdown.cancel();
        while let Some(result) = sessions.join_next().await {
            result.expect("server task").expect("WebSocket session");
        }
        catalog.close().await.expect("close catalog");
    }

    fn hello_frame() -> bytes::Bytes {
        let mut frame = vec![RequestCode::Hello as u8, 1, 1, 1];
        append_bytes(&mut frame, b"test");
        append_bytes(&mut frame, b"dev");
        frame.into()
    }

    fn bind_frame() -> bytes::Bytes {
        let mut frame = vec![RequestCode::Bind as u8];
        append_bytes(&mut frame, b"connector");
        frame.extend_from_slice(&0_u16.to_be_bytes());
        frame.extend_from_slice(&0_u16.to_be_bytes());
        frame.into()
    }

    async fn receive_binary_bytes<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        minimum: usize,
    ) -> Vec<u8>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut received = Vec::with_capacity(minimum);
        while received.len() < minimum {
            let message = timeout(Duration::from_secs(1), websocket.next())
                .await
                .expect("WebSocket response timeout")
                .expect("WebSocket response stream ended")
                .expect("read WebSocket response");
            match message {
                Message::Binary(bytes) => received.extend_from_slice(&bytes),
                message => panic!("unexpected WebSocket response {message:?}"),
            }
        }
        received
    }

    fn append_bytes(frame: &mut Vec<u8>, value: &[u8]) {
        frame.extend_from_slice(
            &u32::try_from(value.len())
                .expect("test value length")
                .to_be_bytes(),
        );
        frame.extend_from_slice(value);
    }
}
