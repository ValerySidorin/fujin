#[cfg(any(all(feature = "unix", unix), feature = "quic"))]
use std::io;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use fujin_core::{BindMiddlewareRunner, Catalog};
use fujin_runtime::fujin_server_config::ServerConfig;
use tokio::task::JoinSet;
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
    let configured = [
        config.tcp.is_some(),
        config.unix.is_some(),
        config.websocket.is_some(),
        config.quic.is_some(),
        config.grpc.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    let _ = (&catalog, &bind_middlewares);

    if let Some(listener) = config.tcp {
        #[cfg(not(feature = "tcp"))]
        let _ = listener;
        #[cfg(feature = "tcp")]
        listeners.spawn(serve_tcp(
            listener.listen,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
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
        ));
        #[cfg(not(all(feature = "unix", unix)))]
        bail!("Unix listener configured but unavailable in this build");
    }
    if let Some(listener) = config.websocket {
        #[cfg(not(feature = "websocket"))]
        let _ = listener;
        #[cfg(feature = "websocket")]
        listeners.spawn(serve_websocket(
            listener.listen,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            config.build.clone(),
            shutdown.clone(),
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
        ));
        #[cfg(not(feature = "quic"))]
        bail!("QUIC listener configured but fujin-server/quic is disabled");
    }
    if let Some(listener) = config.grpc {
        #[cfg(not(feature = "grpc"))]
        let _ = listener;
        #[cfg(feature = "grpc")]
        listeners.spawn(serve_grpc(
            listener.listen,
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
            shutdown.clone(),
        ));
        #[cfg(not(feature = "grpc"))]
        bail!("gRPC listener configured but fujin-server/grpc is disabled");
    }
    if configured == 0 {
        bail!("no listeners configured");
    }

    wait_for_listeners(&mut listeners, &shutdown).await
}

async fn wait_for_listeners(
    listeners: &mut JoinSet<Result<()>>,
    shutdown: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        () = shutdown.cancelled() => {}
        result = listeners.join_next() => match result {
            Some(Ok(Ok(()))) if shutdown.is_cancelled() => {}
            Some(Ok(Ok(()))) => {
                shutdown.cancel();
                bail!("listener stopped unexpectedly");
            }
            Some(Ok(Err(error))) => {
                shutdown.cancel();
                return Err(error);
            }
            Some(Err(error)) => {
                shutdown.cancel();
                return Err(error).context("listener task failed");
            }
            None => bail!("all listeners stopped"),
        },
    }

    shutdown.cancel();
    while let Some(result) = listeners.join_next().await {
        result.context("listener task failed")??;
    }
    Ok(())
}

#[cfg(feature = "tcp")]
async fn serve_tcp(
    address: String,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("bind TCP listener {address:?}"))?;
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
                sessions.spawn(async move {
                    fujin_native::run_with_shutdown(
                        stream,
                        catalog,
                        bind_middlewares,
                        build,
                        session_shutdown.cancelled_owned(),
                    )
                    .await
                    .with_context(|| format!("TCP session {peer}"))
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
) -> Result<()> {
    let listener = tokio::net::UnixListener::bind(&path)
        .with_context(|| format!("bind Unix listener {path:?}"))?;
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

#[cfg(any(
    feature = "tcp",
    all(feature = "unix", unix),
    feature = "websocket",
    feature = "quic"
))]
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
async fn serve_websocket(
    address: String,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("bind WebSocket listener {address:?}"))?;
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
                sessions.spawn(async move {
                    websocket_session(
                        stream,
                        catalog,
                        bind_middlewares,
                        build,
                        session_shutdown,
                    )
                    .await
                    .with_context(|| format!("WebSocket session {peer}"))
                });
            }
        }
    }
    drain_sessions(&mut sessions).await
}

#[cfg(feature = "websocket")]
#[derive(Debug)]
enum WebSocketCompletion {
    Native(Result<()>),
    Inbound(Result<()>),
    Outbound(Result<()>),
    Shutdown,
}

#[cfg(feature = "websocket")]
type WebSocketNativeTask =
    tokio::task::JoinHandle<std::result::Result<(), fujin_native::NativeError>>;

#[cfg(feature = "websocket")]
type WebSocketBridgeTask = tokio::task::JoinHandle<Result<()>>;

#[cfg(feature = "websocket")]
async fn websocket_session(
    stream: tokio::net::TcpStream,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};

    let config = WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(8 * 1024)
        .max_write_buffer_size(4 * 1024 * 1024)
        .max_message_size(Some(4 * 1024 * 1024))
        .max_frame_size(Some(4 * 1024 * 1024));
    let websocket = tokio_tungstenite::accept_async_with_config(stream, Some(config))
        .await
        .context("upgrade WebSocket")?;
    let (mut websocket_sink, mut websocket_source) = websocket.split();
    let (native_stream, bridge) = tokio::io::duplex(64 * 1024);
    let (mut bridge_read, mut bridge_write) = tokio::io::split(bridge);
    let session_shutdown = shutdown.child_token();
    let native_shutdown = session_shutdown.clone();
    let mut native = tokio::spawn(async move {
        fujin_native::run_with_shutdown(
            native_stream,
            catalog,
            bind_middlewares,
            build,
            native_shutdown.cancelled_owned(),
        )
        .await
    });
    let mut inbound = tokio::spawn(async move {
        while let Some(message) = websocket_source.next().await {
            match message.context("read WebSocket message")? {
                Message::Binary(bytes) => bridge_write
                    .write_all(&bytes)
                    .await
                    .context("forward WebSocket input")?,
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Text(_) | Message::Frame(_) => {
                    bail!("only binary WebSocket messages are valid")
                }
            }
        }
        bridge_write
            .shutdown()
            .await
            .context("close WebSocket input")
    });
    let mut outbound = tokio::spawn(async move {
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = bridge_read
                .read(&mut buffer)
                .await
                .context("read native WebSocket output")?;
            if read == 0 {
                websocket_sink
                    .close()
                    .await
                    .context("close WebSocket output")?;
                return Ok::<(), anyhow::Error>(());
            }
            websocket_sink
                .send(Message::Binary(bytes::Bytes::copy_from_slice(
                    &buffer[..read],
                )))
                .await
                .context("send WebSocket output")?;
        }
    });

    let completed = tokio::select! {
        result = &mut native => WebSocketCompletion::Native(
            result.context("join native WebSocket session")?.map_err(anyhow::Error::from),
        ),
        result = &mut inbound => WebSocketCompletion::Inbound(
            result.context("join WebSocket input")?,
        ),
        result = &mut outbound => WebSocketCompletion::Outbound(
            result.context("join WebSocket output")?,
        ),
        () = shutdown.cancelled() => WebSocketCompletion::Shutdown,
    };
    finish_websocket_session(completed, session_shutdown, native, inbound, outbound).await
}

#[cfg(feature = "websocket")]
async fn finish_websocket_session(
    completed: WebSocketCompletion,
    session_shutdown: CancellationToken,
    native: WebSocketNativeTask,
    inbound: WebSocketBridgeTask,
    outbound: WebSocketBridgeTask,
) -> Result<()> {
    session_shutdown.cancel();
    match completed {
        WebSocketCompletion::Native(native_result) => {
            inbound.abort();
            let outbound_result = outbound.await.context("join WebSocket output cleanup")?;
            native_result?;
            outbound_result
        }
        WebSocketCompletion::Inbound(inbound_result) => {
            outbound.abort();
            let native_result = native
                .await
                .context("join native WebSocket cleanup")?
                .map_err(anyhow::Error::from);
            inbound_result?;
            native_result
        }
        WebSocketCompletion::Outbound(outbound_result) => {
            inbound.abort();
            let native_result = native
                .await
                .context("join native WebSocket cleanup")?
                .map_err(anyhow::Error::from);
            outbound_result?;
            native_result
        }
        WebSocketCompletion::Shutdown => {
            inbound.abort();
            outbound.abort();
            native
                .await
                .context("join native WebSocket shutdown")?
                .map_err(anyhow::Error::from)
        }
    }
}

#[cfg(feature = "quic")]
async fn serve_quic(
    config: fujin_runtime::fujin_server_config::QuicListenerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
) -> Result<()> {
    let address = config.listen.parse().context("parse QUIC listen address")?;
    let server_config = quic_server_config(
        &config.certificate,
        &config.private_key,
        config.max_incoming_streams,
    )
    .await?;
    let endpoint = quinn::Endpoint::server(server_config, address).context("bind QUIC endpoint")?;
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
) -> Result<quinn::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

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

#[cfg(feature = "grpc")]
async fn serve_grpc(
    address: String,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
) -> Result<()> {
    use fujin_proto::fujin::v1 as pb;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("bind gRPC listener {address:?}"))?;
    let service = crate::GrpcService::new(catalog, bind_middlewares);
    Server::builder()
        .add_service(pb::fujin_service_server::FujinServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown.cancelled_owned())
        .await
        .context("serve gRPC")
}

#[cfg(all(test, feature = "tcp"))]
mod tests {
    use super::*;
    use fujin_core::{DescriptorRegistry, GenerationCompiler, NoConnectorMiddleware};

    #[tokio::test]
    async fn cancelled_server_binds_and_drains_tcp_listener() {
        let registry = Arc::new(DescriptorRegistry::default());
        let compiler = Arc::new(GenerationCompiler::new(
            registry,
            Arc::new(NoConnectorMiddleware),
        ));
        let catalog = Arc::new(
            Catalog::compile(&std::collections::BTreeMap::default(), compiler)
                .await
                .expect("compile empty catalog"),
        );
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        serve(
            ServerConfig {
                build: "test".into(),
                tcp: Some(fujin_runtime::fujin_server_config::SocketListenerConfig {
                    listen: "127.0.0.1:0".into(),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_disconnects_deliver_final_response() {
        const CLIENTS: usize = 32;

        let catalog = nop_catalog().await.expect("compile nop catalog");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
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
