#[cfg(unix)]
use std::os::fd::AsFd;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, bail};
use fujin_core::{BindMiddlewareRunner, Catalog};
use fujin_runtime::fujin_server_config::{
    ControlPlaneConfig, GrpcListenerConfig, SocketListenerConfig,
};
use fujin_transport::{ConfiguredTransport, Endpoint, TransportContext};
use fujin_upgrade::{InheritedListeners, ListenerMetadata, ListenerRegistry};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

/// Fully compiled server plan. Native transport settings have already been validated by plugins.
#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    pub build: String,
    pub transports: Vec<ConfiguredTransport>,
    pub grpc: Option<GrpcListenerConfig>,
    pub health: Option<SocketListenerConfig>,
}

impl ServerConfig {
    #[must_use]
    pub fn from_control_plane(
        control_plane: ControlPlaneConfig,
        transports: Vec<ConfiguredTransport>,
    ) -> Self {
        Self {
            build: control_plane.build,
            transports,
            grpc: control_plane.grpc,
            health: control_plane.health,
        }
    }
}

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
    let registry = ListenerRegistry::new(configured_listener_count(&config));
    serve_inner(
        config,
        catalog,
        bind_middlewares,
        shutdown,
        None,
        registry,
        InheritedListeners::default(),
    )
    .await
}

/// Runs every configured listener and reports after all listeners are accepting connections.
///
/// # Errors
///
/// Returns the same startup and serving errors as [`serve`].
pub async fn serve_with_readiness(
    config: ServerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
    ready: oneshot::Sender<Vec<Endpoint>>,
) -> Result<()> {
    let registry = ListenerRegistry::new(configured_listener_count(&config));
    serve_inner(
        config,
        catalog,
        bind_middlewares,
        shutdown,
        Some(ready),
        registry,
        InheritedListeners::default(),
    )
    .await
}

/// Runs every configured listener with upgrade descriptor registration and inheritance.
///
/// # Errors
///
/// Returns the same startup and serving errors as [`serve`], and rejects a registry whose
/// expected listener count differs from the configured listener count.
pub async fn serve_with_readiness_and_upgrade(
    config: ServerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
    ready: oneshot::Sender<Vec<Endpoint>>,
    registry: ListenerRegistry,
    inherited: InheritedListeners,
) -> Result<()> {
    serve_inner(
        config,
        catalog,
        bind_middlewares,
        shutdown,
        Some(ready),
        registry,
        inherited,
    )
    .await
}

type ListenerContext = TransportContext;

async fn serve_inner(
    config: ServerConfig,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    shutdown: CancellationToken,
    ready: Option<oneshot::Sender<Vec<Endpoint>>>,
    registry: ListenerRegistry,
    inherited: InheritedListeners,
) -> Result<()> {
    let mut listeners = JoinSet::new();
    let configured = require_configured_listener_count(&config)?;
    if registry.expected() != configured {
        bail!(
            "upgrade listener registry expects {}, but {configured} listeners are configured",
            registry.expected()
        );
    }
    let (ready_sender, ready_receiver) = mpsc::unbounded_channel();
    let health_ready = Arc::new(AtomicBool::new(false));
    let context = ListenerContext::new(
        catalog,
        bind_middlewares,
        config.build.clone(),
        shutdown.clone(),
        ready_sender,
        registry,
        inherited,
    );

    for transport in config.transports {
        listeners.spawn(transport.serve(context.clone()));
    }
    if let Some(listener) = config.grpc {
        #[cfg(not(feature = "grpc"))]
        let _ = listener;
        #[cfg(feature = "grpc")]
        listeners.spawn(serve_grpc(listener, context.clone()));
        #[cfg(not(feature = "grpc"))]
        bail!("gRPC listener configured but fujin-server/grpc is disabled");
    }
    if let Some(listener) = config.health {
        listeners.spawn(serve_health(
            listener.listen,
            Arc::clone(&health_ready),
            context.clone(),
        ));
    }
    drop(context);

    serve_ready_listeners(
        listeners,
        ready_receiver,
        configured,
        shutdown,
        health_ready,
        ready,
    )
    .await
}

async fn serve_ready_listeners(
    mut listeners: JoinSet<Result<()>>,
    mut ready_receiver: mpsc::UnboundedReceiver<Endpoint>,
    configured: usize,
    shutdown: CancellationToken,
    health_ready: Arc<AtomicBool>,
    ready: Option<oneshot::Sender<Vec<Endpoint>>>,
) -> Result<()> {
    let endpoints =
        wait_for_readiness(&mut listeners, &mut ready_receiver, configured, &shutdown).await?;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    health_ready.store(true, Ordering::Release);
    if let Some(ready) = ready {
        let _ = ready.send(endpoints);
    }
    let result = wait_for_listeners(&mut listeners, &shutdown).await;
    health_ready.store(false, Ordering::Release);
    result
}

fn require_configured_listener_count(config: &ServerConfig) -> Result<usize> {
    let configured = configured_listener_count(config);
    if configured == 0 {
        bail!("no listeners configured");
    }
    Ok(configured)
}

pub fn configured_listener_count(config: &ServerConfig) -> usize {
    config
        .transports
        .iter()
        .map(ConfiguredTransport::listener_count)
        .sum::<usize>()
        + usize::from(config.grpc.is_some())
        + usize::from(config.health.is_some())
}

async fn wait_for_readiness(
    listeners: &mut JoinSet<Result<()>>,
    ready: &mut mpsc::UnboundedReceiver<Endpoint>,
    expected: usize,
    shutdown: &CancellationToken,
) -> Result<Vec<Endpoint>> {
    let mut endpoints = Vec::with_capacity(expected);
    while endpoints.len() < expected {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(endpoints),
            signal = ready.recv() => match signal {
                Some(endpoint) => endpoints.push(endpoint),
                None => bail!(
                    "listener readiness channel closed after {}/{}",
                    endpoints.len(),
                    expected
                ),
            },
            result = listeners.join_next() => return listener_stopped(result, shutdown, true).map(|()| endpoints),
        }
    }
    Ok(endpoints)
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

async fn bind_tcp_listener(
    address: &str,
    metadata: ListenerMetadata,
    registry: &ListenerRegistry,
    inherited: &InheritedListeners,
) -> Result<tokio::net::TcpListener> {
    #[cfg(unix)]
    let listener = if let Some(fd) = inherited.take(&metadata) {
        let listener = std::net::TcpListener::from(fd);
        listener
            .set_nonblocking(true)
            .context("configure inherited TCP listener")?;
        tokio::net::TcpListener::from_std(listener).context("inherit TCP listener")?
    } else {
        tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind TCP listener {address:?}"))?
    };
    #[cfg(not(unix))]
    let listener = {
        let _ = (metadata, registry, inherited);
        tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind TCP listener {address:?}"))?
    };
    #[cfg(unix)]
    registry.register(
        metadata,
        listener
            .as_fd()
            .try_clone_to_owned()
            .context("clone TCP listener descriptor")?,
    )?;
    Ok(listener)
}

#[cfg(feature = "grpc")]
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
    context: ListenerContext,
) -> Result<()> {
    use fujin_proto::fujin::v1 as pb;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    let catalog = context.catalog();
    let bind_middlewares = context.bind_middlewares();
    let shutdown = context.shutdown();
    let metadata = ListenerMetadata::grpc(config.listen.clone());
    let listener = bind_tcp_listener(
        &config.listen,
        metadata,
        context.listener_registry(),
        context.inherited_listeners(),
    )
    .await?;
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
    context.signal_ready(Endpoint::grpc(
        listener
            .local_addr()
            .context("read gRPC listener address")?
            .to_string(),
        config.tls.is_some(),
    ));
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
    context: ListenerContext,
) -> Result<()> {
    let shutdown = context.shutdown();
    let metadata = ListenerMetadata::tcp(address.clone());
    let listener = bind_tcp_listener(
        &address,
        metadata,
        context.listener_registry(),
        context.inherited_listeners(),
    )
    .await?;
    context.signal_ready(Endpoint::health(
        listener
            .local_addr()
            .context("read health listener address")?
            .to_string(),
    ));
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
    fujin_transport::listener::drain_tasks(&mut connections).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_tracks_readiness() {
        assert!(health_response(b"/healthz", false).starts_with(b"HTTP/1.1 200"));
        assert!(health_response(b"/readyz", false).starts_with(b"HTTP/1.1 503"));
        assert!(health_response(b"/readyz", true).starts_with(b"HTTP/1.1 200"));
        assert!(health_response(b"/missing", true).starts_with(b"HTTP/1.1 404"));
    }
}
#[cfg(all(test, feature = "grpc"))]
mod grpc_health_tests {
    use std::{collections::BTreeMap, sync::Arc};

    use fujin_core::{Catalog, ConnectorRegistry, GenerationCompiler, NoConnectorMiddleware};
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
        let registry = Arc::new(ConnectorRegistry::default());
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
                TransportContext::new(
                    server_catalog,
                    Arc::new(fujin_core::NoBindMiddleware),
                    "test".into(),
                    server_shutdown,
                    ready_tx,
                    ListenerRegistry::new(1),
                    InheritedListeners::default(),
                ),
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
