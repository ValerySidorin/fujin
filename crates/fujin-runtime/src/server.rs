#[cfg(feature = "grpc")]
mod grpc_listener;
mod health;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, bail};
use fujin_configurator::server_config::{
    ControlPlaneConfig, GrpcListenerConfig, SocketListenerConfig,
};
use fujin_transport::{
    ConfiguredTransport, Endpoint, InheritedListeners, ListenerRegistry, TransportContext,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::native::NativeSessions;
use fujin_connector::Catalog;
use fujin_middleware::BindMiddlewareRunner;
#[cfg(feature = "grpc")]
use grpc_listener::serve as serve_grpc;
use health::serve as serve_health;

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
    let native_sessions = Arc::new(NativeSessions::new(
        Arc::clone(&catalog),
        Arc::clone(&bind_middlewares),
        config.build.clone(),
        shutdown.clone(),
    ));
    let context = ListenerContext::new(
        native_sessions,
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
        listeners.spawn(serve_grpc(
            listener,
            context.clone(),
            Arc::clone(&catalog),
            Arc::clone(&bind_middlewares),
        ));
        #[cfg(not(feature = "grpc"))]
        bail!("gRPC listener configured but fujin-runtime/grpc is disabled");
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
