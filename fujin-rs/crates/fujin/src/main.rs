#[cfg(not(any(feature = "configurator-yaml", feature = "configurator-env")))]
compile_error!("the fujin binary requires at least one configurator feature");

use std::{env, sync::Arc};

use anyhow::{Context, Result, bail};
use fujin_core::{ConnectorRegistry, NoBindMiddleware};
#[cfg(feature = "configurator-env")]
use fujin_runtime::configurator::EnvConfigurator;
#[cfg(feature = "configurator-yaml")]
use fujin_runtime::configurator::YamlConfigurator;
use fujin_runtime::configurator::{
    Configurator, ConfiguratorRegistry, ConnectorReloader, ConnectorRuntime, ConnectorSnapshot,
    RuntimeController, RuntimeQueue, bootstrap_snapshot, selected_configurator,
};
use fujin_runtime::fujin_server_config::ControlPlaneConfig;
use fujin_upgrade::{InheritedListeners, ListenerRegistry};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{
    EnvFilter, Registry, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

const BUILD_VERSION: &str = env!("FUJIN_BUILD_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    let log_filter = configure_logging()?;

    if parse_arguments()? {
        return Ok(());
    }
    run_application(bootstrap().await?, log_filter).await
}

struct UpgradeSetup {
    client: Option<fujin_upgrade::UpgradeClient>,
    inherited: InheritedListeners,
    registry: ListenerRegistry,
    socket: String,
    task: Option<JoinHandle<()>>,
}

async fn prepare_upgrade(
    server_config: &ControlPlaneConfig,
    upgrade_shutdown: &CancellationToken,
    shutdown_requested: &CancellationToken,
) -> Result<UpgradeSetup> {
    let client = fujin_upgrade::request_from_environment()
        .await
        .context("request inherited listeners from old Fujin process")?;
    let inherited = client.as_ref().map_or_else(
        InheritedListeners::default,
        fujin_upgrade::UpgradeClient::inherited,
    );
    if !inherited.is_empty() {
        tracing::info!(listeners = ?inherited.keys(), "received inherited listeners");
    }
    let registry = ListenerRegistry::new(fujin_server::configured_listener_count(server_config));
    let socket = fujin_upgrade::socket_path_from_environment();
    let task = client.is_none().then(|| {
        spawn_upgrade_listener(
            socket.clone(),
            registry.clone(),
            upgrade_shutdown.clone(),
            shutdown_requested.clone(),
        )
    });
    Ok(UpgradeSetup {
        client,
        inherited,
        registry,
        socket,
        task,
    })
}

async fn complete_upgrade(
    client: fujin_upgrade::UpgradeClient,
    socket: &str,
    registry: &ListenerRegistry,
    upgrade_shutdown: &CancellationToken,
    shutdown_requested: &CancellationToken,
) -> Result<JoinHandle<()>> {
    client
        .signal_ready()
        .await
        .context("signal readiness to old Fujin process")?;
    fujin_upgrade::wait_for_socket_release(socket)
        .await
        .context("wait for old upgrade socket release")?;
    tracing::info!("old Fujin process acknowledged listener handoff");
    Ok(spawn_upgrade_listener(
        socket.to_owned(),
        registry.clone(),
        upgrade_shutdown.clone(),
        shutdown_requested.clone(),
    ))
}

fn spawn_server(
    server_config: ControlPlaneConfig,
    catalog: Arc<fujin_core::Catalog>,
    shutdown: CancellationToken,
    registry: ListenerRegistry,
    inherited: InheritedListeners,
) -> (
    JoinHandle<Result<()>>,
    tokio::sync::oneshot::Receiver<Vec<fujin_server::Endpoint>>,
) {
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(fujin_server::serve_with_readiness_and_upgrade(
        server_config,
        catalog,
        Arc::new(NoBindMiddleware),
        shutdown,
        ready_sender,
        registry,
        inherited,
    ));
    (task, ready_receiver)
}

async fn run_application(bootstrap: Bootstrap, log_filter: LogFilterHandle) -> Result<()> {
    let Bootstrap {
        configurator,
        catalog,
        controller,
        server_config,
    } = bootstrap;

    let shutdown_requested = CancellationToken::new();
    let runtime_shutdown = CancellationToken::new();
    let server_shutdown = CancellationToken::new();
    let upgrade_shutdown = CancellationToken::new();
    let signal_task = spawn_shutdown_loop(shutdown_requested.clone())?;
    #[cfg(unix)]
    let reload_task = Some(
        spawn_reload_loop(
            Arc::clone(&configurator),
            Arc::clone(&controller),
            runtime_shutdown.clone(),
            log_filter,
        )
        .await?,
    );
    #[cfg(not(unix))]
    let reload_task: Option<JoinHandle<()>> = {
        drop(log_filter);
        None
    };

    let UpgradeSetup {
        client: mut upgrade_client,
        inherited,
        registry: listener_registry,
        socket: upgrade_socket,
        task: mut upgrade_task,
    } = prepare_upgrade(&server_config, &upgrade_shutdown, &shutdown_requested).await?;
    let (mut server_task, ready_receiver) = spawn_server(
        server_config,
        Arc::clone(&catalog),
        server_shutdown.clone(),
        listener_registry.clone(),
        inherited,
    );
    let mut server_result = None;
    let watcher_task = tokio::select! {
        ready = ready_receiver => {
            if ready.is_err() {
                server_result = Some((&mut server_task).await.context("join Fujin server task")?);
                None
            } else {
                tracing::info!("all configured listeners are ready");
                if let Some(client) = upgrade_client.take() {
                    upgrade_task = Some(
                        complete_upgrade(
                            client,
                            &upgrade_socket,
                            &listener_registry,
                            &upgrade_shutdown,
                            &shutdown_requested,
                        )
                        .await?,
                    );
                }
                start_connector_watcher(
                    Arc::clone(&configurator),
                    Arc::clone(&controller),
                    runtime_shutdown.clone(),
                )
            }
        }
        result = &mut server_task => {
            server_result = Some(result.context("join Fujin server task")?);
            None
        }
        () = shutdown_requested.cancelled() => None,
    };

    if server_result.is_none() && !shutdown_requested.is_cancelled() {
        tokio::select! {
            result = &mut server_task => {
                server_result = Some(result.context("join Fujin server task")?);
            }
            () = shutdown_requested.cancelled() => {}
        }
    }

    runtime_shutdown.cancel();
    settle_task(watcher_task, "connector watcher").await?;
    settle_task(reload_task, "SIGHUP reload loop").await?;

    upgrade_shutdown.cancel();
    settle_task(upgrade_task, "upgrade listener").await?;
    server_shutdown.cancel();
    if server_result.is_none() {
        server_result = Some(server_task.await.context("join Fujin server task")?);
    }
    shutdown_requested.cancel();
    signal_task.await.context("join shutdown signal task")?;

    let catalog_result = catalog.close().await;
    server_result
        .expect("server result recorded")
        .context("serve Fujin")?;
    catalog_result.context("close connector catalog")?;
    Ok(())
}

struct Bootstrap {
    configurator: Arc<dyn Configurator>,
    catalog: Arc<fujin_core::Catalog>,
    controller: Arc<RuntimeController>,
    server_config: ControlPlaneConfig,
}

async fn bootstrap() -> Result<Bootstrap> {
    let mut configurators = ConfiguratorRegistry::default();
    #[cfg(feature = "configurator-yaml")]
    configurators
        .register("yaml", || {
            Ok(Arc::new(YamlConfigurator::from_environment()))
        })
        .context("register yaml configurator")?;
    #[cfg(feature = "configurator-env")]
    configurators
        .register("env", || Ok(Arc::new(EnvConfigurator)))
        .context("register env configurator")?;
    let configurator = selected_configurator(&configurators)
        .context("select Fujin configurator from FUJIN_CONFIGURATOR")?;
    let config = configurator
        .load()
        .await
        .context("load Fujin bootstrap configuration")?;
    let server_config = config
        .server_config(BUILD_VERSION)
        .context("validate Fujin server configuration")?;

    let registry = Arc::new(ConnectorRegistry::default());
    #[cfg(feature = "kafka")]
    registry
        .register("kafka_franz", fujin_kafka::descriptor())
        .context("register Kafka connector")?;
    tracing::info!(
        configurators = ?configurators.list(),
        connectors = ?registry.list(),
        "registered plugins"
    );

    let initial_snapshot = bootstrap_snapshot(configurator.as_ref(), &config)
        .context("validate configurator bootstrap snapshot")?;
    let catalog = fujin_runtime::compile_catalog(&config, Arc::clone(&registry))
        .await
        .context("compile connector catalog")?;
    let reloader: Arc<dyn ConnectorReloader> = catalog.clone();
    let controller =
        RuntimeController::new(reloader, registry.list(), &initial_snapshot, BUILD_VERSION)
            .context("create runtime connector controller")?;
    Ok(Bootstrap {
        configurator,
        catalog,
        controller,
        server_config,
    })
}

fn parse_arguments() -> Result<bool> {
    let mut arguments = env::args().skip(1);
    let Some(argument) = arguments.next() else {
        return Ok(false);
    };
    if argument == "--version" && arguments.next().is_none() {
        println!("version: {BUILD_VERSION}");
        return Ok(true);
    }
    bail!("unexpected argument {argument:?}; select configuration with FUJIN_CONFIGURATOR")
}

fn spawn_upgrade_listener(
    socket_path: String,
    registry: ListenerRegistry,
    shutdown: CancellationToken,
    drain: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result =
            fujin_upgrade::listen_for_upgrade(socket_path, registry, shutdown.clone(), drain).await;
        if let Err(error) = result
            && !shutdown.is_cancelled()
        {
            tracing::error!(%error, "upgrade listener terminated");
        }
    })
}

fn start_connector_watcher(
    configurator: Arc<dyn Configurator>,
    controller: Arc<RuntimeController>,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if !configurator.watches_connectors() {
        return None;
    }
    Some(tokio::spawn(async move {
        let queue = RuntimeQueue::new(controller);
        let runtime: Arc<dyn ConnectorRuntime> = queue.clone();
        let result = configurator
            .watch_connectors(runtime, shutdown.clone())
            .await;
        queue.close().await;
        if shutdown.is_cancelled() {
            return;
        }
        match result {
            Ok(()) => tracing::warn!("connector watcher terminated"),
            Err(error) => tracing::error!(%error, "connector watcher terminated"),
        }
    }))
}

#[cfg(unix)]
async fn spawn_reload_loop(
    configurator: Arc<dyn Configurator>,
    controller: Arc<RuntimeController>,
    shutdown: CancellationToken,
    log_filter: LogFilterHandle,
) -> Result<JoinHandle<()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut hangup = signal(SignalKind::hangup()).context("install SIGHUP handler")?;
    let mut revision = controller.active_revision().await;
    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
                signal = hangup.recv() => {
                    if signal.is_none() {
                        return;
                    }
                    if let Err(error) = log_filter.reload(log_filter_from_environment()) {
                        tracing::error!(%error, "reload log level");
                    }
                    tracing::info!("received SIGHUP, reloading configuration");
                    if configurator.watches_connectors() {
                        continue;
                    }
                    match configurator.load().await {
                        Ok(config) => {
                            revision = revision.saturating_add(1);
                            let result = controller.apply(
                                ConnectorSnapshot {
                                    revision,
                                    connectors: config.connectors,
                                },
                                &shutdown,
                            ).await;
                            if let Some(error) = result.error {
                                tracing::error!(revision, %error, "reload connector snapshot");
                            } else {
                                tracing::info!(revision, changed = result.changed, "reloaded connector snapshot");
                            }
                        }
                        Err(error) => tracing::error!(revision, %error, "load configuration for connector reload"),
                    }
                }
                () = shutdown.cancelled() => return,
            }
        }
    }))
}

type LogFilterHandle = reload::Handle<EnvFilter, Registry>;

fn configure_logging() -> Result<LogFilterHandle> {
    let (filter, handle) = reload::Layer::new(log_filter_from_environment());
    let registry = tracing_subscriber::registry().with(filter);
    if env::var("FUJIN_LOG_TYPE").is_ok_and(|value| value.eq_ignore_ascii_case("json")) {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
            .context("initialize JSON logging")?;
    } else {
        registry
            .with(tracing_subscriber::fmt::layer())
            .try_init()
            .context("initialize text logging")?;
    }
    Ok(handle)
}

fn log_filter_from_environment() -> EnvFilter {
    EnvFilter::new(log_directive(env::var("FUJIN_LOG_LEVEL").ok().as_deref()))
}

fn log_directive(value: Option<&str>) -> &'static str {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("DEBUG") => "debug",
        Some("WARN") => "warn",
        Some("ERROR") => "error",
        _ => "info",
    }
}

fn spawn_shutdown_loop(shutdown: CancellationToken) -> Result<JoinHandle<()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        let task_shutdown = shutdown.clone();
        Ok(tokio::spawn(async move {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::error!(%error, "receive interrupt signal");
                    }
                    task_shutdown.cancel();
                }
                signal = terminate.recv() => {
                    if signal.is_some() {
                        task_shutdown.cancel();
                    }
                }
                () = shutdown.cancelled() => {}
            }
        }))
    }
    #[cfg(not(unix))]
    Ok(tokio::spawn(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "receive interrupt signal");
                }
                shutdown.cancel();
            }
            () = shutdown.cancelled() => {}
        }
    }))
}

async fn settle_task(task: Option<JoinHandle<()>>, name: &str) -> Result<()> {
    if let Some(task) = task {
        task.await.with_context(|| format!("join {name}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::log_directive;

    #[test]
    fn logging_level_matches_go_environment_contract() {
        assert_eq!(log_directive(Some("debug")), "debug");
        assert_eq!(log_directive(Some("INFO")), "info");
        assert_eq!(log_directive(Some("warn")), "warn");
        assert_eq!(log_directive(Some("ERROR")), "error");
        assert_eq!(log_directive(Some("trace")), "info");
        assert_eq!(log_directive(None), "info");
    }
}
