use std::{env, sync::Arc};

use anyhow::{Context, Result};
use fujin_core::{DescriptorRegistry, NoBindMiddleware};
use tokio_util::sync::CancellationToken;
const BUILD_VERSION: &str = env!("FUJIN_BUILD_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let argument = env::args().nth(1);
    if argument.as_deref() == Some("--version") {
        println!("version: {BUILD_VERSION}");
        return Ok(());
    }
    let config_path = argument
        .or_else(|| env::var("FUJIN_CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".to_owned());
    let config = fujin_runtime::load(&config_path)
        .await
        .with_context(|| format!("load Fujin configuration {config_path:?}"))?;
    let server_config = config
        .server_config(BUILD_VERSION)
        .context("validate Fujin server configuration")?;
    let registry = Arc::new(DescriptorRegistry::default());
    #[cfg(feature = "kafka")]
    registry
        .register("kafka_franz", fujin_kafka::descriptor())
        .context("register Kafka connector")?;
    let plugin_paths = fujin_plugin_api::plugin_paths_from_env("FUJIN_CONNECTOR_PLUGINS");
    let _connector_plugins = fujin_plugin_api::load_connector_plugins(plugin_paths, &registry)
        .context("load connector plugins")?;
    let catalog = fujin_runtime::compile_catalog(&config, registry)
        .await
        .context("compile connector catalog")?;
    let shutdown = CancellationToken::new();
    let signal_task = spawn_shutdown_loop(shutdown.clone())?;
    #[cfg(unix)]
    let reload_task =
        spawn_reload_loop(config_path.clone(), Arc::clone(&catalog), shutdown.clone())?;

    let server_result = fujin_server::serve(
        server_config,
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        shutdown.clone(),
    )
    .await;
    shutdown.cancel();
    signal_task.await.context("join shutdown signal task")?;
    #[cfg(unix)]
    reload_task.await.context("join SIGHUP reload task")?;
    let catalog_result = catalog.close().await;
    server_result.context("serve Fujin")?;
    catalog_result.context("close connector catalog")?;
    Ok(())
}

fn spawn_shutdown_loop(shutdown: CancellationToken) -> Result<tokio::task::JoinHandle<()>> {
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

#[cfg(unix)]
fn spawn_reload_loop(
    config_path: String,
    catalog: Arc<fujin_core::Catalog>,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut hangup = signal(SignalKind::hangup()).context("install SIGHUP handler")?;
    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
                signal = hangup.recv() => {
                    if signal.is_none() {
                        return;
                    }
                    match fujin_runtime::reload_connectors(&config_path, &catalog).await {
                        Ok(()) => tracing::info!(path = %config_path, "reloaded connector snapshot"),
                        Err(error) => tracing::error!(path = %config_path, %error, "reload connector snapshot"),
                    }
                }
                () = shutdown.cancelled() => return,
            }
        }
    }))
}
