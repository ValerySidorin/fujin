use std::{env, sync::Arc};

use anyhow::{Context, Result};
use fujin_core::{DescriptorRegistry, NoBindMiddleware};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = env::args()
        .nth(1)
        .or_else(|| env::var("FUJIN_CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".to_owned());
    let config = fujin_runtime::load(&config_path)
        .await
        .with_context(|| format!("load Fujin configuration {config_path:?}"))?;
    let registry = Arc::new(DescriptorRegistry::default());
    #[cfg(feature = "kafka")]
    registry
        .register("kafka", fujin_kafka::descriptor())
        .context("register Kafka connector")?;
    let catalog = fujin_runtime::compile_catalog(&config, registry)
        .await
        .context("compile connector catalog")?;
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "install Ctrl-C handler");
        }
        signal.cancel();
    });

    let server_result = fujin_server::serve(
        config.server,
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        shutdown,
    )
    .await;
    let catalog_result = catalog.close().await;
    server_result.context("serve Fujin")?;
    catalog_result.context("close connector catalog")?;
    Ok(())
}
