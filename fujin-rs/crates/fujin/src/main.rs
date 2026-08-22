#[cfg(not(any(feature = "configurator-yaml", feature = "configurator-env")))]
compile_error!("the fujin binary requires at least one configurator feature");

use std::env;

use anyhow::{Context, Result, bail};
use fujin::{Application, ApplicationHandle, plugins};
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

    let application = plugins::full(Application::builder())
        .build()
        .await
        .context("build Fujin application")?;
    let running = application
        .start()
        .await
        .context("start Fujin application")?;
    tracing::info!(endpoints = ?running.endpoints(), "all configured listeners are ready");

    let signal_shutdown = CancellationToken::new();
    let signal_task = spawn_signal_loop(running.handle(), signal_shutdown.clone(), log_filter)?;
    let result = running.wait().await;
    signal_shutdown.cancel();
    signal_task.await.context("join signal loop")?;
    result
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

fn spawn_signal_loop(
    application: ApplicationHandle,
    shutdown: CancellationToken,
    log_filter: LogFilterHandle,
) -> Result<JoinHandle<()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        let mut hangup = signal(SignalKind::hangup()).context("install SIGHUP handler")?;
        Ok(tokio::spawn(async move {
            let interrupt = tokio::signal::ctrl_c();
            tokio::pin!(interrupt);
            loop {
                tokio::select! {
                    result = &mut interrupt => {
                        if let Err(error) = result {
                            tracing::error!(%error, "receive interrupt signal");
                        }
                        application.shutdown();
                        return;
                    }
                    signal = terminate.recv() => {
                        if signal.is_some() {
                            application.shutdown();
                        }
                        return;
                    }
                    signal = hangup.recv() => {
                        if signal.is_none() {
                            return;
                        }
                        if let Err(error) = log_filter.reload(log_filter_from_environment()) {
                            tracing::error!(%error, "reload log level");
                        }
                        tracing::info!("received SIGHUP, reloading configuration");
                        if application.watches_connectors() {
                            continue;
                        }
                        match application.reload_from_configurator().await {
                            Ok(result) => {
                                if let Some(error) = result.error {
                                    tracing::error!(revision = result.revision, %error, "reload connector snapshot");
                                } else {
                                    tracing::info!(
                                        revision = result.revision,
                                        changed = result.changed,
                                        "reloaded connector snapshot"
                                    );
                                }
                            }
                            Err(error) => tracing::error!(%error, "load configuration for connector reload"),
                        }
                    }
                    () = shutdown.cancelled() => return,
                }
            }
        }))
    }
    #[cfg(not(unix))]
    {
        drop(log_filter);
        Ok(tokio::spawn(async move {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::error!(%error, "receive interrupt signal");
                    }
                    application.shutdown();
                }
                () = shutdown.cancelled() => {}
            }
        }))
    }
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
