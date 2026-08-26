use std::{fmt, sync::Arc, sync::mpsc, thread};

use anyhow::{Context, Result, bail};

use crate::{
    ApplicationBuilder, ApplicationHandle, Endpoint,
    configurator::{ApplyResult, ConnectorRuntimeStatus, ConnectorSnapshot},
};

/// Tokio runtime controls for hosts embedding Fujin in a synchronous process.
#[derive(Clone, Debug)]
pub struct EmbeddedRuntimeConfig {
    pub worker_threads: Option<usize>,
    pub thread_name: String,
}

impl Default for EmbeddedRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: None,
            thread_name: "fujin-runtime".into(),
        }
    }
}

/// Cloneable synchronous controls for a Fujin application running on its owned Tokio runtime.
#[derive(Clone)]
pub struct EmbeddedApplicationControl {
    handle: ApplicationHandle,
    runtime: tokio::runtime::Handle,
    endpoints: Arc<[Endpoint]>,
}

impl fmt::Debug for EmbeddedApplicationControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedApplicationControl")
            .field("endpoints", &self.endpoints)
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl EmbeddedApplicationControl {
    #[must_use]
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    #[must_use]
    pub fn watches_connectors(&self) -> bool {
        self.handle.watches_connectors()
    }

    pub fn request_shutdown(&self) {
        self.handle.shutdown();
    }

    pub fn catalog_status(&self) -> ConnectorRuntimeStatus {
        self.runtime.block_on(self.handle.catalog_status())
    }

    pub fn reload_connectors(&self, snapshot: ConnectorSnapshot) -> ApplyResult {
        self.runtime
            .block_on(self.handle.reload_connectors(snapshot))
    }

    /// Reloads a complete connector snapshot from the retained bootstrap configurator.
    ///
    /// # Errors
    /// Returns an error when a watcher owns connector state or the configurator cannot load.
    pub fn reload_from_configurator(&self) -> Result<ApplyResult> {
        self.runtime
            .block_on(self.handle.reload_from_configurator())
    }
}

/// A Fujin application owned by a dedicated Tokio runtime thread.
pub struct EmbeddedApplication {
    control: EmbeddedApplicationControl,
    thread: Option<thread::JoinHandle<Result<()>>>,
}

impl fmt::Debug for EmbeddedApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedApplication")
            .field("control", &self.control)
            .field("running", &self.thread.is_some())
            .finish()
    }
}

impl EmbeddedApplication {
    /// Builds and starts Fujin on a dedicated Tokio runtime, blocking until every listener is ready.
    ///
    /// # Errors
    /// Returns runtime construction, application compilation, listener readiness, or thread errors.
    pub fn start(builder: ApplicationBuilder, config: &EmbeddedRuntimeConfig) -> Result<Self> {
        if matches!(config.worker_threads, Some(0)) {
            bail!("embedded runtime worker_threads must be positive");
        }
        if config.thread_name.is_empty() {
            bail!("embedded runtime thread_name is empty");
        }
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let thread_name = config.thread_name.clone();
        let worker_threads = config.worker_threads;
        let thread = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_multi_thread();
                runtime.enable_all().thread_name(thread_name);
                if let Some(worker_threads) = worker_threads {
                    runtime.worker_threads(worker_threads);
                }
                let runtime = runtime.build().context("build embedded Tokio runtime")?;
                let runtime_handle = runtime.handle().clone();
                runtime.block_on(async move {
                    let application = match builder.build().await {
                        Ok(application) => application,
                        Err(error) => {
                            let message = format!("{error:#}");
                            let _ = ready_sender.send(Err(message.clone()));
                            bail!(message);
                        }
                    };
                    let running = match application.start().await {
                        Ok(running) => running,
                        Err(error) => {
                            let message = format!("{error:#}");
                            let _ = ready_sender.send(Err(message.clone()));
                            bail!(message);
                        }
                    };
                    let control = EmbeddedApplicationControl {
                        handle: running.handle(),
                        runtime: runtime_handle,
                        endpoints: running.endpoints().to_vec().into(),
                    };
                    if ready_sender.send(Ok(control)).is_err() {
                        return running.shutdown().await;
                    }
                    running.wait().await
                })
            })
            .context("spawn embedded Fujin runtime")?;
        let control = ready_receiver
            .recv()
            .context("embedded Fujin runtime stopped before readiness")?
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            control,
            thread: Some(thread),
        })
    }

    #[must_use]
    pub fn handle(&self) -> ApplicationHandle {
        self.control.handle.clone()
    }

    #[must_use]
    pub fn control(&self) -> EmbeddedApplicationControl {
        self.control.clone()
    }

    pub fn request_shutdown(&self) {
        self.control.request_shutdown();
    }

    /// Requests shutdown and joins the runtime thread.
    ///
    /// # Errors
    /// Returns listener, connector cleanup, runtime, or thread panic failures.
    pub fn shutdown(mut self) -> Result<()> {
        self.request_shutdown();
        self.join()
    }

    /// Joins the runtime thread after an externally requested or terminal shutdown.
    ///
    /// # Errors
    /// Returns listener, connector cleanup, runtime, or thread panic failures.
    pub fn wait(mut self) -> Result<()> {
        self.join()
    }

    fn join(&mut self) -> Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        thread
            .join()
            .map_err(|_| anyhow::anyhow!("embedded Fujin runtime thread panicked"))?
    }
}

impl Drop for EmbeddedApplication {
    fn drop(&mut self) {
        self.control.request_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Application, RuntimeConfig};

    #[test]
    fn starts_and_stops_on_owned_runtime() {
        let config: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "fujin": {
                "transports": [{"type": "tcp", "settings": {"addr": "127.0.0.1:0"}}]
            },
            "grpc": {"enabled": false}
        }))
        .expect("parse embedded config");
        let application = EmbeddedApplication::start(
            Application::builder()
                .config(config)
                .transport(fujin_transport_tcp::plugin()),
            &EmbeddedRuntimeConfig {
                worker_threads: Some(1),
                ..EmbeddedRuntimeConfig::default()
            },
        )
        .expect("start embedded application");
        application
            .shutdown()
            .expect("shutdown embedded application");
    }
}
