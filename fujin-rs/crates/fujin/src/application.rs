use std::{collections::BTreeSet, fmt, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use fujin_core::{
    BindMiddlewareRegistration, BindMiddlewareRegistry, Catalog, ConnectorMiddlewareRegistration,
    ConnectorMiddlewareRegistry, ConnectorPlugin, ConnectorRegistry,
};
use fujin_runtime::configurator::{
    ApplyResult, Configurator, ConfiguratorPlugin, ConfiguratorRegistry, ConnectorReloader,
    ConnectorRuntime, ConnectorRuntimeStatus, ConnectorSnapshot, RuntimeController, RuntimeQueue,
    bootstrap_snapshot, selected_configurator,
};
use fujin_runtime::{Endpoint, RuntimeConfig, RuntimeError, ServerConfig};
use fujin_transport::{TransportRegistration, TransportRegistry};
use fujin_upgrade::{InheritedListeners, ListenerRegistry};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const BUILD_VERSION: &str = env!("FUJIN_BUILD_VERSION");

/// Builder for one independently configured Fujin application instance.
pub struct ApplicationBuilder {
    build_version: String,
    graceful_upgrade: bool,
    config: Option<RuntimeConfig>,
    configurator: Option<Arc<dyn Configurator>>,
    configurator_name: Option<String>,
    configurators: Vec<ConfiguratorPlugin>,
    connectors: Vec<ConnectorPlugin>,
    transports: Vec<TransportRegistration>,
    bind_middlewares: Vec<BindMiddlewareRegistration>,
    connector_middlewares: Vec<ConnectorMiddlewareRegistration>,
}

impl fmt::Debug for ApplicationBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationBuilder")
            .field("build_version", &self.build_version)
            .field("graceful_upgrade", &self.graceful_upgrade)
            .field("has_config", &self.config.is_some())
            .field("has_configurator", &self.configurator.is_some())
            .field("configurator_name", &self.configurator_name)
            .field("configurators", &self.configurators)
            .field("connectors", &self.connectors)
            .field("transports", &self.transports)
            .field("bind_middlewares", &self.bind_middlewares)
            .field("connector_middlewares", &self.connector_middlewares)
            .finish()
    }
}

impl Default for ApplicationBuilder {
    fn default() -> Self {
        Self {
            graceful_upgrade: true,
            build_version: BUILD_VERSION.into(),
            config: None,
            configurator: None,
            configurator_name: None,
            configurators: Vec::new(),
            connectors: Vec::new(),
            transports: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
        }
    }
}

impl ApplicationBuilder {
    #[must_use]
    pub fn build_version(mut self, version: impl Into<String>) -> Self {
        self.build_version = version.into();
        self
    }

    /// Enables or disables Unix listener handoff for this application instance.
    #[must_use]
    pub fn graceful_upgrade(mut self, enabled: bool) -> Self {
        self.graceful_upgrade = enabled;
        self
    }

    /// Uses a complete in-memory bootstrap configuration.
    #[must_use]
    pub fn config(mut self, config: RuntimeConfig) -> Self {
        self.config = Some(config);
        self.configurator = None;
        self
    }

    /// Uses one already-constructed configurator rather than selecting a registered factory.
    #[must_use]
    pub fn configurator_instance(mut self, configurator: Arc<dyn Configurator>) -> Self {
        self.configurator = Some(configurator);
        self.config = None;
        self
    }

    /// Selects a registered configurator without consulting `FUJIN_CONFIGURATOR`.
    #[must_use]
    pub fn selected_configurator(mut self, name: impl Into<String>) -> Self {
        self.configurator_name = Some(name.into());
        self
    }

    #[must_use]
    pub fn configurator(mut self, plugin: ConfiguratorPlugin) -> Self {
        self.configurators.push(plugin);
        self
    }

    #[must_use]
    pub fn connector(mut self, plugin: ConnectorPlugin) -> Self {
        self.connectors.push(plugin);
        self
    }

    #[must_use]
    pub fn transport(mut self, plugin: TransportRegistration) -> Self {
        self.transports.push(plugin);
        self
    }

    #[must_use]
    pub fn bind_middleware(mut self, plugin: BindMiddlewareRegistration) -> Self {
        self.bind_middlewares.push(plugin);
        self
    }

    #[must_use]
    pub fn connector_middleware(mut self, plugin: ConnectorMiddlewareRegistration) -> Self {
        self.connector_middlewares.push(plugin);
        self
    }

    /// Compiles all registered plugins and the complete bootstrap snapshot without binding sockets.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate registrations, missing configured plugins, invalid settings,
    /// or connector generation compilation failures.
    #[allow(clippy::too_many_lines)]
    pub async fn build(self) -> Result<Application> {
        let Self {
            graceful_upgrade,
            build_version,
            config,
            configurator,
            configurator_name,
            configurators,
            connectors,
            transports,
            bind_middlewares,
            connector_middlewares,
        } = self;

        let mut configurator_registry = ConfiguratorRegistry::default();
        for plugin in configurators {
            configurator_registry
                .register_plugin(plugin)
                .context("register configurator plugin")?;
        }
        let configurator: Arc<dyn Configurator> = if let Some(config) = config {
            Arc::new(StaticConfigurator(config))
        } else if let Some(configurator) = configurator {
            configurator
        } else if let Some(name) = configurator_name {
            configurator_registry
                .create(&name)
                .with_context(|| format!("create configurator {name:?}"))?
        } else {
            selected_configurator(&configurator_registry)
                .context("select Fujin configurator from FUJIN_CONFIGURATOR")?
        };
        let config = configurator
            .load()
            .await
            .context("load Fujin bootstrap configuration")?;

        let connector_registry = Arc::new(ConnectorRegistry::default());
        for plugin in connectors {
            connector_registry
                .register_plugin(plugin)
                .context("register connector plugin")?;
        }
        let bind_registry = Arc::new(BindMiddlewareRegistry::default());
        for plugin in bind_middlewares {
            bind_registry
                .register_plugin(plugin)
                .context("register BIND middleware plugin")?;
        }
        let connector_middleware_registry = Arc::new(ConnectorMiddlewareRegistry::default());
        for plugin in connector_middlewares {
            connector_middleware_registry
                .register_plugin(plugin)
                .context("register connector middleware plugin")?;
        }
        let transport_registry = TransportRegistry::default();
        for plugin in transports {
            transport_registry
                .register(plugin)
                .context("register transport plugin")?;
        }

        let mut transport_names = BTreeSet::new();
        let mut compiled_transports = Vec::new();
        for entry in config.fujin.transports.iter().filter(|entry| entry.enabled) {
            if !transport_names.insert(entry.transport_type.clone()) {
                bail!("duplicate enabled {:?} transport", entry.transport_type);
            }
            compiled_transports.push(
                transport_registry
                    .compile(entry)
                    .with_context(|| format!("compile {:?} transport", entry.transport_type))?,
            );
        }
        let control_plane = config
            .control_plane_config(build_version.clone())
            .context("validate Fujin control-plane configuration")?;
        let server_config = ServerConfig::from_control_plane(control_plane, compiled_transports);

        let initial_snapshot = bootstrap_snapshot(configurator.as_ref(), &config)
            .context("validate configurator bootstrap snapshot")?;
        let catalog = fujin_runtime::compile_catalog(
            &config,
            Arc::clone(&connector_registry),
            connector_middleware_registry.clone(),
        )
        .await
        .context("compile connector catalog")?;
        let reloader: Arc<dyn ConnectorReloader> = catalog.clone();
        let controller = RuntimeController::new(
            reloader,
            connector_registry.list(),
            &initial_snapshot,
            build_version,
        )
        .context("create runtime connector controller")?;

        Ok(Application {
            configurator,
            catalog,
            controller,
            bind_middlewares: bind_registry,
            server_config,
            graceful_upgrade,
        })
    }
}

/// A validated Fujin application that has not bound any listeners yet.
pub struct Application {
    configurator: Arc<dyn Configurator>,
    catalog: Arc<Catalog>,
    controller: Arc<RuntimeController>,
    bind_middlewares: Arc<BindMiddlewareRegistry>,
    graceful_upgrade: bool,
    server_config: ServerConfig,
}

impl fmt::Debug for Application {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Application")
            .field("configurator", &self.configurator)
            .field("catalog", &self.catalog.status())
            .field("server_config", &self.server_config)
            .finish_non_exhaustive()
    }
}

impl Application {
    #[must_use]
    pub fn builder() -> ApplicationBuilder {
        ApplicationBuilder::default()
    }

    /// Starts all configured listeners and returns only after every listener is accepting traffic.
    ///
    /// # Errors
    ///
    /// Returns listener, graceful-upgrade, watcher, or task failures observed before readiness.
    pub async fn start(self) -> Result<RunningApplication> {
        let shutdown_requested = CancellationToken::new();
        let runtime_shutdown = CancellationToken::new();
        let server_shutdown = CancellationToken::new();
        let upgrade_shutdown = CancellationToken::new();
        let UpgradeSetup {
            client: upgrade_client,
            inherited,
            registry,
            socket,
            mut task,
        } = if self.graceful_upgrade {
            prepare_upgrade(&self.server_config, &upgrade_shutdown, &shutdown_requested).await?
        } else {
            UpgradeSetup {
                client: None,
                inherited: InheritedListeners::default(),
                registry: ListenerRegistry::new(fujin_runtime::configured_listener_count(
                    &self.server_config,
                )),
                socket: String::new(),
                task: None,
            }
        };
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let mut server_task = tokio::spawn(fujin_runtime::serve_with_readiness_and_upgrade(
            self.server_config,
            Arc::clone(&self.catalog),
            self.bind_middlewares,
            server_shutdown.clone(),
            ready_sender,
            registry.clone(),
            inherited,
        ));
        let endpoints = tokio::select! {
            ready = ready_receiver => ready.context("Fujin server stopped before readiness")?,
            result = &mut server_task => {
                let result = result.context("join Fujin server task")?;
                self.catalog.close().await.context("close connector catalog")?;
                result.context("serve Fujin before readiness")?;
                bail!("Fujin server stopped before readiness");
            }
        };
        if let Some(client) = upgrade_client {
            task = Some(
                complete_upgrade(
                    client,
                    &socket,
                    &registry,
                    &upgrade_shutdown,
                    &shutdown_requested,
                )
                .await?,
            );
        }
        let watcher_task = start_connector_watcher(
            Arc::clone(&self.configurator),
            Arc::clone(&self.controller),
            runtime_shutdown.clone(),
        );
        Ok(RunningApplication {
            endpoints,
            configurator: self.configurator,
            catalog: self.catalog,
            controller: self.controller,
            shutdown_requested,
            runtime_shutdown,
            server_shutdown,
            upgrade_shutdown,
            server_task: Some(server_task),
            watcher_task,
            upgrade_task: task,
        })
    }
}

/// A ready Fujin application with explicit lifecycle and runtime connector controls.
pub struct RunningApplication {
    endpoints: Vec<Endpoint>,
    configurator: Arc<dyn Configurator>,
    catalog: Arc<Catalog>,
    controller: Arc<RuntimeController>,
    shutdown_requested: CancellationToken,
    runtime_shutdown: CancellationToken,
    server_shutdown: CancellationToken,
    upgrade_shutdown: CancellationToken,
    server_task: Option<JoinHandle<Result<()>>>,
    watcher_task: Option<JoinHandle<()>>,
    upgrade_task: Option<JoinHandle<()>>,
}

impl fmt::Debug for RunningApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningApplication")
            .field("endpoints", &self.endpoints)
            .field("catalog", &self.catalog.status())
            .field(
                "shutdown_requested",
                &self.shutdown_requested.is_cancelled(),
            )
            .finish_non_exhaustive()
    }
}

/// Cloneable control-plane handle for signals, management endpoints, and embedding hosts.
#[derive(Clone)]
pub struct ApplicationHandle {
    configurator: Arc<dyn Configurator>,
    controller: Arc<RuntimeController>,
    runtime_shutdown: CancellationToken,
    shutdown_requested: CancellationToken,
}

impl fmt::Debug for ApplicationHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationHandle")
            .field(
                "shutdown_requested",
                &self.shutdown_requested.is_cancelled(),
            )
            .finish_non_exhaustive()
    }
}

impl ApplicationHandle {
    #[must_use]
    pub fn watches_connectors(&self) -> bool {
        self.configurator.watches_connectors()
    }
    pub fn shutdown(&self) {
        self.shutdown_requested.cancel();
    }

    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_requested.clone()
    }

    pub async fn catalog_status(&self) -> ConnectorRuntimeStatus {
        self.controller.status().await
    }

    pub async fn reload_connectors(&self, snapshot: ConnectorSnapshot) -> ApplyResult {
        self.controller
            .apply(snapshot, &self.runtime_shutdown)
            .await
    }

    /// Reloads a complete connector snapshot from the retained bootstrap configurator.
    ///
    /// # Errors
    ///
    /// Returns an error when a watcher owns runtime connector state or the configurator cannot load.
    pub async fn reload_from_configurator(&self) -> Result<ApplyResult> {
        if self.configurator.watches_connectors() {
            bail!("connector snapshots are owned by the configurator watcher");
        }
        let config = self
            .configurator
            .load()
            .await
            .context("load configuration for connector reload")?;
        let revision = self.controller.active_revision().await.saturating_add(1);
        Ok(self
            .controller
            .apply(
                ConnectorSnapshot {
                    revision,
                    connectors: config.connectors,
                },
                &self.runtime_shutdown,
            )
            .await)
    }
}
impl RunningApplication {
    #[must_use]
    pub fn handle(&self) -> ApplicationHandle {
        ApplicationHandle {
            configurator: Arc::clone(&self.configurator),
            controller: Arc::clone(&self.controller),
            runtime_shutdown: self.runtime_shutdown.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
        }
    }

    #[must_use]
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_requested.clone()
    }

    pub async fn catalog_status(&self) -> ConnectorRuntimeStatus {
        self.handle().catalog_status().await
    }

    pub async fn reload_connectors(&self, snapshot: ConnectorSnapshot) -> ApplyResult {
        self.handle().reload_connectors(snapshot).await
    }

    /// Reloads a complete connector snapshot from the retained bootstrap configurator.
    ///
    /// # Errors
    ///
    /// Returns an error when a watcher owns runtime connector state or the configurator cannot load.
    pub async fn reload_from_configurator(&self) -> Result<ApplyResult> {
        self.handle().reload_from_configurator().await
    }

    /// Requests shutdown and waits for listeners, watchers, upgrades, and connector generations.
    ///
    /// # Errors
    ///
    /// Returns the first lifecycle or cleanup failure.
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown_requested.cancel();
        self.finish(None).await
    }

    /// Waits for a terminal listener, an upgrade drain request, or explicit cancellation.
    ///
    /// # Errors
    ///
    /// Returns the first lifecycle or cleanup failure.
    pub async fn wait(mut self) -> Result<()> {
        let Some(mut server_task) = self.server_task.take() else {
            bail!("Fujin server task is unavailable");
        };
        let server_result = tokio::select! {
            result = &mut server_task => Some(result.context("join Fujin server task")?),
            () = self.shutdown_requested.cancelled() => {
                self.server_task = Some(server_task);
                None
            }
        };
        self.finish(server_result).await
    }

    async fn finish(&mut self, mut server_result: Option<Result<()>>) -> Result<()> {
        self.runtime_shutdown.cancel();
        settle_task(self.watcher_task.take(), "connector watcher").await?;
        self.upgrade_shutdown.cancel();
        settle_task(self.upgrade_task.take(), "upgrade listener").await?;
        self.server_shutdown.cancel();
        if server_result.is_none()
            && let Some(task) = self.server_task.take()
        {
            server_result = Some(task.await.context("join Fujin server task")?);
        }
        let catalog_result = self.catalog.close().await;
        server_result.unwrap_or(Ok(())).context("serve Fujin")?;
        catalog_result.context("close connector catalog")?;
        Ok(())
    }
}

impl Drop for RunningApplication {
    fn drop(&mut self) {
        self.runtime_shutdown.cancel();
        self.upgrade_shutdown.cancel();
        self.server_shutdown.cancel();
        self.shutdown_requested.cancel();
    }
}

#[derive(Debug)]
struct StaticConfigurator(RuntimeConfig);

#[async_trait]
impl Configurator for StaticConfigurator {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
        Ok(self.0.clone())
    }
}

struct UpgradeSetup {
    client: Option<fujin_upgrade::UpgradeClient>,
    inherited: InheritedListeners,
    registry: ListenerRegistry,
    socket: String,
    task: Option<JoinHandle<()>>,
}

async fn prepare_upgrade(
    server_config: &ServerConfig,
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
    let registry = ListenerRegistry::new(fujin_runtime::configured_listener_count(server_config));
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
    Ok(spawn_upgrade_listener(
        socket.to_owned(),
        registry.clone(),
        upgrade_shutdown.clone(),
        shutdown_requested.clone(),
    ))
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

async fn settle_task(task: Option<JoinHandle<()>>, name: &str) -> Result<()> {
    if let Some(task) = task {
        task.await.with_context(|| format!("join {name}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Arc};

    use fujin_core::{
        AcceptanceGuarantee, BoxFuture, Capabilities, CompiledConnector, Completion,
        CompletionSink, ConnectorDescriptor, ConnectorRuntime as CoreConnectorRuntime, CoreError,
        Message, OperationToken, Reader, ReaderEventSink, Result as CoreResult, RouteProfile,
        Writer,
    };
    #[cfg(feature = "tcp")]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug)]
    struct NopDescriptor;

    impl ConnectorDescriptor for NopDescriptor {
        fn compile(&self, settings: &serde_json::Value) -> CoreResult<Arc<dyn CompiledConnector>> {
            if !settings.is_null() && !settings.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err(CoreError::InvalidConfig(
                    "nop settings must be empty".into(),
                ));
            }
            Ok(Arc::new(NopCompiled {
                routes: BTreeMap::from([(
                    "default".into(),
                    RouteProfile {
                        capabilities: Capabilities::PRODUCE,
                        produce_guarantee: AcceptanceGuarantee::Local,
                        ..RouteProfile::default()
                    },
                )]),
            }))
        }
    }

    #[derive(Debug)]
    struct NopCompiled {
        routes: BTreeMap<String, RouteProfile>,
    }

    impl CompiledConnector for NopCompiled {
        fn routes(&self) -> &BTreeMap<String, RouteProfile> {
            &self.routes
        }

        fn open_runtime(&self) -> CoreResult<Arc<dyn CoreConnectorRuntime>> {
            Ok(Arc::new(NopRuntime))
        }
    }

    #[derive(Debug)]
    struct NopRuntime;

    impl CoreConnectorRuntime for NopRuntime {
        fn open_reader(
            &self,
            _route: &str,
            _auto_settle: bool,
            _events: Arc<dyn ReaderEventSink>,
        ) -> CoreResult<Arc<dyn Reader>> {
            Err(CoreError::OperationUnsupported)
        }

        fn open_writer(
            &self,
            route: &str,
            completions: Arc<dyn CompletionSink>,
        ) -> CoreResult<Arc<dyn Writer>> {
            if route != "default" {
                return Err(CoreError::InvalidConfig("unknown nop route".into()));
            }
            Ok(Arc::new(NopWriter { completions }))
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, CoreResult<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct NopWriter {
        completions: Arc<dyn CompletionSink>,
    }

    impl fmt::Debug for NopWriter {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_struct("NopWriter").finish_non_exhaustive()
        }
    }

    impl Writer for NopWriter {
        fn produce(&self, token: OperationToken, _message: Message) -> CoreResult<()> {
            self.complete(token);
            Ok(())
        }

        fn flush(&self, token: OperationToken) -> CoreResult<()> {
            self.complete(token);
            Ok(())
        }

        fn begin_transaction(&self, _token: OperationToken) -> CoreResult<()> {
            Err(CoreError::OperationUnsupported)
        }

        fn commit_transaction(&self, _token: OperationToken) -> CoreResult<()> {
            Err(CoreError::OperationUnsupported)
        }

        fn rollback_transaction(&self, _token: OperationToken) -> CoreResult<()> {
            Err(CoreError::OperationUnsupported)
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, CoreResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn writer_contract_compliant(&self) -> bool {
            true
        }
    }

    impl NopWriter {
        fn complete(&self, token: OperationToken) {
            self.completions.complete(Completion {
                token,
                result: Ok(()),
            });
        }
    }

    fn embedded_config() -> RuntimeConfig {
        yaml_serde::from_str(
            r"
fujin:
  transports:
    - type: tcp
      settings:
        addr: 127.0.0.1:0
grpc: { enabled: false }
connectors:
  primary:
    type: nop
    settings: {}
",
        )
        .expect("parse embedded config")
    }

    fn plugin_config() -> RuntimeConfig {
        serde_json::from_value(serde_json::json!({
            "fujin": {
                "transports": [{"type": "test_transport", "settings": {}}]
            },
            "grpc": {"enabled": false},
            "connectors": {}
        }))
        .expect("parse plugin config")
    }

    #[derive(Debug)]
    struct TestTransportPlugin;

    impl fujin_transport::TransportPlugin for TestTransportPlugin {
        fn compile(
            &self,
            _settings: &serde_json::Value,
        ) -> Result<Arc<dyn fujin_transport::CompiledTransport>> {
            Ok(Arc::new(TestTransport))
        }
    }

    #[derive(Debug)]
    struct TestTransport;

    impl fujin_transport::CompiledTransport for TestTransport {
        fn serve(
            self: Arc<Self>,
            _context: fujin_transport::TransportContext,
        ) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn builder_accepts_registered_configurator_and_transport_plugins() {
        let application = Application::builder()
            .graceful_upgrade(false)
            .configurator(ConfiguratorPlugin::new("test_configurator", || {
                Ok(StaticConfigurator(plugin_config()))
            }))
            .selected_configurator("test_configurator")
            .transport(TransportRegistration::new(
                "test_transport",
                TestTransportPlugin,
            ))
            .build()
            .await
            .expect("build application from registered plugins");

        assert_eq!(application.server_config.transports.len(), 1);
    }

    #[tokio::test]
    #[cfg(feature = "tcp")]
    async fn embedded_application_starts_reports_actual_endpoint_and_shuts_down() {
        let application = Application::builder()
            .graceful_upgrade(false)
            .config(embedded_config())
            .connector(ConnectorPlugin::new("nop", NopDescriptor))
            .transport(crate::plugins::transport::tcp())
            .build()
            .await
            .expect("build embedded application");
        let running = application
            .start()
            .await
            .expect("start embedded application");
        let endpoint = running.endpoints().first().expect("TCP endpoint");
        assert_eq!(endpoint.interface, "native");
        assert_eq!(endpoint.transport.as_deref(), Some("tcp"));
        assert_ne!(endpoint.address, "127.0.0.1:0");
        let mut stream = tokio::net::TcpStream::connect(&endpoint.address)
            .await
            .expect("connect embedded application");
        let mut hello = vec![0_u8, 1, 1, 1];
        for value in [b"test".as_slice(), b"embed".as_slice()] {
            hello.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("field length")
                    .to_be_bytes(),
            );
            hello.extend_from_slice(value);
        }
        stream.write_all(&hello).await.expect("write HELLO");
        assert_eq!(stream.read_u8().await.expect("read HELLO response"), 19);

        running
            .shutdown()
            .await
            .expect("shutdown embedded application");
    }

    #[tokio::test]
    async fn builder_rejects_configured_unregistered_transport() {
        let error = Application::builder()
            .graceful_upgrade(false)
            .config(embedded_config())
            .connector(ConnectorPlugin::new("nop", NopDescriptor))
            .build()
            .await
            .expect_err("missing transport plugin");
        assert!(error.to_string().contains("transport"));
    }
}
