use std::{collections::BTreeMap, env, fmt, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use fujin_core::{Catalog, CatalogStatus, ConnectorsConfig};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Notify, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{RuntimeConfig, RuntimeError};

pub const CONFIGURATOR_ENV: &str = "FUJIN_CONFIGURATOR";
pub const YAML_PATHS_ENV: &str = "FUJIN_CONFIGURATOR_YAML_PATHS";
pub const ENV_CONFIG_ENV: &str = "FUJIN_CONFIGURATOR_ENV_CONFIG";

const DEFAULT_YAML_PATHS: [&str; 3] = ["./config.yaml", "conf/config.yaml", "config/config.yaml"];

type ConfiguratorFactory =
    Arc<dyn Fn() -> Result<Arc<dyn Configurator>, RuntimeError> + Send + Sync + 'static>;

#[async_trait]
pub trait Configurator: fmt::Debug + Send + Sync {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError>;

    fn initial_connector_snapshot(&self) -> Option<ConnectorSnapshot> {
        None
    }

    fn watches_connectors(&self) -> bool {
        false
    }

    async fn watch_connectors(
        &self,
        _runtime: Arc<dyn ConnectorRuntime>,
        _shutdown: CancellationToken,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::InvalidConfig(
            "configurator does not watch connector snapshots".into(),
        ))
    }
}

#[derive(Default)]
pub struct ConfiguratorRegistry {
    factories: BTreeMap<String, ConfiguratorFactory>,
}

impl fmt::Debug for ConfiguratorRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguratorRegistry")
            .field("configurators", &self.factories.keys())
            .finish()
    }
}

impl ConfiguratorRegistry {
    /// Registers one configurator factory exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for an empty or duplicate name.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        factory: impl Fn() -> Result<Arc<dyn Configurator>, RuntimeError> + Send + Sync + 'static,
    ) -> Result<(), RuntimeError> {
        let name = name.into();
        if name.is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "configurator name is empty".into(),
            ));
        }
        if self.factories.contains_key(&name) {
            return Err(RuntimeError::InvalidConfig(format!(
                "configurator {name:?} is already registered"
            )));
        }
        self.factories.insert(name, Arc::new(factory));
        Ok(())
    }

    /// Constructs one registered configurator.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] when the name is not registered, or propagates the
    /// selected factory's construction error.
    pub fn create(&self, name: &str) -> Result<Arc<dyn Configurator>, RuntimeError> {
        let factory = self.factories.get(name).ok_or_else(|| {
            RuntimeError::InvalidConfig(format!(
                "configurator {name:?} not found (available: {:?})",
                self.list()
            ))
        })?;
        factory()
    }

    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

/// Selects and constructs the configurator named by [`CONFIGURATOR_ENV`].
///
/// # Errors
///
/// Returns [`RuntimeError::InvalidConfig`] when the selector is absent, empty, or unregistered.
pub fn selected_configurator(
    registry: &ConfiguratorRegistry,
) -> Result<Arc<dyn Configurator>, RuntimeError> {
    let name = env::var(CONFIGURATOR_ENV).map_err(|_| {
        RuntimeError::InvalidConfig(format!(
            "configurator not specified: set {CONFIGURATOR_ENV}"
        ))
    })?;
    if name.is_empty() {
        return Err(RuntimeError::InvalidConfig(format!(
            "configurator not specified: set {CONFIGURATOR_ENV}"
        )));
    }
    registry.create(&name)
}

#[derive(Debug)]
pub struct YamlConfigurator {
    paths: Vec<PathBuf>,
}

impl YamlConfigurator {
    #[must_use]
    pub fn from_environment() -> Self {
        let paths = env::var(YAML_PATHS_ENV).map_or_else(
            |_| DEFAULT_YAML_PATHS.iter().map(PathBuf::from).collect(),
            |value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(PathBuf::from)
                    .collect()
            },
        );
        Self { paths }
    }

    #[must_use]
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

#[async_trait]
impl Configurator for YamlConfigurator {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
        if self.paths.is_empty() {
            return Err(RuntimeError::InvalidConfig(format!(
                "yaml configurator: {YAML_PATHS_ENV} contains no paths"
            )));
        }
        for path in &self.paths {
            match tokio::fs::read(path).await {
                Ok(bytes) => {
                    tracing::info!(path = %path.display(), "loading configuration with yaml configurator");
                    return decode_config(&bytes, &path.display().to_string());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(RuntimeError::Read {
                        path: path.display().to_string(),
                        source,
                    });
                }
            }
        }
        Err(RuntimeError::InvalidConfig(format!(
            "yaml configurator: failed to find configuration in paths {:?}",
            self.paths
        )))
    }
}

#[derive(Debug, Default)]
pub struct EnvConfigurator;

#[async_trait]
impl Configurator for EnvConfigurator {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
        let value = env::var(ENV_CONFIG_ENV).map_err(|_| {
            RuntimeError::InvalidConfig(format!(
                "env configurator: {ENV_CONFIG_ENV} is not set or empty"
            ))
        })?;
        if value.is_empty() {
            return Err(RuntimeError::InvalidConfig(format!(
                "env configurator: {ENV_CONFIG_ENV} is not set or empty"
            )));
        }
        tracing::info!(
            variable = ENV_CONFIG_ENV,
            "loading configuration with env configurator"
        );
        decode_config(value.as_bytes(), ENV_CONFIG_ENV)
    }
}

/// Decodes one complete JSON or YAML bootstrap document.
///
/// # Errors
///
/// Returns [`RuntimeError::Parse`] when neither format matches [`RuntimeConfig`].
pub fn decode_config(bytes: &[u8], source: &str) -> Result<RuntimeConfig, RuntimeError> {
    if let Ok(config) = serde_json::from_slice(bytes) {
        return Ok(config);
    }
    yaml_serde::from_slice(bytes).map_err(|error| RuntimeError::Parse {
        path: source.to_owned(),
        source: error,
    })
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectorSnapshot {
    pub revision: u64,
    pub connectors: ConnectorsConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyState {
    Accepted,
    Rejected,
    Stale,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyResult {
    pub revision: u64,
    pub state: ApplyState,
    pub changed: bool,
    pub error: Option<String>,
}

impl ApplyResult {
    fn rejected(revision: u64, error: impl Into<String>) -> Self {
        Self {
            revision,
            state: ApplyState::Rejected,
            changed: false,
            error: Some(error.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorRuntimeStatus {
    pub build_version: String,
    pub connector_types: Vec<String>,
    pub active_revision: u64,
    pub active_digest: [u8; 32],
    pub last_rejected_revision: u64,
    pub last_rejected_diagnostic: String,
    pub runtime_source_connected: bool,
    pub catalog: CatalogStatus,
}

#[async_trait]
pub trait ConnectorReloader: fmt::Debug + Send + Sync {
    async fn reload_connectors(&self, connectors: &ConnectorsConfig) -> Result<(), RuntimeError>;
    fn catalog_status(&self) -> CatalogStatus;
}

#[async_trait]
impl ConnectorReloader for Catalog {
    async fn reload_connectors(&self, connectors: &ConnectorsConfig) -> Result<(), RuntimeError> {
        self.reload(connectors).await?;
        Ok(())
    }

    fn catalog_status(&self) -> CatalogStatus {
        self.status()
    }
}

#[derive(Debug)]
struct ControllerState {
    active_revision: u64,
    active_digest: [u8; 32],
    last_rejected_revision: u64,
    last_rejected_diagnostic: String,
    runtime_source_connected: bool,
}

#[derive(Debug)]
pub struct RuntimeController {
    reloader: Arc<dyn ConnectorReloader>,
    connector_types: Vec<String>,
    build_version: String,
    state: tokio::sync::Mutex<ControllerState>,
}

impl RuntimeController {
    /// Records the already-published initial connector snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] when the initial snapshot cannot be encoded for its
    /// stable content digest.
    pub fn new(
        reloader: Arc<dyn ConnectorReloader>,
        connector_types: Vec<String>,
        initial: &ConnectorSnapshot,
        build_version: impl Into<String>,
    ) -> Result<Arc<Self>, RuntimeError> {
        let active_digest = connector_digest(&initial.connectors)?;
        Ok(Arc::new(Self {
            reloader,
            connector_types,
            build_version: build_version.into(),
            state: tokio::sync::Mutex::new(ControllerState {
                active_revision: initial.revision,
                active_digest,
                last_rejected_revision: 0,
                last_rejected_diagnostic: String::new(),
                runtime_source_connected: false,
            }),
        }))
    }

    pub async fn active_revision(&self) -> u64 {
        self.state.lock().await.active_revision
    }

    pub async fn set_source_connected(&self, connected: bool) {
        self.state.lock().await.runtime_source_connected = connected;
    }

    pub async fn status(&self) -> ConnectorRuntimeStatus {
        let state = self.state.lock().await;
        ConnectorRuntimeStatus {
            build_version: self.build_version.clone(),
            connector_types: self.connector_types.clone(),
            active_revision: state.active_revision,
            active_digest: state.active_digest,
            last_rejected_revision: state.last_rejected_revision,
            last_rejected_diagnostic: state.last_rejected_diagnostic.clone(),
            runtime_source_connected: state.runtime_source_connected,
            catalog: self.reloader.catalog_status(),
        }
    }

    pub async fn apply(
        &self,
        snapshot: ConnectorSnapshot,
        cancellation: &CancellationToken,
    ) -> ApplyResult {
        let mut state = self.state.lock().await;
        let digest = match connector_digest(&snapshot.connectors) {
            Ok(digest) => digest,
            Err(error) => return reject(&mut state, snapshot.revision, error.to_string()),
        };
        if cancellation.is_cancelled() {
            return reject(
                &mut state,
                snapshot.revision,
                "connector snapshot apply canceled",
            );
        }
        if snapshot.revision < state.active_revision {
            return ApplyResult {
                revision: snapshot.revision,
                state: ApplyState::Stale,
                changed: false,
                error: None,
            };
        }
        if snapshot.revision == state.active_revision {
            if digest == state.active_digest {
                return ApplyResult {
                    revision: snapshot.revision,
                    state: ApplyState::Accepted,
                    changed: false,
                    error: None,
                };
            }
            return reject(
                &mut state,
                snapshot.revision,
                format!(
                    "connector snapshot revision {} conflicts with active content",
                    snapshot.revision
                ),
            );
        }
        if let Err(error) = self.reloader.reload_connectors(&snapshot.connectors).await {
            return reject(&mut state, snapshot.revision, error.to_string());
        }
        state.active_revision = snapshot.revision;
        state.active_digest = digest;
        ApplyResult {
            revision: snapshot.revision,
            state: ApplyState::Accepted,
            changed: true,
            error: None,
        }
    }
}

fn reject(state: &mut ControllerState, revision: u64, error: impl Into<String>) -> ApplyResult {
    let error = error.into();
    state.last_rejected_revision = revision;
    state.last_rejected_diagnostic.clone_from(&error);
    ApplyResult::rejected(revision, error)
}

fn connector_digest(connectors: &ConnectorsConfig) -> Result<[u8; 32], RuntimeError> {
    let encoded = serde_json::to_vec(connectors).map_err(|error| {
        RuntimeError::InvalidConfig(format!("encode connector snapshot: {error}"))
    })?;
    Ok(Sha256::digest(encoded).into())
}

/// Binds optional source revision metadata to the configuration loaded during bootstrap.
///
/// # Errors
///
/// Returns [`RuntimeError::InvalidConfig`] when declared snapshot content differs from the loaded
/// connector configuration.
pub fn bootstrap_snapshot(
    configurator: &dyn Configurator,
    config: &RuntimeConfig,
) -> Result<ConnectorSnapshot, RuntimeError> {
    let loaded = ConnectorSnapshot {
        revision: 0,
        connectors: config.connectors.clone(),
    };
    let Some(mut declared) = configurator.initial_connector_snapshot() else {
        return Ok(loaded);
    };
    if connector_digest(&declared.connectors)? != connector_digest(&loaded.connectors)? {
        return Err(RuntimeError::InvalidConfig(
            "configurator bootstrap snapshot does not match loaded connectors".into(),
        ));
    }
    declared.connectors = loaded.connectors;
    Ok(declared)
}

#[async_trait]
pub trait ConnectorRuntime: fmt::Debug + Send + Sync {
    async fn submit(
        &self,
        snapshot: ConnectorSnapshot,
        cancellation: CancellationToken,
    ) -> ApplyResult;
    async fn set_source_connected(&self, connected: bool);
    async fn status(&self) -> ConnectorRuntimeStatus;
}

struct SnapshotRequest {
    snapshot: ConnectorSnapshot,
    cancellation: CancellationToken,
    result: oneshot::Sender<ApplyResult>,
}

#[derive(Default)]
struct QueueState {
    closed: bool,
    pending: Option<SnapshotRequest>,
}

pub struct RuntimeQueue {
    controller: Arc<RuntimeController>,
    shutdown: CancellationToken,
    ready: Notify,
    state: Mutex<QueueState>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for RuntimeQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeQueue")
            .finish_non_exhaustive()
    }
}

impl RuntimeQueue {
    #[must_use]
    pub fn new(controller: Arc<RuntimeController>) -> Arc<Self> {
        let queue = Arc::new(Self {
            controller,
            shutdown: CancellationToken::new(),
            ready: Notify::new(),
            state: Mutex::new(QueueState::default()),
            task: Mutex::new(None),
        });
        let actor = Arc::clone(&queue);
        *queue.task.lock() = Some(tokio::spawn(async move { actor.run().await }));
        queue
    }

    pub async fn close(&self) {
        self.shutdown.cancel();
        self.ready.notify_waiters();
        let task = self.task.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            if self.shutdown.is_cancelled() {
                self.shutdown_pending("runtime connector queue closed");
                return;
            }
            let notified = self.ready.notified();
            let request = { self.state.lock().pending.take() };
            if let Some(request) = request {
                let result = self
                    .controller
                    .apply(request.snapshot, &request.cancellation)
                    .await;
                let _ = request.result.send(result);
                continue;
            }
            tokio::select! {
                () = self.shutdown.cancelled() => {}
                () = notified => {}
            }
        }
    }

    fn shutdown_pending(&self, error: &str) {
        let pending = {
            let mut state = self.state.lock();
            state.closed = true;
            state.pending.take()
        };
        if let Some(request) = pending {
            let _ = request
                .result
                .send(ApplyResult::rejected(request.snapshot.revision, error));
        }
    }
}

#[async_trait]
impl ConnectorRuntime for RuntimeQueue {
    async fn submit(
        &self,
        snapshot: ConnectorSnapshot,
        cancellation: CancellationToken,
    ) -> ApplyResult {
        let revision = snapshot.revision;
        let (sender, receiver) = oneshot::channel();
        let request = SnapshotRequest {
            snapshot,
            cancellation,
            result: sender,
        };
        {
            let mut state = self.state.lock();
            if state.closed || self.shutdown.is_cancelled() {
                return ApplyResult::rejected(revision, "runtime connector queue closed");
            }
            if let Some(pending) = state.pending.as_ref()
                && revision <= pending.snapshot.revision
            {
                return ApplyResult {
                    revision,
                    state: ApplyState::Superseded,
                    changed: false,
                    error: Some(format!(
                        "connector snapshot revision {revision} superseded by pending revision {}",
                        pending.snapshot.revision
                    )),
                };
            }
            if let Some(superseded) = state.pending.replace(request) {
                let _ = superseded.result.send(ApplyResult {
                    revision: superseded.snapshot.revision,
                    state: ApplyState::Superseded,
                    changed: false,
                    error: Some(format!(
                        "connector snapshot revision {} superseded by pending revision {revision}",
                        superseded.snapshot.revision
                    )),
                });
            }
        }
        self.ready.notify_one();
        receiver.await.unwrap_or_else(|_| {
            ApplyResult::rejected(
                revision,
                "runtime connector queue stopped before apply result",
            )
        })
    }

    async fn set_source_connected(&self, connected: bool) {
        self.controller.set_source_connected(connected).await;
    }

    async fn status(&self) -> ConnectorRuntimeStatus {
        self.controller.status().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use serde_json::json;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::RuntimeConfig;

    #[derive(Debug)]
    struct StaticConfigurator(RuntimeConfig);

    #[async_trait]
    impl Configurator for StaticConfigurator {
        async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn registry_rejects_duplicate_names_and_lists_available_configurators() {
        let mut registry = ConfiguratorRegistry::default();
        registry
            .register("test", || {
                Ok(Arc::new(StaticConfigurator(RuntimeConfig::default())) as Arc<dyn Configurator>)
            })
            .expect("register configurator");
        assert!(
            registry
                .register("test", || {
                    Ok(Arc::new(StaticConfigurator(RuntimeConfig::default()))
                        as Arc<dyn Configurator>)
                })
                .is_err()
        );
        assert_eq!(registry.list(), ["test"]);
        let error = registry
            .create("missing")
            .expect_err("missing configurator");
        assert!(error.to_string().contains("available"));
    }

    #[tokio::test]
    async fn yaml_configurator_uses_first_existing_path_and_decodes_json() {
        let directory = std::env::temp_dir().join(format!(
            "fujin-configurator-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("runtime")
        ));
        tokio::fs::create_dir_all(&directory)
            .await
            .expect("create fixture directory");
        let existing = directory.join("config.json");
        tokio::fs::write(&existing, br#"{"grpc":{"enabled":false}}"#)
            .await
            .expect("write fixture");
        let configurator = YamlConfigurator::new(vec![directory.join("missing"), existing]);
        let config = configurator.load().await.expect("load first existing path");
        assert!(!config.grpc.enabled);
        tokio::fs::remove_dir_all(directory)
            .await
            .expect("remove fixture directory");
    }

    #[derive(Debug, Default)]
    struct RecordingReloader {
        versions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ConnectorReloader for RecordingReloader {
        async fn reload_connectors(
            &self,
            connectors: &ConnectorsConfig,
        ) -> Result<(), RuntimeError> {
            let version = connectors["main"].settings["version"]
                .as_str()
                .expect("version")
                .to_owned();
            if version == "invalid" {
                return Err(RuntimeError::InvalidConfig("rejected test snapshot".into()));
            }
            self.versions.lock().push(version);
            Ok(())
        }

        fn catalog_status(&self) -> CatalogStatus {
            CatalogStatus::default()
        }
    }

    #[tokio::test]
    async fn controller_orders_deduplicates_and_rejects_snapshots() {
        let reloader = Arc::new(RecordingReloader::default());
        let controller = RuntimeController::new(
            reloader.clone(),
            vec!["test".into()],
            &ConnectorSnapshot {
                revision: 10,
                connectors: connectors("v1"),
            },
            "build",
        )
        .expect("create controller");
        let cancellation = CancellationToken::new();

        let stale = controller
            .apply(
                ConnectorSnapshot {
                    revision: 9,
                    connectors: connectors("stale"),
                },
                &cancellation,
            )
            .await;
        assert_eq!(stale.state, ApplyState::Stale);

        let duplicate = controller
            .apply(
                ConnectorSnapshot {
                    revision: 10,
                    connectors: connectors("v1"),
                },
                &cancellation,
            )
            .await;
        assert_eq!(duplicate.state, ApplyState::Accepted);
        assert!(!duplicate.changed);

        let conflict = controller
            .apply(
                ConnectorSnapshot {
                    revision: 10,
                    connectors: connectors("conflict"),
                },
                &cancellation,
            )
            .await;
        assert_eq!(conflict.state, ApplyState::Rejected);

        let accepted = controller
            .apply(
                ConnectorSnapshot {
                    revision: 11,
                    connectors: connectors("v2"),
                },
                &cancellation,
            )
            .await;
        assert_eq!(accepted.state, ApplyState::Accepted);
        assert!(accepted.changed);

        let rejected = controller
            .apply(
                ConnectorSnapshot {
                    revision: 12,
                    connectors: connectors("invalid"),
                },
                &cancellation,
            )
            .await;
        assert_eq!(rejected.state, ApplyState::Rejected);
        let status = controller.status().await;
        assert_eq!(status.active_revision, 11);
        assert_eq!(status.last_rejected_revision, 12);
        assert!(status.last_rejected_diagnostic.contains("rejected"));
        assert_eq!(&*reloader.versions.lock(), &["v2"]);
    }

    #[derive(Debug)]
    struct SnapshotConfigurator {
        config: RuntimeConfig,
        snapshot: ConnectorSnapshot,
    }

    #[async_trait]
    impl Configurator for SnapshotConfigurator {
        async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
            Ok(self.config.clone())
        }

        fn initial_connector_snapshot(&self) -> Option<ConnectorSnapshot> {
            Some(self.snapshot.clone())
        }
    }

    #[test]
    fn bootstrap_snapshot_requires_declared_content_to_match_loaded_config() {
        let config = RuntimeConfig {
            connectors: connectors("v1"),
            ..RuntimeConfig::default()
        };
        let matching = SnapshotConfigurator {
            config: config.clone(),
            snapshot: ConnectorSnapshot {
                revision: 7,
                connectors: connectors("v1"),
            },
        };
        assert_eq!(
            bootstrap_snapshot(&matching, &config)
                .expect("matching bootstrap snapshot")
                .revision,
            7
        );

        let conflicting = SnapshotConfigurator {
            config: config.clone(),
            snapshot: ConnectorSnapshot {
                revision: 8,
                connectors: connectors("v2"),
            },
        };
        assert!(bootstrap_snapshot(&conflicting, &config).is_err());
    }

    #[derive(Debug)]
    struct BlockingReloader {
        started: AtomicUsize,
        gate: Semaphore,
        versions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ConnectorReloader for BlockingReloader {
        async fn reload_connectors(
            &self,
            connectors: &ConnectorsConfig,
        ) -> Result<(), RuntimeError> {
            self.started.fetch_add(1, Ordering::Relaxed);
            self.gate.acquire().await.expect("gate open").forget();
            let version = connectors["main"].settings["version"]
                .as_str()
                .expect("version")
                .to_owned();
            self.versions.lock().push(version);
            Ok(())
        }

        fn catalog_status(&self) -> CatalogStatus {
            CatalogStatus::default()
        }
    }

    fn connectors(version: &str) -> ConnectorsConfig {
        BTreeMap::from([(
            "main".into(),
            fujin_core::ConnectorConfig {
                connector_type: "test".into(),
                overridable: Vec::new(),
                bind_middlewares: Vec::new(),
                connector_middlewares: Vec::new(),
                settings: json!({"version": version}),
            },
        )])
    }

    #[tokio::test]
    async fn runtime_queue_coalesces_to_newest_pending_snapshot() {
        let reloader = Arc::new(BlockingReloader {
            started: AtomicUsize::new(0),
            gate: Semaphore::new(0),
            versions: Mutex::new(Vec::new()),
        });
        let controller = RuntimeController::new(
            reloader.clone(),
            vec!["test".into()],
            &ConnectorSnapshot {
                revision: 0,
                connectors: connectors("initial"),
            },
            "test",
        )
        .expect("create controller");
        let queue = RuntimeQueue::new(controller);
        let first_queue = Arc::clone(&queue);
        let first = tokio::spawn(async move {
            first_queue
                .submit(
                    ConnectorSnapshot {
                        revision: 1,
                        connectors: connectors("v1"),
                    },
                    CancellationToken::new(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while reloader.started.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first apply started");

        let second_queue = Arc::clone(&queue);
        let second = tokio::spawn(async move {
            second_queue
                .submit(
                    ConnectorSnapshot {
                        revision: 2,
                        connectors: connectors("v2"),
                    },
                    CancellationToken::new(),
                )
                .await
        });
        wait_for_pending_revision(&queue, 2).await;

        let third_queue = Arc::clone(&queue);
        let third = tokio::spawn(async move {
            third_queue
                .submit(
                    ConnectorSnapshot {
                        revision: 3,
                        connectors: connectors("v3"),
                    },
                    CancellationToken::new(),
                )
                .await
        });
        wait_for_pending_revision(&queue, 3).await;

        reloader.gate.add_permits(2);
        assert_eq!(first.await.expect("first task").state, ApplyState::Accepted);
        assert_eq!(
            second.await.expect("second task").state,
            ApplyState::Superseded
        );
        assert_eq!(third.await.expect("third task").state, ApplyState::Accepted);
        assert_eq!(&*reloader.versions.lock(), &["v1", "v3"]);
        queue.close().await;
    }

    async fn wait_for_pending_revision(queue: &RuntimeQueue, revision: u64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if queue
                    .state
                    .lock()
                    .pending
                    .as_ref()
                    .is_some_and(|request| request.snapshot.revision == revision)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("snapshot became pending");
    }
}
