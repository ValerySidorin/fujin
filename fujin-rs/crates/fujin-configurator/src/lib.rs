//! Public configurator contracts and Fujin bootstrap configuration.

mod config;

use std::{collections::BTreeMap, env, fmt, sync::Arc};

use async_trait::async_trait;
use fujin_connector::{CatalogStatus, ConnectorsConfig};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

pub use config::{
    FujinConfig, GrpcClientKeepAliveSettings, GrpcConfig, GrpcServerKeepAliveSettings,
    HealthConfig, RuntimeConfig, RuntimeError, TransportConfig, compile_catalog, server_config,
};

pub const CONFIGURATOR_ENV: &str = "FUJIN_CONFIGURATOR";

type ConfiguratorFactory =
    Arc<dyn Fn() -> Result<Arc<dyn Configurator>, RuntimeError> + Send + Sync + 'static>;

/// One explicitly registered, statically linked configurator plugin.
#[derive(Clone)]
pub struct ConfiguratorPlugin {
    name: String,
    factory: ConfiguratorFactory,
}

impl ConfiguratorPlugin {
    #[must_use]
    pub fn new<C, F>(name: impl Into<String>, factory: F) -> Self
    where
        C: Configurator,
        F: Fn() -> Result<C, RuntimeError> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            factory: Arc::new(move || {
                factory().map(|configurator| Arc::new(configurator) as Arc<dyn Configurator>)
            }),
        }
    }

    #[must_use]
    pub fn from_factory(
        name: impl Into<String>,
        factory: impl Fn() -> Result<Arc<dyn Configurator>, RuntimeError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            factory: Arc::new(factory),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for ConfiguratorPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguratorPlugin")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait Configurator: fmt::Debug + Send + Sync + 'static {
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

    /// Registers one configurator plugin exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for an empty or duplicate name.
    pub fn register_plugin(&mut self, plugin: ConfiguratorPlugin) -> Result<(), RuntimeError> {
        let ConfiguratorPlugin { name, factory } = plugin;
        self.register(name, move || factory())
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
    #[doc(hidden)]
    pub fn rejected(revision: u64, error: impl Into<String>) -> Self {
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
pub trait ConnectorRuntime: fmt::Debug + Send + Sync {
    async fn submit(
        &self,
        snapshot: ConnectorSnapshot,
        cancellation: CancellationToken,
    ) -> ApplyResult;
    async fn set_source_connected(&self, connected: bool);
    async fn status(&self) -> ConnectorRuntimeStatus;
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

#[doc(hidden)]
pub fn connector_digest(connectors: &ConnectorsConfig) -> Result<[u8; 32], RuntimeError> {
    let encoded = serde_json::to_vec(connectors).map_err(|error| {
        RuntimeError::InvalidConfig(format!("encode connector snapshot: {error}"))
    })?;
    Ok(Sha256::digest(encoded).into())
}
