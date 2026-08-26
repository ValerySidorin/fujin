use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwapOption;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    time::timeout,
};

use crate::{
    CompiledConnector, CompiledConnectorMiddleware, CompletionSink, ConnectorConfig,
    ConnectorDescriptor, ConnectorMiddlewareCompiler, ConnectorRuntime, ConnectorsConfig,
    CoreError, NoConnectorMiddleware, Reader, ReaderEventSink, Result, RouteProfile, Writer,
    writer_contract::{contract_sink, enforce_writer_contract},
};

const DEFAULT_CATALOG_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const GENERATION_TRANSITION_LIMIT: usize = 64;
static NEXT_GENERATION_ID: AtomicU64 = AtomicU64::new(1);

/// One explicitly registered, statically linked connector plugin.
#[derive(Clone)]
pub struct ConnectorPlugin {
    name: String,
    descriptor: Arc<dyn ConnectorDescriptor>,
}

impl ConnectorPlugin {
    #[must_use]
    pub fn new(name: impl Into<String>, descriptor: impl ConnectorDescriptor) -> Self {
        Self {
            name: name.into(),
            descriptor: Arc::new(descriptor),
        }
    }

    #[must_use]
    pub fn from_arc(name: impl Into<String>, descriptor: Arc<dyn ConnectorDescriptor>) -> Self {
        Self {
            name: name.into(),
            descriptor,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for ConnectorPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorPlugin")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Explicit registry of statically linked connector compilers.
#[derive(Default)]
pub struct ConnectorRegistry {
    descriptors: RwLock<BTreeMap<String, Arc<dyn ConnectorDescriptor>>>,
}

impl fmt::Debug for ConnectorRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorRegistry")
            .field("descriptors", &self.descriptors.read().keys())
            .finish()
    }
}

impl ConnectorRegistry {
    /// Registers one connector type exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] for an empty or duplicate name.
    pub fn register(
        &self,
        name: impl Into<String>,
        descriptor: Arc<dyn ConnectorDescriptor>,
    ) -> Result<()> {
        let name = name.into();
        if name.is_empty() {
            return Err(CoreError::InvalidConfig(
                "connector type name is empty".into(),
            ));
        }
        let mut descriptors = self.descriptors.write();
        if descriptors.contains_key(&name) {
            return Err(CoreError::InvalidConfig(format!(
                "connector type {name:?} is already registered"
            )));
        }
        descriptors.insert(name, descriptor);
        Ok(())
    }

    /// Registers one connector plugin exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] for an empty or duplicate name.
    pub fn register_plugin(&self, plugin: ConnectorPlugin) -> Result<()> {
        self.register(plugin.name, plugin.descriptor)
    }

    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.descriptors.read().keys().cloned().collect()
    }

    fn get(&self, name: &str) -> Option<Arc<dyn ConnectorDescriptor>> {
        self.descriptors.read().get(name).cloned()
    }
}

/// Side-effect-free compiler for complete immutable connector snapshots.
pub struct GenerationCompiler {
    registry: Arc<ConnectorRegistry>,
    middleware: Arc<dyn ConnectorMiddlewareCompiler>,
}

impl fmt::Debug for GenerationCompiler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationCompiler")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl GenerationCompiler {
    pub fn new(
        registry: Arc<ConnectorRegistry>,
        middleware: Arc<dyn ConnectorMiddlewareCompiler>,
    ) -> Self {
        Self {
            registry,
            middleware,
        }
    }

    pub fn without_middlewares(registry: Arc<ConnectorRegistry>) -> Self {
        Self::new(registry, Arc::new(NoConnectorMiddleware))
    }

    /// Compiles every connector and middleware before constructing a generation.
    ///
    /// # Errors
    ///
    /// Returns an error when a connector type is absent or any configuration, route profile,
    /// or middleware chain is invalid. Already-compiled middleware resources are closed.
    pub async fn compile(self: &Arc<Self>, configs: &ConnectorsConfig) -> Result<Arc<Generation>> {
        let mut connectors = BTreeMap::new();
        for (name, config) in configs {
            if name.is_empty() {
                return Err(CoreError::InvalidConfig(
                    "connector instance name is empty".into(),
                ));
            }
            let descriptor = self.registry.get(&config.connector_type).ok_or_else(|| {
                CoreError::InvalidConfig(format!(
                    "connector {name:?}: unsupported type {:?}",
                    config.connector_type
                ))
            })?;
            let compiled = descriptor.compile(&config.settings)?;
            let declared = compiled.routes();
            if declared.is_empty() {
                return Err(CoreError::InvalidConfig(format!(
                    "connector {name:?}: no routes"
                )));
            }
            let mut profiles = BTreeMap::new();
            for (route, profile) in declared {
                profile.validate(route)?;
                profiles.insert(route.clone(), *profile);
            }
            connectors.insert(
                name.clone(),
                Arc::new(CompiledEntry {
                    name: name.clone(),
                    config: config.clone(),
                    compiled,
                    profiles,
                    middleware: Mutex::new(None),
                    runtime: Mutex::new(None),
                }),
            );
        }

        for entry in connectors.values() {
            match self.middleware.compile(&entry.config.connector_middlewares) {
                Ok(middleware) => *entry.middleware.lock() = middleware,
                Err(error) => {
                    close_middlewares(&connectors).await;
                    return Err(CoreError::InvalidConfig(format!(
                        "connector {:?}: compile middlewares: {error}",
                        entry.name
                    )));
                }
            }
        }

        Ok(Arc::new(Generation {
            id: NEXT_GENERATION_ID.fetch_add(1, Ordering::Relaxed),
            compiler: Arc::clone(self),
            connectors,
            bindings: AtomicUsize::new(0),
            retired: AtomicBool::new(false),
            close_started: AtomicBool::new(false),
            closed: Notify::new(),
            close_result: Mutex::new(None),
        }))
    }
}

impl Default for GenerationCompiler {
    fn default() -> Self {
        Self::without_middlewares(Arc::new(ConnectorRegistry::default()))
    }
}

struct RuntimeOwner {
    runtime: Arc<dyn ConnectorRuntime>,
    generation_refs: AtomicUsize,
    closed: AtomicBool,
}

impl fmt::Debug for RuntimeOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeOwner")
            .field(
                "generation_refs",
                &self.generation_refs.load(Ordering::Acquire),
            )
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl RuntimeOwner {
    fn new(runtime: Arc<dyn ConnectorRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            generation_refs: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
        })
    }

    fn retain(&self) {
        self.generation_refs.fetch_add(1, Ordering::Relaxed);
    }

    async fn release(self: Arc<Self>) -> Result<()> {
        let previous = self.generation_refs.fetch_sub(1, Ordering::AcqRel);
        if previous == 0 {
            return Err(CoreError::Internal(
                "connector runtime released too many times".into(),
            ));
        }
        if previous != 1 || self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        Arc::clone(&self.runtime).close().await
    }
}

struct CompiledEntry {
    name: String,
    config: ConnectorConfig,
    compiled: Arc<dyn CompiledConnector>,
    profiles: BTreeMap<String, RouteProfile>,
    middleware: Mutex<Option<Arc<dyn CompiledConnectorMiddleware>>>,
    runtime: Mutex<Option<Arc<RuntimeOwner>>>,
}

impl fmt::Debug for CompiledEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledEntry")
            .field("name", &self.name)
            .field("config", &self.config)
            .field("profiles", &self.profiles)
            .field("runtime_open", &self.runtime.lock().is_some())
            .finish_non_exhaustive()
    }
}

impl CompiledEntry {
    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        let mut owner = self.runtime.lock();
        if let Some(owner) = owner.as_ref() {
            return Ok(Arc::clone(&owner.runtime));
        }
        let runtime = self.compiled.open_runtime()?;
        *owner = Some(RuntimeOwner::new(Arc::clone(&runtime)));
        Ok(runtime)
    }
}

/// Immutable compiled connector snapshot pinned by successful BIND operations.
pub struct Generation {
    id: u64,
    compiler: Arc<GenerationCompiler>,
    connectors: BTreeMap<String, Arc<CompiledEntry>>,
    bindings: AtomicUsize,
    retired: AtomicBool,
    close_started: AtomicBool,
    closed: Notify,
    close_result: Mutex<Option<Result<()>>>,
}

impl fmt::Debug for Generation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Generation")
            .field("id", &self.id)
            .field("connectors", &self.connectors.keys())
            .field("bindings", &self.bindings.load(Ordering::Acquire))
            .field("retired", &self.retired.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Generation {
    pub const fn id(&self) -> u64 {
        self.id
    }

    pub fn binding_count(&self) -> usize {
        self.bindings.load(Ordering::Acquire)
    }

    pub fn connector_config(&self, name: &str) -> Option<ConnectorConfig> {
        self.connectors.get(name).map(|entry| entry.config.clone())
    }

    /// Pins this generation directly or compiles a private derived generation for overrides.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is absent, an override is disallowed or invalid,
    /// or the derived generation cannot be compiled and preflighted.
    pub async fn acquire_with_overrides(
        self: &Arc<Self>,
        name: &str,
        overrides: &BTreeMap<String, String>,
    ) -> Result<Binding> {
        if overrides.is_empty() {
            return self.acquire(name);
        }
        let config = self
            .connector_config(name)
            .ok_or_else(|| CoreError::ConnectorNotFound(name.into()))?;
        let descriptor = self
            .compiler
            .registry
            .get(&config.connector_type)
            .ok_or_else(|| {
                CoreError::InvalidConfig(format!(
                    "connector {name:?}: unsupported type {:?}",
                    config.connector_type
                ))
            })?;
        let modified = crate::overrides::apply_overrides(&config, descriptor.as_ref(), overrides)?;
        let configs = BTreeMap::from([(name.to_owned(), modified)]);
        let derived = self.compile_derived(&configs).await?;
        let binding = match derived.acquire(name) {
            Ok(binding) => binding,
            Err(error) => {
                derived.retire();
                return Err(error);
            }
        };
        derived.retire();
        Ok(binding)
    }

    /// Compiles a private generation with the same plugin registries as this generation.
    ///
    /// # Errors
    ///
    /// Returns an error when compilation or eager-runtime preflight fails.
    pub async fn compile_derived(
        self: &Arc<Self>,
        configs: &ConnectorsConfig,
    ) -> Result<Arc<Self>> {
        let next = self.compiler.compile(configs).await?;
        if let Err(error) = prepare_generation(&next, Some(self)) {
            next.abort().await;
            return Err(error);
        }
        Ok(next)
    }

    /// Pins one connector in this generation for a session.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is absent or the generation is retired.
    pub fn acquire(self: &Arc<Self>, name: &str) -> Result<Binding> {
        let connector = self
            .connectors
            .get(name)
            .cloned()
            .ok_or_else(|| CoreError::ConnectorNotFound(name.into()))?;
        if self.retired.load(Ordering::Acquire) {
            return Err(CoreError::Unavailable(
                "connector generation retired".into(),
            ));
        }
        self.bindings.fetch_add(1, Ordering::AcqRel);
        if self.retired.load(Ordering::Acquire) {
            self.release_binding();
            return Err(CoreError::Unavailable(
                "connector generation retired".into(),
            ));
        }
        Ok(Binding {
            generation: Arc::clone(self),
            connector,
        })
    }

    pub fn retire(self: &Arc<Self>) {
        if !self.retired.swap(true, Ordering::AcqRel) && self.binding_count() == 0 {
            self.start_close();
        }
    }

    /// Waits until a retired generation has released all owned resources.
    ///
    /// # Errors
    ///
    /// Returns the aggregated runtime or middleware cleanup failure.
    pub async fn wait_closed(&self) -> Result<()> {
        loop {
            let notified = self.closed.notified();
            if let Some(result) = self.close_result.lock().clone() {
                return result;
            }
            notified.await;
        }
    }

    fn release_binding(self: &Arc<Self>) {
        let previous = self.bindings.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "generation binding released too many times");
        if previous == 1 && self.retired.load(Ordering::Acquire) {
            self.start_close();
        }
    }

    fn start_close(self: &Arc<Self>) {
        if self.close_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let generation = Arc::clone(self);
        tokio::spawn(async move {
            generation.finish_close().await;
        });
    }

    async fn finish_close(self: Arc<Self>) {
        let result = close_generation_resources(&self.connectors).await;
        *self.close_result.lock() = Some(result);
        self.closed.notify_waiters();
    }

    async fn abort(self: &Arc<Self>) {
        if self.close_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let result = close_generation_resources(&self.connectors).await;
        *self.close_result.lock() = Some(result);
        self.closed.notify_waiters();
    }
}

/// Session-scoped lease over one connector in an immutable generation.
pub struct Binding {
    generation: Arc<Generation>,
    connector: Arc<CompiledEntry>,
}

impl fmt::Debug for Binding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Binding")
            .field("generation", &self.generation.id)
            .field("connector", &self.connector.name)
            .finish()
    }
}

impl Binding {
    pub fn name(&self) -> &str {
        &self.connector.name
    }

    pub fn generation_id(&self) -> u64 {
        self.generation.id
    }

    /// Returns the immutable route profile pinned by this binding.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::RouteNotFound`] when the route is absent.
    pub fn route_profile(&self, route: &str) -> Result<RouteProfile> {
        self.connector
            .profiles
            .get(route)
            .copied()
            .ok_or_else(|| CoreError::RouteNotFound(route.into()))
    }

    pub fn route_profiles(&self) -> BTreeMap<String, RouteProfile> {
        self.connector.profiles.clone()
    }

    /// Opens and generation-locally wraps one reader lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the route, runtime, reader, or middleware cannot be opened.
    pub fn open_reader(
        &self,
        route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        self.route_profile(route)?;
        let runtime = self.connector.open_runtime()?;
        let reader = runtime.open_reader(route, auto_settle, events)?;
        match self.connector.middleware.lock().as_ref() {
            Some(middleware) => middleware.wrap_reader(reader, &self.connector.name),
            None => Ok(reader),
        }
    }

    /// Opens and generation-locally wraps one writer lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the route, runtime, writer, or middleware cannot be opened.
    pub fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        self.route_profile(route)?;
        let runtime = self.connector.open_runtime()?;
        let sink = contract_sink(completions);
        let writer = runtime.open_writer(route, Arc::clone(&sink) as Arc<dyn CompletionSink>)?;
        let writer = match self.connector.middleware.lock().as_ref() {
            Some(middleware) => middleware.wrap_writer(writer, &self.connector.name)?,
            None => writer,
        };
        Ok(enforce_writer_contract(writer, sink))
    }
}

impl Drop for Binding {
    fn drop(&mut self) {
        self.generation.release_binding();
    }
}

fn prepare_generation(next: &Arc<Generation>, previous: Option<&Arc<Generation>>) -> Result<()> {
    let previous_exclusive = previous.map_or_else(BTreeSet::new, |generation| {
        generation
            .connectors
            .values()
            .flat_map(|entry| entry.compiled.exclusive_runtime_keys().iter().cloned())
            .collect()
    });

    for (name, entry) in &next.connectors {
        if !entry.compiled.open_runtime_eagerly() {
            continue;
        }
        if let Some(prior) = previous.and_then(|generation| generation.connectors.get(name))
            && prior.config.connector_type == entry.config.connector_type
            && prior.config.settings == entry.config.settings
            && let Some(owner) = prior.runtime.lock().as_ref().cloned()
        {
            owner.retain();
            *entry.runtime.lock() = Some(owner);
            continue;
        }
        if let Some(key) = entry
            .compiled
            .exclusive_runtime_keys()
            .iter()
            .find(|key| previous_exclusive.contains(*key))
        {
            return Err(CoreError::Unavailable(format!(
                "connector {name:?}: exclusive runtime {key:?} requires drain"
            )));
        }
        entry.open_runtime()?;
    }
    Ok(())
}

async fn close_middlewares(connectors: &BTreeMap<String, Arc<CompiledEntry>>) {
    let resources: Vec<_> = connectors
        .values()
        .filter_map(|entry| entry.middleware.lock().take())
        .collect();
    for middleware in resources {
        let _ = middleware.close().await;
    }
}

async fn close_generation_resources(
    connectors: &BTreeMap<String, Arc<CompiledEntry>>,
) -> Result<()> {
    let runtimes: Vec<_> = connectors
        .values()
        .filter_map(|entry| entry.runtime.lock().take())
        .collect();
    let middlewares: Vec<_> = connectors
        .values()
        .filter_map(|entry| entry.middleware.lock().take())
        .collect();
    let mut errors = Vec::new();
    for owner in runtimes {
        if let Err(error) = owner.release().await {
            errors.push(error.to_string());
        }
    }
    for middleware in middlewares {
        if let Err(error) = middleware.close().await {
            errors.push(error.to_string());
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CoreError::Internal(errors.join("; ")))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationState {
    Published,
    Draining,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenerationStatus {
    pub id: u64,
    pub state: GenerationState,
    pub bindings: usize,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenerationTransition {
    pub sequence: u64,
    pub generation: GenerationStatus,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogStatus {
    pub current: Option<GenerationStatus>,
    pub draining: Vec<GenerationStatus>,
    pub retired_total: u64,
    pub recent_transitions: Vec<GenerationTransition>,
}

#[derive(Debug, Default)]
struct CatalogLifecycle {
    draining: Mutex<BTreeMap<u64, Arc<Generation>>>,
    retired_total: AtomicU64,
    transition_sequence: AtomicU64,
    transitions: Mutex<VecDeque<GenerationTransition>>,
}

impl CatalogLifecycle {
    fn record(&self, generation: &Generation, state: GenerationState, error: String) {
        let sequence = self.transition_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let mut transitions = self.transitions.lock();
        transitions.push_back(GenerationTransition {
            sequence,
            generation: GenerationStatus {
                id: generation.id(),
                state,
                bindings: generation.binding_count(),
                error,
            },
        });
        while transitions.len() > GENERATION_TRANSITION_LIMIT {
            transitions.pop_front();
        }
    }

    fn track_draining(self: &Arc<Self>, generation: Arc<Generation>) {
        if self
            .draining
            .lock()
            .insert(generation.id(), Arc::clone(&generation))
            .is_some()
        {
            return;
        }
        self.record(&generation, GenerationState::Draining, String::new());
        let lifecycle = Arc::clone(self);
        tokio::spawn(async move {
            let error = generation
                .wait_closed()
                .await
                .err()
                .map_or_else(String::new, |error| error.to_string());
            lifecycle.draining.lock().remove(&generation.id());
            lifecycle.retired_total.fetch_add(1, Ordering::Relaxed);
            lifecycle.record(&generation, GenerationState::Retired, error);
        });
    }
}

/// Atomically publishes complete immutable connector generations.
pub struct Catalog {
    compiler: Arc<GenerationCompiler>,
    current: ArcSwapOption<Generation>,
    reload: AsyncMutex<()>,
    closed: AtomicBool,
    closing: Mutex<Option<Arc<Generation>>>,
    lifecycle: Arc<CatalogLifecycle>,
}

impl fmt::Debug for Catalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Catalog")
            .field("current", &self.current.load_full())
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Catalog {
    /// Compiles, preflights, and publishes the initial complete snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when compilation or eager-runtime preflight fails.
    pub async fn compile(
        configs: &ConnectorsConfig,
        compiler: Arc<GenerationCompiler>,
    ) -> Result<Self> {
        let generation = compiler.compile(configs).await?;
        if let Err(error) = prepare_generation(&generation, None) {
            generation.abort().await;
            return Err(error);
        }
        let lifecycle = Arc::new(CatalogLifecycle::default());
        lifecycle.record(&generation, GenerationState::Published, String::new());
        Ok(Self {
            compiler,
            current: ArcSwapOption::new(Some(generation)),
            reload: AsyncMutex::new(()),
            closed: AtomicBool::new(false),
            closing: Mutex::new(None),
            lifecycle,
        })
    }

    pub fn current(&self) -> Option<Arc<Generation>> {
        self.current.load_full()
    }

    #[must_use]
    pub fn status(&self) -> CatalogStatus {
        let mut draining = self
            .lifecycle
            .draining
            .lock()
            .values()
            .map(|generation| GenerationStatus {
                id: generation.id(),
                state: GenerationState::Draining,
                bindings: generation.binding_count(),
                error: String::new(),
            })
            .collect::<Vec<_>>();
        draining.sort_by_key(|generation| generation.id);
        CatalogStatus {
            current: self.current.load_full().map(|generation| GenerationStatus {
                id: generation.id(),
                state: GenerationState::Published,
                bindings: generation.binding_count(),
                error: String::new(),
            }),
            draining,
            retired_total: self.lifecycle.retired_total.load(Ordering::Relaxed),
            recent_transitions: self.lifecycle.transitions.lock().iter().cloned().collect(),
        }
    }

    /// Compiles and atomically publishes one complete replacement snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error without publication when compilation or preflight fails, or when closed.
    pub async fn reload(&self, configs: &ConnectorsConfig) -> Result<Arc<Generation>> {
        let _guard = self.reload.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(CoreError::Closed);
        }
        let next = self.compiler.compile(configs).await?;
        let previous = self.current.load_full();
        if let Err(error) = prepare_generation(&next, previous.as_ref()) {
            next.abort().await;
            return Err(error);
        }
        let previous = self.current.swap(Some(Arc::clone(&next)));
        self.lifecycle
            .record(&next, GenerationState::Published, String::new());
        if let Some(previous) = previous {
            self.lifecycle.track_draining(Arc::clone(&previous));
            previous.retire();
        }
        Ok(next)
    }

    /// Retires the active generation and waits for its bindings and resources to drain.
    ///
    /// # Errors
    ///
    /// Returns a generation resource cleanup error.
    pub async fn close(&self) -> Result<()> {
        self.close_with_timeout(DEFAULT_CATALOG_CLEANUP_TIMEOUT)
            .await
    }

    /// Retires the active generation and bounds how long the caller waits for drain.
    /// Generation cleanup continues after a timeout and can be awaited by a later call.
    ///
    /// # Errors
    ///
    /// Returns cleanup failures or [`CoreError::Unavailable`] when the deadline expires.
    pub async fn close_with_timeout(&self, cleanup_timeout: Duration) -> Result<()> {
        let _guard = self.reload.lock().await;
        let generation = if self.closed.swap(true, Ordering::AcqRel) {
            self.closing.lock().clone()
        } else {
            let generation = self.current.swap(None);
            if let Some(generation) = generation.as_ref() {
                self.lifecycle.track_draining(Arc::clone(generation));
                generation.retire();
            }
            self.closing.lock().clone_from(&generation);
            generation
        };
        let Some(generation) = generation else {
            return Ok(());
        };
        match timeout(cleanup_timeout, generation.wait_closed()).await {
            Ok(result) => {
                *self.closing.lock() = None;
                result
            }
            Err(_) => Err(CoreError::Unavailable(format!(
                "connector catalog cleanup timed out after {cleanup_timeout:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use serde_json::Value;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        AcceptanceGuarantee, BoxFuture, Capabilities, Completion, CompletionSink, ConnectorRuntime,
        Message, OperationToken, Reader, ReaderEventSink, Writer,
    };

    struct TestDescriptor {
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorDescriptor for TestDescriptor {
        fn compile(&self, _settings: &Value) -> Result<Arc<dyn CompiledConnector>> {
            let mut routes = BTreeMap::new();
            routes.insert(
                "route".into(),
                RouteProfile {
                    capabilities: Capabilities::PRODUCE,
                    produce_guarantee: AcceptanceGuarantee::Peer,
                    ..RouteProfile::default()
                },
            );
            Ok(Arc::new(TestCompiled {
                routes,
                opens: Arc::clone(&self.opens),
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    struct TestCompiled {
        routes: BTreeMap<String, RouteProfile>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    impl CompiledConnector for TestCompiled {
        fn routes(&self) -> &BTreeMap<String, RouteProfile> {
            &self.routes
        }

        fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
            self.opens.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(TestRuntime {
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    struct TestRuntime {
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorRuntime for TestRuntime {
        fn open_reader(
            &self,
            _route: &str,
            _auto_settle: bool,
            _events: Arc<dyn ReaderEventSink>,
        ) -> Result<Arc<dyn Reader>> {
            Err(CoreError::OperationUnsupported)
        }

        fn open_writer(
            &self,
            _route: &str,
            completions: Arc<dyn CompletionSink>,
        ) -> Result<Arc<dyn Writer>> {
            Ok(Arc::new(TestWriter { completions }))
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                self.closes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    struct TestWriter {
        completions: Arc<dyn CompletionSink>,
    }

    impl TestWriter {
        fn complete(&self, token: OperationToken) {
            self.completions.complete(Completion {
                token,
                result: Ok(()),
            });
        }
    }

    impl Writer for TestWriter {
        fn produce(&self, token: OperationToken, _message: Message) -> Result<()> {
            self.complete(token);
            Ok(())
        }

        fn flush(&self, token: OperationToken) -> Result<()> {
            self.complete(token);
            Ok(())
        }

        fn begin_transaction(&self, token: OperationToken) -> Result<()> {
            self.complete(token);
            Ok(())
        }

        fn commit_transaction(&self, token: OperationToken) -> Result<()> {
            self.complete(token);
            Ok(())
        }

        fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
            self.complete(token);
            Ok(())
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    struct IgnoreCompletions;

    impl CompletionSink for IgnoreCompletions {
        fn complete(&self, _completion: Completion) {}
    }

    fn config(connector_type: &str) -> ConnectorsConfig {
        BTreeMap::from([(
            "connector".into(),
            ConnectorConfig {
                connector_type: connector_type.into(),
                overridable: Vec::new(),
                bind_middlewares: Vec::new(),
                connector_middlewares: Vec::new(),
                settings: Value::Null,
            },
        )])
    }

    fn compiler(opens: Arc<AtomicUsize>, closes: Arc<AtomicUsize>) -> Arc<GenerationCompiler> {
        let registry = Arc::new(ConnectorRegistry::default());
        registry
            .register("test", Arc::new(TestDescriptor { opens, closes }))
            .expect("register test connector");
        Arc::new(GenerationCompiler::without_middlewares(registry))
    }

    #[tokio::test]
    async fn rejected_reload_preserves_active_generation() {
        let compiler = compiler(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let catalog = Catalog::compile(&config("test"), compiler)
            .await
            .expect("compile initial catalog");
        let current = catalog.current().expect("published generation");

        assert!(catalog.reload(&config("missing")).await.is_err());
        assert!(Arc::ptr_eq(
            &current,
            &catalog.current().expect("active generation remains")
        ));
        catalog.close().await.expect("close catalog");
    }

    #[tokio::test]
    async fn ordinary_runtime_opens_lazily() {
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let catalog = Catalog::compile(
            &config("test"),
            compiler(Arc::clone(&opens), Arc::clone(&closes)),
        )
        .await
        .expect("compile catalog");
        assert_eq!(opens.load(Ordering::Acquire), 0);

        let binding = catalog
            .current()
            .expect("published generation")
            .acquire("connector")
            .expect("acquire binding");
        assert_eq!(opens.load(Ordering::Acquire), 0);
        let writer = binding
            .open_writer("route", Arc::new(IgnoreCompletions))
            .expect("open writer");
        assert_eq!(opens.load(Ordering::Acquire), 1);
        drop(writer);
        drop(binding);

        catalog.close().await.expect("close catalog");
        assert_eq!(closes.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn retired_generation_waits_for_last_binding() {
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let catalog = Catalog::compile(
            &config("test"),
            compiler(Arc::clone(&opens), Arc::clone(&closes)),
        )
        .await
        .expect("compile catalog");
        let old = catalog.current().expect("published generation");
        let binding = old.acquire("connector").expect("acquire binding");
        let writer = binding
            .open_writer("route", Arc::new(IgnoreCompletions))
            .expect("open writer");
        drop(writer);

        catalog
            .reload(&config("test"))
            .await
            .expect("publish replacement");
        let status = catalog.status();
        assert_eq!(
            status.current.as_ref().map(|current| current.id),
            catalog.current().map(|current| current.id())
        );
        assert_eq!(status.draining.len(), 1);
        assert_eq!(status.draining[0].id, old.id());
        assert_eq!(status.draining[0].bindings, 1);
        assert!(
            timeout(Duration::from_millis(20), old.wait_closed())
                .await
                .is_err()
        );

        drop(binding);
        timeout(Duration::from_secs(1), old.wait_closed())
            .await
            .expect("generation should close")
            .expect("generation cleanup");
        assert_eq!(closes.load(Ordering::Acquire), 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = catalog.status();
                if status.draining.is_empty() && status.retired_total == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("catalog status should observe retirement");
        catalog.close().await.expect("close catalog");
    }
}
