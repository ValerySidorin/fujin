//! Public native transport plugin contracts and explicit registry.

pub mod listener;
pub mod settings;
pub mod tls;

use std::{collections::BTreeMap, fmt, sync::Arc};

use anyhow::{Result, bail};
use fujin_core::{BindMiddlewareRunner, BoxFuture, Catalog};
use fujin_upgrade::{InheritedListeners, ListenerRegistry};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

/// One configured native transport plugin instance.
#[derive(Clone, Debug, Deserialize)]
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub settings: Value,
}

const fn default_enabled() -> bool {
    true
}

/// One listener after it has bound its actual address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint {
    pub interface: String,
    pub transport: Option<String>,
    pub network: String,
    pub address: String,
    pub path: Option<String>,
    pub tls: bool,
}

impl Endpoint {
    #[must_use]
    pub fn native(
        transport: impl Into<String>,
        network: impl Into<String>,
        address: impl Into<String>,
        path: Option<String>,
        tls: bool,
    ) -> Self {
        Self {
            interface: "native".into(),
            transport: Some(transport.into()),
            network: network.into(),
            address: address.into(),
            path,
            tls,
        }
    }

    #[must_use]
    pub fn grpc(address: impl Into<String>, tls: bool) -> Self {
        Self {
            interface: "grpc".into(),
            transport: None,
            network: "tcp".into(),
            address: address.into(),
            path: None,
            tls,
        }
    }

    #[must_use]
    pub fn health(address: impl Into<String>) -> Self {
        Self {
            interface: "health".into(),
            transport: None,
            network: "tcp".into(),
            address: address.into(),
            path: None,
            tls: false,
        }
    }
}

/// One configured native transport listener.
pub trait CompiledTransport: Send + Sync + 'static {
    /// Runs the listener until shutdown or a terminal listener failure.
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>>;

    /// Number of independently ready listeners started by this compiled transport.
    fn listener_count(&self) -> usize {
        1
    }
}

/// Side-effect-free compiler for one statically linked native transport type.
pub trait TransportPlugin: Send + Sync + 'static {
    /// Parses and validates immutable transport settings without binding a socket.
    ///
    /// # Errors
    ///
    /// Returns an error when the settings are invalid or immutable resources cannot be compiled.
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>>;
}

/// One explicitly registered, statically linked native transport plugin.
#[derive(Clone)]
pub struct TransportRegistration {
    name: String,
    plugin: Arc<dyn TransportPlugin>,
}

impl TransportRegistration {
    #[must_use]
    pub fn new(name: impl Into<String>, plugin: impl TransportPlugin) -> Self {
        Self {
            name: name.into(),
            plugin: Arc::new(plugin),
        }
    }

    #[must_use]
    pub fn from_arc(name: impl Into<String>, plugin: Arc<dyn TransportPlugin>) -> Self {
        Self {
            name: name.into(),
            plugin,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for TransportRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportRegistration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Explicit registry of statically linked native transport plugins.
#[derive(Default)]
pub struct TransportRegistry {
    plugins: RwLock<BTreeMap<String, Arc<dyn TransportPlugin>>>,
}

impl fmt::Debug for TransportRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportRegistry")
            .field("plugins", &self.plugins.read().keys())
            .finish()
    }
}

impl TransportRegistry {
    /// Registers one transport plugin exactly once.
    ///
    /// # Errors
    ///
    /// Returns an error when the plugin name is empty or already registered.
    pub fn register(&self, registration: TransportRegistration) -> Result<()> {
        let TransportRegistration { name, plugin } = registration;
        if name.is_empty() {
            bail!("transport name is empty");
        }
        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            bail!("transport {name:?} is already registered");
        }
        plugins.insert(name, plugin);
        Ok(())
    }

    /// Compiles one enabled transport entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the plugin is absent, settings are invalid, or compilation yields no
    /// listener.
    pub fn compile(&self, config: &TransportConfig) -> Result<ConfiguredTransport> {
        let plugins = self.plugins.read();
        let plugin = plugins.get(&config.transport_type).ok_or_else(|| {
            anyhow::anyhow!(
                "transport {:?} is not registered (available: {:?})",
                config.transport_type,
                plugins.keys().collect::<Vec<_>>()
            )
        })?;
        let listener = plugin.compile(&config.settings)?;
        if listener.listener_count() == 0 {
            bail!(
                "transport {:?} compiled zero listeners",
                config.transport_type
            );
        }
        Ok(ConfiguredTransport {
            name: config.transport_type.clone(),
            listener,
        })
    }

    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.plugins.read().keys().cloned().collect()
    }
}

/// One validated transport listener ready to be started by the server lifecycle.
#[derive(Clone)]
pub struct ConfiguredTransport {
    name: String,
    listener: Arc<dyn CompiledTransport>,
}

impl ConfiguredTransport {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn listener_count(&self) -> usize {
        self.listener.listener_count()
    }

    pub fn serve(&self, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Arc::clone(&self.listener).serve(context)
    }
}

impl fmt::Debug for ConfiguredTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguredTransport")
            .field("name", &self.name)
            .field("listener_count", &self.listener.listener_count())
            .finish()
    }
}

/// Host services supplied to one compiled transport listener.
#[derive(Clone)]
pub struct TransportContext {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    pub(crate) build: String,
    pub(crate) shutdown: CancellationToken,
    pub(crate) ready: mpsc::UnboundedSender<Endpoint>,
    pub(crate) registry: ListenerRegistry,
    pub(crate) inherited: InheritedListeners,
}

impl fmt::Debug for TransportContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportContext")
            .field("build", &self.build)
            .field("shutdown", &self.shutdown.is_cancelled())
            .field("listener_registry", &self.registry)
            .field("inherited_listeners", &self.inherited)
            .finish_non_exhaustive()
    }
}

impl TransportContext {
    #[doc(hidden)]
    pub fn new(
        catalog: Arc<Catalog>,
        bind_middlewares: Arc<dyn BindMiddlewareRunner>,
        build: String,
        shutdown: CancellationToken,
        ready: mpsc::UnboundedSender<Endpoint>,
        listener_registry: ListenerRegistry,
        inherited_listeners: InheritedListeners,
    ) -> Self {
        Self {
            catalog,
            bind_middlewares,
            build,
            shutdown,
            ready,
            registry: listener_registry,
            inherited: inherited_listeners,
        }
    }

    /// Reports that one listener owned by this transport is accepting connections.
    pub fn signal_ready(&self, endpoint: Endpoint) {
        let _ = self.ready.send(endpoint);
    }

    #[must_use]
    pub fn shutdown(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    #[must_use]
    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    #[must_use]
    pub fn bind_middlewares(&self) -> Arc<dyn BindMiddlewareRunner> {
        Arc::clone(&self.bind_middlewares)
    }

    #[must_use]
    pub fn listener_registry(&self) -> &ListenerRegistry {
        &self.registry
    }

    #[must_use]
    pub fn inherited_listeners(&self) -> &InheritedListeners {
        &self.inherited
    }

    #[must_use]
    pub fn build(&self) -> &str {
        &self.build
    }

    /// Runs one native Fujin v1 session over an accepted byte stream.
    ///
    /// # Errors
    ///
    /// Returns native framing, session, or stream I/O failures.
    pub async fn serve_native_stream<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        fujin_native::run_with_shutdown(
            stream,
            Arc::clone(&self.catalog),
            Arc::clone(&self.bind_middlewares),
            self.build.clone(),
            self.shutdown.clone().cancelled_owned(),
        )
        .await
        .map_err(anyhow::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestPlugin;

    impl TransportPlugin for TestPlugin {
        fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
            let listeners = settings
                .get("listeners")
                .and_then(Value::as_u64)
                .unwrap_or(1);
            Ok(Arc::new(TestTransport {
                listeners: usize::try_from(listeners)?,
            }))
        }
    }

    struct TestTransport {
        listeners: usize,
    }

    impl CompiledTransport for TestTransport {
        fn serve(self: Arc<Self>, _context: TransportContext) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn listener_count(&self) -> usize {
            self.listeners
        }
    }

    #[test]
    fn registry_compiles_explicit_transport_plugin() {
        let registry = TransportRegistry::default();
        registry
            .register(TransportRegistration::new("test", TestPlugin))
            .expect("register transport");
        let compiled = registry
            .compile(&TransportConfig {
                transport_type: "test".into(),
                enabled: true,
                settings: serde_json::json!({"listeners": 2}),
            })
            .expect("compile transport");

        assert_eq!(compiled.name(), "test");
        assert_eq!(compiled.listener_count(), 2);
        assert_eq!(registry.list(), ["test"]);
    }

    #[test]
    fn registry_rejects_duplicate_and_zero_listener_plugins() {
        let registry = TransportRegistry::default();
        registry
            .register(TransportRegistration::new("test", TestPlugin))
            .expect("register transport");
        assert!(
            registry
                .register(TransportRegistration::new("test", TestPlugin))
                .is_err()
        );
        assert!(
            registry
                .compile(&TransportConfig {
                    transport_type: "test".into(),
                    enabled: true,
                    settings: serde_json::json!({"listeners": 0}),
                })
                .is_err()
        );
    }
}
