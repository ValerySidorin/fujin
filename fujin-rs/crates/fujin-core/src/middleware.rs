use std::{collections::BTreeMap, fmt, sync::Arc};

use parking_lot::RwLock;
use serde_json::Value;

use crate::{BoxFuture, ConnectorConfig, CoreError, MiddlewareConfig, Reader, Result, Writer};

#[derive(Debug)]
pub struct BindContext<'a> {
    pub connector_name: &'a str,
    pub connector: &'a ConnectorConfig,
    pub metadata: &'a mut BTreeMap<String, String>,
}

pub trait BindMiddleware: Send + Sync + 'static {
    /// Evaluates one BIND request before connector acquisition.
    ///
    /// # Errors
    ///
    /// Returns an authentication, authorization, or validation error to reject BIND.
    fn process(&self, context: &mut BindContext<'_>) -> Result<()>;
}

/// Public BIND middleware plugin contract.
///
/// The plugin receives its inline configuration on every BIND so one registered implementation can
/// serve differently configured connector instances without process-global state.
pub trait BindMiddlewarePlugin: Send + Sync + 'static {
    /// Evaluates one configured BIND middleware invocation.
    ///
    /// # Errors
    ///
    /// Returns an authentication, authorization, or validation error to reject BIND.
    fn process(
        &self,
        settings: &BTreeMap<String, Value>,
        context: &mut BindContext<'_>,
    ) -> Result<()>;
}

pub trait BindMiddlewareRunner: Send + Sync + 'static {
    /// Runs the configured BIND middleware chain in declaration order.
    ///
    /// # Errors
    ///
    /// Returns the first middleware compilation or request rejection error.
    fn run(
        &self,
        connector_name: &str,
        connector: &ConnectorConfig,
        metadata: &mut BTreeMap<String, String>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoBindMiddleware;

impl BindMiddlewareRunner for NoBindMiddleware {
    fn run(
        &self,
        _connector_name: &str,
        connector: &ConnectorConfig,
        _metadata: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        if connector
            .bind_middlewares
            .iter()
            .any(|middleware| middleware.enabled)
        {
            return Err(CoreError::InvalidConfig(
                "bind middleware configured but no runner is installed".into(),
            ));
        }
        Ok(())
    }
}

/// Explicit registry and runner for statically linked BIND middleware plugins.
#[derive(Default)]
pub struct BindMiddlewareRegistry {
    plugins: RwLock<BTreeMap<String, Arc<dyn BindMiddlewarePlugin>>>,
}

impl fmt::Debug for BindMiddlewareRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindMiddlewareRegistry")
            .field("plugins", &self.plugins.read().keys())
            .finish()
    }
}

impl BindMiddlewareRegistry {
    /// Registers one BIND middleware plugin exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] for an empty or duplicate name.
    pub fn register(
        &self,
        name: impl Into<String>,
        plugin: Arc<dyn BindMiddlewarePlugin>,
    ) -> Result<()> {
        let name = name.into();
        if name.is_empty() {
            return Err(CoreError::InvalidConfig(
                "bind middleware name is empty".into(),
            ));
        }
        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(CoreError::InvalidConfig(format!(
                "bind middleware {name:?} is already registered"
            )));
        }
        plugins.insert(name, plugin);
        Ok(())
    }

    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.plugins.read().keys().cloned().collect()
    }
}

impl BindMiddlewareRunner for BindMiddlewareRegistry {
    fn run(
        &self,
        connector_name: &str,
        connector: &ConnectorConfig,
        metadata: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        let plugins = self.plugins.read();
        let mut context = BindContext {
            connector_name,
            connector,
            metadata,
        };
        for config in connector
            .bind_middlewares
            .iter()
            .filter(|entry| entry.enabled)
        {
            let plugin = plugins.get(&config.name).ok_or_else(|| {
                CoreError::InvalidConfig(format!(
                    "bind middleware {:?} is not registered (available: {:?})",
                    config.name,
                    plugins.keys().collect::<Vec<_>>()
                ))
            })?;
            plugin.process(&config.settings, &mut context)?;
        }
        Ok(())
    }
}

/// Generation-scoped connector middleware resources.
pub trait CompiledConnectorMiddleware: Send + Sync + 'static {
    /// Wraps one session-scoped reader lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the middleware cannot preserve the reader contract.
    fn wrap_reader(&self, reader: Arc<dyn Reader>, connector_name: &str)
    -> Result<Arc<dyn Reader>>;
    /// Wraps one session-scoped writer lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the middleware cannot preserve the writer contract.
    fn wrap_writer(&self, writer: Arc<dyn Writer>, connector_name: &str)
    -> Result<Arc<dyn Writer>>;
    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>>;
}

pub trait ConnectorMiddlewareCompiler: Send + Sync + 'static {
    /// Compiles an immutable middleware chain without broker I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when any enabled middleware is missing or invalid.
    fn compile(
        &self,
        configs: &[MiddlewareConfig],
    ) -> Result<Option<Arc<dyn CompiledConnectorMiddleware>>>;
}

/// Public connector middleware plugin contract.
pub trait ConnectorMiddlewarePlugin: Send + Sync + 'static {
    /// Compiles one inline middleware configuration into generation-scoped resources.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is invalid or resources cannot be compiled.
    fn compile(
        &self,
        settings: &BTreeMap<String, Value>,
    ) -> Result<Arc<dyn CompiledConnectorMiddleware>>;
}

/// Explicit registry and compiler for statically linked connector middleware plugins.
#[derive(Default)]
pub struct ConnectorMiddlewareRegistry {
    plugins: RwLock<BTreeMap<String, Arc<dyn ConnectorMiddlewarePlugin>>>,
}

impl fmt::Debug for ConnectorMiddlewareRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorMiddlewareRegistry")
            .field("plugins", &self.plugins.read().keys())
            .finish()
    }
}

impl ConnectorMiddlewareRegistry {
    /// Registers one connector middleware plugin exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] for an empty or duplicate name.
    pub fn register(
        &self,
        name: impl Into<String>,
        plugin: Arc<dyn ConnectorMiddlewarePlugin>,
    ) -> Result<()> {
        let name = name.into();
        if name.is_empty() {
            return Err(CoreError::InvalidConfig(
                "connector middleware name is empty".into(),
            ));
        }
        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(CoreError::InvalidConfig(format!(
                "connector middleware {name:?} is already registered"
            )));
        }
        plugins.insert(name, plugin);
        Ok(())
    }

    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.plugins.read().keys().cloned().collect()
    }
}

impl ConnectorMiddlewareCompiler for ConnectorMiddlewareRegistry {
    fn compile(
        &self,
        configs: &[MiddlewareConfig],
    ) -> Result<Option<Arc<dyn CompiledConnectorMiddleware>>> {
        let plugins = self.plugins.read();
        let enabled = configs.iter().filter(|entry| entry.enabled);
        let mut compiled = Vec::new();
        for config in enabled {
            let plugin = plugins.get(&config.name).ok_or_else(|| {
                CoreError::InvalidConfig(format!(
                    "connector middleware {:?} is not registered (available: {:?})",
                    config.name,
                    plugins.keys().collect::<Vec<_>>()
                ))
            })?;
            compiled.push(plugin.compile(&config.settings)?);
        }
        if compiled.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(CompiledMiddlewareChain { compiled })))
    }
}

struct CompiledMiddlewareChain {
    compiled: Vec<Arc<dyn CompiledConnectorMiddleware>>,
}

impl CompiledConnectorMiddleware for CompiledMiddlewareChain {
    fn wrap_reader(
        &self,
        mut reader: Arc<dyn Reader>,
        connector_name: &str,
    ) -> Result<Arc<dyn Reader>> {
        for middleware in &self.compiled {
            reader = middleware.wrap_reader(reader, connector_name)?;
        }
        Ok(reader)
    }

    fn wrap_writer(
        &self,
        mut writer: Arc<dyn Writer>,
        connector_name: &str,
    ) -> Result<Arc<dyn Writer>> {
        for middleware in &self.compiled {
            writer = middleware.wrap_writer(writer, connector_name)?;
        }
        Ok(writer)
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let mut first_error = None;
            for middleware in self.compiled.iter().rev() {
                if let Err(error) = Arc::clone(middleware).close().await
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }
}

#[derive(Debug, Default)]
pub struct NoConnectorMiddleware;

impl ConnectorMiddlewareCompiler for NoConnectorMiddleware {
    fn compile(
        &self,
        configs: &[MiddlewareConfig],
    ) -> Result<Option<Arc<dyn CompiledConnectorMiddleware>>> {
        if configs.iter().any(|config| config.enabled) {
            return Err(CoreError::InvalidConfig(
                "connector middleware configured but no compiler is installed".into(),
            ));
        }
        Ok(None)
    }
}

/// Reads one required non-empty string setting.
///
/// # Errors
///
/// Returns [`CoreError::InvalidConfig`] when the key is missing, empty, or not a string.
pub fn required_string(settings: &BTreeMap<String, Value>, key: &str) -> Result<String> {
    settings
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| CoreError::InvalidConfig(format!("{key} is required")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TrackingBindPlugin {
        name: &'static str,
        events: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl BindMiddlewarePlugin for TrackingBindPlugin {
        fn process(
            &self,
            settings: &BTreeMap<String, Value>,
            context: &mut BindContext<'_>,
        ) -> Result<()> {
            self.events.lock().push(format!(
                "{}:{}:{}",
                self.name,
                context.connector_name,
                settings["value"].as_str().expect("string setting")
            ));
            context
                .metadata
                .insert(self.name.into(), "processed".into());
            Ok(())
        }
    }

    fn middleware(name: &str, value: &str) -> MiddlewareConfig {
        MiddlewareConfig {
            name: name.into(),
            enabled: true,
            settings: BTreeMap::from([("value".into(), Value::String(value.into()))]),
        }
    }

    fn connector(bind: Vec<MiddlewareConfig>) -> ConnectorConfig {
        ConnectorConfig {
            connector_type: "test".into(),
            overridable: Vec::new(),
            bind_middlewares: bind,
            connector_middlewares: Vec::new(),
            settings: Value::Null,
        }
    }

    #[test]
    fn bind_registry_runs_enabled_plugins_in_configuration_order() {
        let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let registry = BindMiddlewareRegistry::default();
        registry
            .register(
                "first",
                Arc::new(TrackingBindPlugin {
                    name: "first",
                    events: Arc::clone(&events),
                }),
            )
            .expect("register first plugin");
        registry
            .register(
                "second",
                Arc::new(TrackingBindPlugin {
                    name: "second",
                    events: Arc::clone(&events),
                }),
            )
            .expect("register second plugin");
        let mut metadata = BTreeMap::new();

        registry
            .run(
                "primary",
                &connector(vec![middleware("first", "a"), middleware("second", "b")]),
                &mut metadata,
            )
            .expect("run configured middleware");

        assert_eq!(*events.lock(), ["first:primary:a", "second:primary:b"]);
        assert_eq!(metadata["first"], "processed");
        assert_eq!(metadata["second"], "processed");
    }

    struct TrackingConnectorPlugin {
        name: &'static str,
        events: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl ConnectorMiddlewarePlugin for TrackingConnectorPlugin {
        fn compile(
            &self,
            settings: &BTreeMap<String, Value>,
        ) -> Result<Arc<dyn CompiledConnectorMiddleware>> {
            self.events.lock().push(format!(
                "compile-{}-{}",
                self.name,
                settings["value"].as_str().expect("string setting")
            ));
            Ok(Arc::new(TrackingCompiledMiddleware {
                name: self.name,
                events: Arc::clone(&self.events),
            }))
        }
    }

    struct TrackingCompiledMiddleware {
        name: &'static str,
        events: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl CompiledConnectorMiddleware for TrackingCompiledMiddleware {
        fn wrap_reader(
            &self,
            reader: Arc<dyn Reader>,
            _connector_name: &str,
        ) -> Result<Arc<dyn Reader>> {
            Ok(reader)
        }

        fn wrap_writer(
            &self,
            writer: Arc<dyn Writer>,
            _connector_name: &str,
        ) -> Result<Arc<dyn Writer>> {
            Ok(writer)
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                self.events.lock().push(format!("close-{}", self.name));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn connector_registry_compiles_in_order_and_closes_in_reverse() {
        let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let registry = ConnectorMiddlewareRegistry::default();
        for name in ["first", "second"] {
            registry
                .register(
                    name,
                    Arc::new(TrackingConnectorPlugin {
                        name,
                        events: Arc::clone(&events),
                    }),
                )
                .expect("register connector middleware plugin");
        }

        let compiled = registry
            .compile(&[middleware("first", "a"), middleware("second", "b")])
            .expect("compile middleware chain")
            .expect("non-empty middleware chain");
        compiled.close().await.expect("close middleware chain");

        assert_eq!(
            *events.lock(),
            [
                "compile-first-a",
                "compile-second-b",
                "close-second",
                "close-first",
            ]
        );
    }

    #[test]
    fn registries_reject_duplicate_names() {
        let bind = BindMiddlewareRegistry::default();
        let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
        bind.register(
            "duplicate",
            Arc::new(TrackingBindPlugin {
                name: "duplicate",
                events: Arc::clone(&events),
            }),
        )
        .expect("register first BIND middleware");
        assert!(
            bind.register(
                "duplicate",
                Arc::new(TrackingBindPlugin {
                    name: "duplicate",
                    events,
                }),
            )
            .is_err()
        );
    }
}
