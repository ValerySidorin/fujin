use std::{collections::BTreeMap, sync::Arc};

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
