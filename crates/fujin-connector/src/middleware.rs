use std::sync::Arc;

use fujin_error::{CoreError, Result};

use crate::{BoxFuture, MiddlewareConfig, Reader, Writer};

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
