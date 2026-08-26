use std::sync::Arc;

use fujin_connector::Catalog;
use fujin_middleware::BindMiddlewareRunner;
use fujin_transport::{BoxFuture, BoxNativeStream, NativeSessionConfig, NativeSessionService};
use tokio_util::sync::CancellationToken;

/// Runtime adapter from the public transport seam to the native Session Core host.
#[derive(Clone)]
pub(crate) struct NativeSessions {
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    build: String,
    shutdown: CancellationToken,
}

impl NativeSessions {
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        bind_middlewares: Arc<dyn BindMiddlewareRunner>,
        build: String,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            catalog,
            bind_middlewares,
            build,
            shutdown,
        }
    }
}

impl NativeSessionService for NativeSessions {
    fn serve(
        &self,
        stream: BoxNativeStream,
        config: NativeSessionConfig,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        let catalog = Arc::clone(&self.catalog);
        let bind_middlewares = Arc::clone(&self.bind_middlewares);
        let build = self.build.clone();
        let shutdown = self.shutdown.clone().cancelled_owned();
        Box::pin(async move {
            fujin_native::run_with_config_and_shutdown(
                stream,
                catalog,
                bind_middlewares,
                build,
                config,
                shutdown,
            )
            .await
            .map_err(anyhow::Error::from)
        })
    }
}
