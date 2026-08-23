#![cfg(unix)]

//! Optional native Fujin v1 transport over Unix sockets.

use std::{io, sync::Arc};

use anyhow::{Context, Result, bail};
use fujin_core::BoxFuture;
use fujin_transport::{
    CompiledTransport, Endpoint, TransportContext, TransportPlugin, TransportRegistration,
    listener::{bind_unix, drain_tasks},
    settings::NativeProtocolSettings,
};
use fujin_upgrade::ListenerMetadata;
use serde::Deserialize;
use serde_json::Value;
use tokio::task::JoinSet;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    path: String,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Debug)]
struct Plugin;

impl TransportPlugin for Plugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: Settings =
            serde_json::from_value(settings.clone()).context("parse unix transport settings")?;
        settings.fujin.validate_supported()?;
        if settings.path.is_empty() {
            bail!("Unix path is required");
        }
        Ok(Arc::new(Transport {
            path: settings.path,
        }))
    }
}

#[derive(Debug)]
struct Transport {
    path: String,
}

impl CompiledTransport for Transport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { self.run(context).await })
    }
}

impl Transport {
    async fn run(&self, context: TransportContext) -> Result<()> {
        let listener = bind_unix(
            &self.path,
            ListenerMetadata::unix(self.path.clone()),
            context.listener_registry(),
            context.inherited_listeners(),
        )?;
        context.signal_ready(Endpoint::native(
            "unix",
            "unix",
            self.path.clone(),
            Some(self.path.clone()),
            false,
        ));
        let shutdown = context.shutdown();
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept Unix connection")?;
                    let session = context.clone();
                    sessions.spawn(async move { session.serve_native_stream(stream).await.context("Unix session") });
                }
            }
        }
        drop(listener);
        let result = drain_tasks(&mut sessions).await;
        if context.listener_registry().is_handed_off() {
            return result;
        }
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => result,
            Err(error) if error.kind() == io::ErrorKind::NotFound => result,
            Err(error) => Err(error).with_context(|| format!("remove Unix socket {:?}", self.path)),
        }
    }
}

#[must_use]
pub fn plugin() -> TransportRegistration {
    TransportRegistration::new("unix", Plugin)
}
