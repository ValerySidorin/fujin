//! Optional native Fujin v1 transport over TCP.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use fujin_core::BoxFuture;
use fujin_runtime::TlsSettings;
use fujin_tls::TlsConfig;
use fujin_transport::{
    CompiledTransport, Endpoint, TransportContext, TransportPlugin, TransportRegistration,
    listener::{bind_tcp, drain_tasks},
    settings::NativeProtocolSettings,
};
use fujin_upgrade::ListenerMetadata;
use serde::Deserialize;
use serde_json::Value;
use tokio::task::JoinSet;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    addr: String,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Debug)]
struct Plugin;

impl TransportPlugin for Plugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: Settings =
            serde_json::from_value(settings.clone()).context("parse tcp transport settings")?;
        settings.fujin.validate_supported()?;
        if settings.addr.is_empty() {
            bail!("TCP addr is required");
        }
        Ok(Arc::new(Transport {
            address: settings.addr,
            tls: settings.tls.listener_config("TCP")?,
        }))
    }
}

#[derive(Debug)]
struct Transport {
    address: String,
    tls: Option<TlsConfig>,
}

impl CompiledTransport for Transport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { self.run(context).await })
    }
}

impl Transport {
    async fn run(&self, context: TransportContext) -> Result<()> {
        let tls = match self.tls.as_ref() {
            Some(config) => Some(fujin_tls::load_acceptor(config, "TCP").await?),
            None => None,
        };
        let listener = bind_tcp(
            &self.address,
            ListenerMetadata::tcp(self.address.clone()),
            context.listener_registry(),
            context.inherited_listeners(),
        )
        .await?;
        context.signal_ready(Endpoint::native(
            "tcp",
            "tcp",
            listener
                .local_addr()
                .context("read TCP listener address")?
                .to_string(),
            None,
            self.tls.is_some(),
        ));
        let shutdown = context.shutdown();
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, peer) = accepted.context("accept TCP connection")?;
                    stream.set_nodelay(true).context("configure TCP_NODELAY")?;
                    let session = context.clone();
                    let tls = tls.clone();
                    sessions.spawn(async move {
                        if let Some(tls) = tls {
                            let stream = tls.accept(stream).await.context("accept TCP TLS")?;
                            session.serve_native_stream(stream).await.with_context(|| format!("TCP TLS session {peer}"))
                        } else {
                            session.serve_native_stream(stream).await.with_context(|| format!("TCP session {peer}"))
                        }
                    });
                }
            }
        }
        drain_tasks(&mut sessions).await
    }
}

#[must_use]
pub fn plugin() -> TransportRegistration {
    TransportRegistration::new("tcp", Plugin)
}
