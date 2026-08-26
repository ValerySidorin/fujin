//! Optional native Fujin v1 transport over TCP.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use fujin_transport::{
    BoxFuture, CompiledTransport, Endpoint, ListenerMetadata, TransportContext, TransportPlugin,
    TransportRegistration,
    listener::{bind_tcp, drain_tasks},
    settings::{NativeProtocolSettings, TlsSettings},
    tls::TlsConfig,
};
use serde::Deserialize;
use serde_json::Value;
use socket2::{SockRef, TcpKeepalive};
use tokio::task::JoinSet;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    addr: String,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
    #[serde(default)]
    tcp_keepalive: TcpKeepaliveSettings,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TcpKeepaliveSettings {
    #[serde(default)]
    time: Option<String>,
    #[serde(default)]
    interval: Option<String>,
    #[serde(default)]
    retries: Option<u32>,
}

impl TcpKeepaliveSettings {
    fn compile(&self) -> Result<Option<TcpKeepalive>> {
        if self.time.is_none() && self.interval.is_none() && self.retries.is_none() {
            return Ok(None);
        }
        let mut config = TcpKeepalive::new();
        if let Some(value) = self.time.as_deref() {
            config = config.with_time(parse_duration("TCP tcp_keepalive.time", value)?);
        }
        if let Some(value) = self.interval.as_deref() {
            config = config.with_interval(parse_duration("TCP tcp_keepalive.interval", value)?);
        }
        if let Some(retries) = self.retries {
            if retries == 0 {
                bail!("TCP tcp_keepalive.retries must be positive");
            }
            config = config.with_retries(retries);
        }
        Ok(Some(config))
    }
}

fn parse_duration(name: &str, value: &str) -> Result<Duration> {
    humantime::parse_duration(value).with_context(|| format!("parse {name}"))
}

#[derive(Debug)]
struct Plugin;

impl TransportPlugin for Plugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: Settings =
            serde_json::from_value(settings.clone()).context("parse tcp transport settings")?;
        let native = settings.fujin.compile()?;
        if settings.addr.is_empty() {
            bail!("TCP addr is required");
        }
        Ok(Arc::new(Transport {
            address: settings.addr,
            tls: settings.tls.listener_config("TCP")?,
            native,
            tcp_keepalive: settings.tcp_keepalive.compile()?,
        }))
    }
}

#[derive(Debug)]
struct Transport {
    address: String,
    tls: Option<TlsConfig>,
    native: fujin_transport::settings::NativeProtocolConfig,
    tcp_keepalive: Option<TcpKeepalive>,
}

impl CompiledTransport for Transport {
    fn serve(self: Arc<Self>, context: TransportContext) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { self.run(context).await })
    }
}

impl Transport {
    async fn run(&self, context: TransportContext) -> Result<()> {
        let tls = match self.tls.as_ref() {
            Some(config) => Some(fujin_transport::tls::load_acceptor(config, "TCP").await?),
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
                    if let Some(keepalive) = self.tcp_keepalive.as_ref() {
                        SockRef::from(&stream)
                            .set_tcp_keepalive(keepalive)
                            .context("configure TCP keepalive")?;
                    }
                    let session = context.clone();
                    let native = self.native.session_config(true);
                    let tls = tls.clone();
                    sessions.spawn(async move {
                        if let Some(tls) = tls {
                            let stream = tls.accept(stream).await.context("accept TCP TLS")?;
                            session.serve_native_stream_with_config(stream, native).await.with_context(|| format!("TCP TLS session {peer}"))
                        } else {
                            session.serve_native_stream_with_config(stream, native).await.with_context(|| format!("TCP session {peer}"))
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
