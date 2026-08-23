//! Optional native Fujin v1 transport over binary WebSocket messages.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll, ready},
};

use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes, BytesMut};
use fujin_transport::{
    BoxFuture, CompiledTransport, Endpoint, ListenerMetadata, TransportContext, TransportPlugin,
    TransportRegistration,
    listener::{bind_tcp, drain_tasks},
    settings::{NativeProtocolSettings, TlsSettings},
    tls::TlsConfig,
};
use futures_util::{Sink, Stream};
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    task::JoinSet,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

fn default_path() -> String {
    "/fujin".into()
}
fn default_max_message_bytes() -> usize {
    4 * 1024 * 1024
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    addr: String,
    #[serde(default = "default_path")]
    path: String,
    #[serde(default)]
    allowed_origins: Vec<String>,
    #[serde(default = "default_max_message_bytes")]
    max_message_bytes: usize,
    #[serde(default)]
    tls: TlsSettings,
    #[serde(default)]
    fujin: NativeProtocolSettings,
}

#[derive(Debug)]
struct Plugin;

impl TransportPlugin for Plugin {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledTransport>> {
        let settings: Settings = serde_json::from_value(settings.clone())
            .context("parse websocket transport settings")?;
        settings.fujin.validate_supported()?;
        if settings.addr.is_empty() {
            bail!("WebSocket addr is required");
        }
        if !settings.path.starts_with('/') {
            bail!("WebSocket path must start with '/'");
        }
        if settings.max_message_bytes == 0 {
            bail!("WebSocket max_message_bytes must be positive");
        }
        Ok(Arc::new(Transport {
            address: settings.addr,
            path: settings.path,
            allowed_origins: settings.allowed_origins,
            max_message_bytes: settings.max_message_bytes,
            tls: settings.tls.listener_config("WebSocket")?,
        }))
    }
}

#[derive(Debug)]
struct Transport {
    address: String,
    path: String,
    allowed_origins: Vec<String>,
    max_message_bytes: usize,
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
            Some(config) => Some(fujin_transport::tls::load_acceptor(config, "WebSocket").await?),
            None => None,
        };
        let listener = bind_tcp(
            &self.address,
            ListenerMetadata::tcp(self.address.clone()),
            context.listener_registry(),
            context.inherited_listeners(),
        )
        .await?;
        let policy = WebSocketPolicy {
            path: self.path.clone().into(),
            allowed_origins: self.allowed_origins.clone().into(),
            max_message_bytes: self.max_message_bytes,
        };
        context.signal_ready(Endpoint::native(
            "websocket",
            "tcp",
            listener
                .local_addr()
                .context("read WebSocket listener address")?
                .to_string(),
            Some(self.path.clone()),
            self.tls.is_some(),
        ));
        let shutdown = context.shutdown();
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, peer) = accepted.context("accept WebSocket connection")?;
                    let session = context.clone();
                    let tls = tls.clone();
                    let policy = policy.clone();
                    sessions.spawn(async move {
                        if let Some(tls) = tls {
                            let stream = tls.accept(stream).await.context("accept WebSocket TLS")?;
                            websocket_session(stream, session, policy).await
                        } else {
                            websocket_session(stream, session, policy).await
                        }.with_context(|| format!("WebSocket session {peer}"))
                    });
                }
            }
        }
        drain_tasks(&mut sessions).await
    }
}

#[derive(Clone)]
struct WebSocketPolicy {
    path: Arc<str>,
    allowed_origins: Arc<[String]>,
    max_message_bytes: usize,
}

#[derive(Debug)]
pub struct NativeWebSocketStream<S> {
    websocket: WebSocketStream<S>,
    pending: Bytes,
    input_closed: bool,
    pending_write: bool,
}

impl<S> NativeWebSocketStream<S> {
    pub fn new(websocket: WebSocketStream<S>) -> Self {
        Self {
            websocket,
            pending: Bytes::new(),
            input_closed: false,
            pending_write: false,
        }
    }

    fn websocket_error(error: tokio_tungstenite::tungstenite::Error) -> io::Error {
        io::Error::other(error)
    }
}

impl<S> AsyncRead for NativeWebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pending_write {
            match Pin::new(&mut self.websocket).poll_flush(context) {
                Poll::Ready(Ok(())) => self.pending_write = false,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(Self::websocket_error(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
        loop {
            if !self.pending.is_empty() {
                let read = self.pending.len().min(buffer.remaining());
                buffer.put_slice(&self.pending[..read]);
                self.pending.advance(read);
                return Poll::Ready(Ok(()));
            }
            if self.input_closed {
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut self.websocket).poll_next(context)) {
                Some(Ok(Message::Binary(bytes))) => self.pending = bytes,
                Some(Ok(Message::Close(_))) | None => self.input_closed = true,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                    self.input_closed = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "only binary WebSocket messages are valid",
                    )));
                }
                Some(Err(error)) => return Poll::Ready(Err(Self::websocket_error(error))),
            }
        }
    }
}

impl<S> AsyncWrite for NativeWebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.pending_write {
            ready!(Pin::new(&mut self.websocket).poll_flush(context))
                .map_err(Self::websocket_error)?;
            self.pending_write = false;
        }
        ready!(Pin::new(&mut self.websocket).poll_ready(context)).map_err(Self::websocket_error)?;
        Pin::new(&mut self.websocket)
            .start_send(Message::Binary(Bytes::copy_from_slice(buffer)))
            .map_err(Self::websocket_error)?;
        self.pending_write = true;
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let length = buffers.iter().try_fold(0_usize, |length, buffer| {
            length.checked_add(buffer.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "WebSocket write overflow")
            })
        })?;
        if length == 0 {
            return Poll::Ready(Ok(0));
        }
        if self.pending_write {
            ready!(Pin::new(&mut self.websocket).poll_flush(context))
                .map_err(Self::websocket_error)?;
            self.pending_write = false;
        }
        ready!(Pin::new(&mut self.websocket).poll_ready(context)).map_err(Self::websocket_error)?;
        let mut message = BytesMut::with_capacity(length);
        for buffer in buffers {
            message.extend_from_slice(buffer);
        }
        Pin::new(&mut self.websocket)
            .start_send(Message::Binary(message.freeze()))
            .map_err(Self::websocket_error)?;
        self.pending_write = true;
        Poll::Ready(Ok(length))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.websocket).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                self.pending_write = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(Self::websocket_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(context))?;
        Pin::new(&mut self.websocket)
            .poll_close(context)
            .map_err(Self::websocket_error)
    }
}

#[allow(clippy::result_large_err)]
async fn websocket_session<S>(
    stream: S,
    context: TransportContext,
    policy: WebSocketPolicy,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use tokio_tungstenite::tungstenite::{
        handshake::server::{Request, Response},
        http::StatusCode,
        protocol::WebSocketConfig,
    };
    let config = WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(8 * 1024)
        .max_write_buffer_size(policy.max_message_bytes)
        .max_message_size(Some(policy.max_message_bytes))
        .max_frame_size(Some(policy.max_message_bytes));
    let callback = move |request: &Request, response: Response| {
        if request.uri().path() != policy.path.as_ref() {
            return Err(websocket_rejection(
                StatusCode::NOT_FOUND,
                "WebSocket path not found",
            ));
        }
        if !websocket_origin_allowed(request, &policy.allowed_origins) {
            return Err(websocket_rejection(
                StatusCode::FORBIDDEN,
                "WebSocket origin denied",
            ));
        }
        Ok(response)
    };
    let websocket = tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(config))
        .await
        .context("upgrade WebSocket")?;
    context
        .serve_native_stream(NativeWebSocketStream::new(websocket))
        .await
}

fn websocket_rejection(
    status: tokio_tungstenite::tungstenite::http::StatusCode,
    message: &str,
) -> tokio_tungstenite::tungstenite::handshake::server::ErrorResponse {
    let mut response = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some(
        message.to_owned(),
    ));
    *response.status_mut() = status;
    response
}

fn websocket_origin_allowed(
    request: &tokio_tungstenite::tungstenite::handshake::server::Request,
    allowed: &[String],
) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let Some(origin) = request
        .headers()
        .get("origin")
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    if allowed.iter().any(|value| value == "*") {
        return true;
    }
    let Ok(parsed) = origin.parse::<tokio_tungstenite::tungstenite::http::Uri>() else {
        return false;
    };
    if parsed.scheme().is_none() || parsed.authority().is_none() {
        return false;
    }
    let origin = origin.trim_end_matches('/');
    allowed
        .iter()
        .any(|value| value.trim_end_matches('/') == origin)
}

#[must_use]
pub fn plugin() -> TransportRegistration {
    TransportRegistration::new("websocket", Plugin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_policy_allows_configured_origin_and_rejects_others() {
        let allowed = ["https://console.example".to_owned()];
        let allowed_request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .header("origin", "https://console.example/")
            .body(())
            .expect("allowed request");
        let denied_request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .header("origin", "https://attacker.example")
            .body(())
            .expect("denied request");
        assert!(websocket_origin_allowed(&allowed_request, &allowed));
        assert!(!websocket_origin_allowed(&denied_request, &allowed));
    }

    #[test]
    fn origin_policy_allows_non_browser_clients_without_origin() {
        let request = tokio_tungstenite::tungstenite::handshake::server::Request::builder()
            .uri("/fujin")
            .body(())
            .expect("request without origin");
        assert!(websocket_origin_allowed(
            &request,
            &["https://console.example".to_owned()]
        ));
    }
}
