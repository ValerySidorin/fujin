use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use fujin_transport::{
    Endpoint, TransportContext,
    listener::{bind_tcp, drain_tasks},
};
use fujin_upgrade::ListenerMetadata;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub(super) async fn serve(
    address: String,
    readiness: Arc<AtomicBool>,
    context: TransportContext,
) -> Result<()> {
    let shutdown = context.shutdown();
    let listener = bind_tcp(
        &address,
        ListenerMetadata::tcp(address.clone()),
        context.listener_registry(),
        context.inherited_listeners(),
    )
    .await?;
    context.signal_ready(Endpoint::health(
        listener
            .local_addr()
            .context("read health listener address")?
            .to_string(),
    ));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept health connection")?;
                let readiness = Arc::clone(&readiness);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    serve_connection(stream, readiness, connection_shutdown).await
                });
            }
        }
    }
    drain_tasks(&mut connections).await
}

async fn serve_connection(
    mut stream: tokio::net::TcpStream,
    readiness: Arc<AtomicBool>,
    shutdown: CancellationToken,
) -> Result<()> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut request = Vec::with_capacity(1024);
    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() == MAX_REQUEST_BYTES {
            stream.write_all(HTTP_TOO_LARGE).await?;
            return Ok(());
        }
        let mut buffer = [0_u8; 1024];
        let maximum = buffer.len().min(MAX_REQUEST_BYTES - request.len());
        let read = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            read = stream.read(&mut buffer[..maximum]) => read?,
        };
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let path = request
        .split(|byte| *byte == b' ')
        .nth(1)
        .unwrap_or_default();
    let response = response(path, readiness.load(Ordering::Acquire));
    stream.write_all(response).await?;
    stream.shutdown().await?;
    Ok(())
}

const HTTP_HEALTHY: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";
const HTTP_NOT_READY: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 10\r\nConnection: close\r\n\r\nnot ready\n";
const HTTP_NOT_FOUND: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 10\r\nConnection: close\r\n\r\nnot found\n";
const HTTP_TOO_LARGE: &[u8] = b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 18\r\nConnection: close\r\n\r\nrequest too large\n";

fn response(path: &[u8], ready: bool) -> &'static [u8] {
    match path {
        b"/healthz" => HTTP_HEALTHY,
        b"/readyz" if ready => HTTP_HEALTHY,
        b"/readyz" => HTTP_NOT_READY,
        _ => HTTP_NOT_FOUND,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_tracks_readiness() {
        assert!(response(b"/healthz", false).starts_with(b"HTTP/1.1 200"));
        assert!(response(b"/readyz", false).starts_with(b"HTTP/1.1 503"));
        assert!(response(b"/readyz", true).starts_with(b"HTTP/1.1 200"));
        assert!(response(b"/missing", true).starts_with(b"HTTP/1.1 404"));
    }
}
