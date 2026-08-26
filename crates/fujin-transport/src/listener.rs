#[cfg(unix)]
use std::os::fd::AsFd;

use crate::{InheritedListeners, ListenerMetadata, ListenerRegistry};
use anyhow::{Context, Result};
use tokio::task::JoinSet;

/// Binds or inherits a TCP listener and registers it for graceful handoff.
///
/// # Errors
///
/// Returns bind, inheritance, descriptor cloning, or registry errors.
pub async fn bind_tcp(
    address: &str,
    metadata: ListenerMetadata,
    registry: &ListenerRegistry,
    inherited: &InheritedListeners,
) -> Result<tokio::net::TcpListener> {
    #[cfg(unix)]
    let listener = if let Some(fd) = inherited.take(&metadata) {
        let listener = std::net::TcpListener::from(fd);
        listener
            .set_nonblocking(true)
            .context("configure inherited TCP listener")?;
        tokio::net::TcpListener::from_std(listener).context("inherit TCP listener")?
    } else {
        tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind TCP listener {address:?}"))?
    };
    #[cfg(not(unix))]
    let listener = {
        let _ = (metadata, registry, inherited);
        tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind TCP listener {address:?}"))?
    };
    #[cfg(unix)]
    registry.register(
        metadata,
        listener
            .as_fd()
            .try_clone_to_owned()
            .context("clone TCP listener descriptor")?,
    )?;
    Ok(listener)
}

#[cfg(unix)]
/// Binds or inherits a Unix listener and registers it for graceful handoff.
///
/// # Errors
///
/// Returns bind, inheritance, descriptor cloning, or registry errors.
pub fn bind_unix(
    path: &str,
    metadata: ListenerMetadata,
    registry: &ListenerRegistry,
    inherited: &InheritedListeners,
) -> Result<tokio::net::UnixListener> {
    let listener = if let Some(fd) = inherited.take(&metadata) {
        let listener = std::os::unix::net::UnixListener::from(fd);
        listener
            .set_nonblocking(true)
            .context("configure inherited Unix listener")?;
        tokio::net::UnixListener::from_std(listener).context("inherit Unix listener")?
    } else {
        tokio::net::UnixListener::bind(path)
            .with_context(|| format!("bind Unix listener {path:?}"))?
    };
    registry.register(
        metadata,
        listener
            .as_fd()
            .try_clone_to_owned()
            .context("clone Unix listener descriptor")?,
    )?;
    Ok(listener)
}

/// Binds or inherits a UDP socket and registers it for graceful handoff.
///
/// # Errors
///
/// Returns bind, inheritance, descriptor cloning, or registry errors.
pub fn bind_udp(
    address: std::net::SocketAddr,
    metadata: ListenerMetadata,
    registry: &ListenerRegistry,
    inherited: &InheritedListeners,
) -> Result<std::net::UdpSocket> {
    #[cfg(unix)]
    let socket = if let Some(fd) = inherited.take(&metadata) {
        std::net::UdpSocket::from(fd)
    } else {
        std::net::UdpSocket::bind(address).context("bind UDP socket")?
    };
    #[cfg(not(unix))]
    let socket = {
        let _ = (metadata, registry, inherited);
        std::net::UdpSocket::bind(address).context("bind UDP socket")?
    };
    socket
        .set_nonblocking(true)
        .context("configure UDP socket nonblocking")?;
    #[cfg(unix)]
    registry.register(
        metadata,
        socket
            .as_fd()
            .try_clone_to_owned()
            .context("clone UDP socket descriptor")?,
    )?;
    Ok(socket)
}

/// Drains connection tasks after listener shutdown.
///
/// # Errors
///
/// Returns a task panic or terminal connection error.
pub async fn drain_tasks(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "connection ended with error"),
            Err(error) => return Err(error).context("connection task failed"),
        }
    }
    Ok(())
}
