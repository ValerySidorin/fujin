#![cfg_attr(unix, allow(unsafe_code))]

//! Unix listener file-descriptor handoff for graceful Fujin binary upgrades.

use std::path::Path;

#[cfg(unix)]
use fujin_transport::ListenerMetadata;
use fujin_transport::{InheritedListeners, ListenerRegistry, ListenerRegistryError};
#[cfg(unix)]
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const UPGRADE_ENV: &str = "FUJIN_UPGRADE";
pub const UPGRADE_SOCKET_ENV: &str = "FUJIN_UPGRADE_SOCK";
pub const DEFAULT_UPGRADE_SOCKET: &str = "/run/fujin/upgrade.sock";

#[cfg(unix)]
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(unix)]
const STARTUP_TIMEOUT: Duration = Duration::from_mins(1);
#[cfg(unix)]
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
#[cfg(unix)]
const MAX_LISTENER_FDS: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    #[error("upgrade unavailable on this platform")]
    Unsupported,
    #[error(transparent)]
    Listener(#[from] ListenerRegistryError),
    #[error("upgrade protocol: {0}")]
    Protocol(String),
    #[error("upgrade operation timed out")]
    Timeout,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[cfg(unix)]
    #[error(transparent)]
    Nix(#[from] nix::Error),
    #[error("upgrade task failed: {0}")]
    Task(String),
}

#[derive(Debug)]
pub struct UpgradeClient {
    inherited: InheritedListeners,
    #[cfg(unix)]
    connection: Option<std::os::unix::net::UnixStream>,
}

impl UpgradeClient {
    #[must_use]
    pub fn inherited(&self) -> InheritedListeners {
        self.inherited.clone()
    }

    /// Signals listener readiness and waits until the old process acknowledges drain.
    ///
    /// # Errors
    ///
    /// Returns a protocol, timeout, task, or I/O error.
    pub async fn signal_ready(self) -> Result<(), UpgradeError> {
        #[cfg(unix)]
        {
            let connection = self.connection.ok_or_else(|| {
                UpgradeError::Protocol("upgrade control connection is missing".into())
            })?;
            tokio::task::spawn_blocking(move || signal_ready_blocking(&connection))
                .await
                .map_err(|error| UpgradeError::Task(error.to_string()))?
        }
        #[cfg(not(unix))]
        {
            let _ = self;
            Err(UpgradeError::Unsupported)
        }
    }
}

#[must_use]
pub fn socket_path_from_environment() -> String {
    std::env::var(UPGRADE_SOCKET_ENV).unwrap_or_else(|_| DEFAULT_UPGRADE_SOCKET.into())
}

/// Requests listener descriptors from the old process when `FUJIN_UPGRADE=1`.
///
/// # Errors
///
/// Returns a protocol, timeout, task, or I/O error when upgrade mode is requested but handoff
/// cannot be completed.
pub async fn request_from_environment() -> Result<Option<UpgradeClient>, UpgradeError> {
    if std::env::var(UPGRADE_ENV).as_deref() != Ok("1") {
        return Ok(None);
    }
    request_upgrade(socket_path_from_environment())
        .await
        .map(Some)
}

/// Requests listener descriptors from a running Fujin process.
///
/// # Errors
///
/// Returns a protocol, timeout, task, or I/O error.
pub async fn request_upgrade(path: impl AsRef<Path>) -> Result<UpgradeClient, UpgradeError> {
    #[cfg(unix)]
    {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || request_upgrade_blocking(&path))
            .await
            .map_err(|error| UpgradeError::Task(error.to_string()))?
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(UpgradeError::Unsupported)
    }
}

/// Serves one successful listener handoff, then requests process drain.
///
/// Invalid or premature requests are rejected while the control listener remains available.
///
/// # Errors
///
/// Returns a bind, accept, protocol, task, timeout, or I/O error.
pub async fn listen_for_upgrade(
    path: impl AsRef<Path>,
    registry: ListenerRegistry,
    shutdown: CancellationToken,
    drain: CancellationToken,
) -> Result<(), UpgradeError> {
    #[cfg(unix)]
    {
        return unix::listen_for_upgrade(path.as_ref(), registry, shutdown, drain).await;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, registry, shutdown, drain);
        Ok(())
    }
}

/// Waits until the old process unlinks its upgrade control socket after acknowledging readiness.
///
/// # Errors
///
/// Returns a timeout error when the old process does not release the path.
pub async fn wait_for_socket_release(path: impl AsRef<Path>) -> Result<(), UpgradeError> {
    #[cfg(unix)]
    {
        let path = path.as_ref();
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            while tokio::fs::try_exists(path).await? {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| UpgradeError::Timeout)??;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(UpgradeError::Unsupported)
    }
}

#[cfg(unix)]
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    cmd: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fds: Vec<ListenerMetadata>,
}

#[cfg(unix)]
const CMD_REQUEST_FDS: &str = "request_fds";
#[cfg(unix)]
const CMD_FD_RESPONSE: &str = "fds_response";
#[cfg(unix)]
const CMD_READY: &str = "ready";
#[cfg(unix)]
const CMD_DRAIN_ACK: &str = "drain_ack";

#[cfg(unix)]
fn request_upgrade_blocking(path: &Path) -> Result<UpgradeClient, UpgradeError> {
    let connection = std::os::unix::net::UnixStream::connect(path)?;
    connection.set_read_timeout(Some(CONTROL_TIMEOUT))?;
    connection.set_write_timeout(Some(CONTROL_TIMEOUT))?;
    send_message(
        &connection,
        &Message {
            cmd: CMD_REQUEST_FDS.into(),
            fds: Vec::new(),
        },
    )?;
    let entries = recv_fd_response(&connection, MAX_LISTENER_FDS)?;
    Ok(UpgradeClient {
        inherited: InheritedListeners::from_entries(entries),
        connection: Some(connection),
    })
}

#[cfg(unix)]
fn signal_ready_blocking(connection: &std::os::unix::net::UnixStream) -> Result<(), UpgradeError> {
    send_message(
        connection,
        &Message {
            cmd: CMD_READY.into(),
            fds: Vec::new(),
        },
    )?;
    let response = recv_message(connection)?;
    if response.cmd != CMD_DRAIN_ACK {
        return Err(UpgradeError::Protocol(format!(
            "unexpected command {:?}, expected {CMD_DRAIN_ACK:?}",
            response.cmd
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn send_message(
    mut connection: &std::os::unix::net::UnixStream,
    message: &Message,
) -> Result<(), UpgradeError> {
    use std::io::Write;

    connection.write_all(&serde_json::to_vec(message)?)?;
    Ok(())
}

#[cfg(unix)]
fn recv_message(mut connection: &std::os::unix::net::UnixStream) -> Result<Message, UpgradeError> {
    use std::io::Read;

    let mut buffer = vec![0; MAX_MESSAGE_BYTES];
    let size = connection.read(&mut buffer)?;
    if size == 0 {
        return Err(UpgradeError::Protocol(
            "control connection closed while reading message".into(),
        ));
    }
    Ok(serde_json::from_slice(&buffer[..size])?)
}

#[cfg(unix)]
fn send_fd_response(
    connection: &std::os::unix::net::UnixStream,
    entries: &[(ListenerMetadata, std::os::fd::OwnedFd)],
) -> Result<(), UpgradeError> {
    use std::{io::IoSlice, os::fd::AsRawFd};

    use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};

    if entries.is_empty() {
        return Err(UpgradeError::Protocol(
            "no listener file descriptors to send".into(),
        ));
    }
    let metadata = serde_json::to_vec(&Message {
        cmd: CMD_FD_RESPONSE.into(),
        fds: entries
            .iter()
            .map(|(metadata, _)| metadata.clone())
            .collect(),
    })?;
    let length = u32::try_from(metadata.len())
        .map_err(|_| UpgradeError::Protocol("listener metadata is too large".into()))?;
    let mut payload = Vec::with_capacity(4 + metadata.len());
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(&metadata);
    let fds = entries
        .iter()
        .map(|(_, fd)| fd.as_raw_fd())
        .collect::<Vec<_>>();
    let buffers = [IoSlice::new(&payload)];
    let control = [ControlMessage::ScmRights(&fds)];
    sendmsg::<()>(
        connection.as_raw_fd(),
        &buffers,
        &control,
        MsgFlags::empty(),
        None,
    )?;
    Ok(())
}

#[cfg(unix)]
fn recv_fd_response(
    connection: &std::os::unix::net::UnixStream,
    maximum: usize,
) -> Result<Vec<(ListenerMetadata, std::os::fd::OwnedFd)>, UpgradeError> {
    use std::{
        io::IoSliceMut,
        os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    };

    use nix::{
        cmsg_space,
        sys::socket::{ControlMessageOwned, MsgFlags, recvmsg},
    };

    let mut payload = vec![0; MAX_MESSAGE_BYTES];
    let mut buffers = [IoSliceMut::new(&mut payload)];
    let mut control = cmsg_space!([RawFd; MAX_LISTENER_FDS]);
    let message = recvmsg::<()>(
        connection.as_raw_fd(),
        &mut buffers,
        Some(&mut control),
        MsgFlags::empty(),
    )?;
    let size = message.bytes;
    let mut received = Vec::new();
    for control in message.cmsgs()? {
        if let ControlMessageOwned::ScmRights(fds) = control {
            for fd in fds {
                // SAFETY: SCM_RIGHTS transfers one new descriptor owned by the receiving process.
                received.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        }
    }
    if received.len() > maximum {
        return Err(UpgradeError::Protocol(format!(
            "received {} listener descriptors, maximum is {maximum}",
            received.len()
        )));
    }
    if size < 4 {
        return Err(UpgradeError::Protocol(format!(
            "listener metadata is too short: {size} bytes"
        )));
    }
    let metadata_size = usize::try_from(u32::from_be_bytes(
        payload[..4]
            .try_into()
            .expect("metadata length prefix has four bytes"),
    ))
    .expect("u32 fits usize on supported Unix targets");
    if metadata_size > size - 4 {
        return Err(UpgradeError::Protocol(format!(
            "listener metadata length mismatch: declared {metadata_size}, available {}",
            size - 4
        )));
    }
    let response: Message = serde_json::from_slice(&payload[4..4 + metadata_size])?;
    if response.cmd != CMD_FD_RESPONSE {
        return Err(UpgradeError::Protocol(format!(
            "unexpected command {:?}, expected {CMD_FD_RESPONSE:?}",
            response.cmd
        )));
    }
    if response.fds.len() != received.len() {
        return Err(UpgradeError::Protocol(format!(
            "listener descriptor count mismatch: {} descriptors, {} metadata entries",
            received.len(),
            response.fds.len()
        )));
    }
    Ok(response.fds.into_iter().zip(received).collect())
}

#[cfg(unix)]
mod unix {
    use std::{fs, os::unix::fs::DirBuilderExt, path::Path};

    use tokio::net::UnixListener;
    use tokio_util::sync::CancellationToken;

    use super::{
        CMD_DRAIN_ACK, CMD_READY, CMD_REQUEST_FDS, CONTROL_TIMEOUT, ListenerRegistry, Message,
        STARTUP_TIMEOUT, UpgradeError, recv_message, send_fd_response, send_message,
    };

    pub async fn listen_for_upgrade(
        path: &Path,
        registry: ListenerRegistry,
        shutdown: CancellationToken,
        drain: CancellationToken,
    ) -> Result<(), UpgradeError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
        }
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(path)?;
        tracing::info!(path = %path.display(), "upgrade socket listening");
        let result = loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => break Ok(()),
                accepted = listener.accept() => accepted,
            };
            let (connection, _) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    if shutdown.is_cancelled() {
                        break Ok(());
                    }
                    tracing::error!(%error, "accept upgrade connection");
                    continue;
                }
            };
            let entries = match registry.snapshot() {
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(%error, "reject premature upgrade request");
                    continue;
                }
            };
            let standard = connection.into_std()?;
            standard.set_nonblocking(false)?;
            let drain_for_request = drain.clone();
            let registry_for_request = registry.clone();
            let handled = tokio::task::spawn_blocking(move || {
                handle_request(
                    &standard,
                    &entries,
                    &registry_for_request,
                    &drain_for_request,
                )
            })
            .await
            .map_err(|error| UpgradeError::Task(error.to_string()))?;
            match handled {
                Ok(()) => break Ok(()),
                Err(error) => tracing::error!(%error, "handle upgrade request"),
            }
        };
        drop(listener);
        match fs::remove_file(path) {
            Ok(()) => result,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => result,
            Err(error) => Err(error.into()),
        }
    }

    fn handle_request(
        connection: &std::os::unix::net::UnixStream,
        entries: &[(super::ListenerMetadata, std::os::fd::OwnedFd)],
        registry: &ListenerRegistry,
        drain: &CancellationToken,
    ) -> Result<(), UpgradeError> {
        connection.set_read_timeout(Some(CONTROL_TIMEOUT))?;
        connection.set_write_timeout(Some(CONTROL_TIMEOUT))?;
        let request = recv_message(connection)?;
        if request.cmd != CMD_REQUEST_FDS {
            return Err(UpgradeError::Protocol(format!(
                "unexpected command {:?}, expected {CMD_REQUEST_FDS:?}",
                request.cmd
            )));
        }
        send_fd_response(connection, entries)?;
        connection.set_read_timeout(Some(STARTUP_TIMEOUT))?;
        let ready = recv_message(connection)?;
        if ready.cmd != CMD_READY {
            return Err(UpgradeError::Protocol(format!(
                "unexpected command {:?}, expected {CMD_READY:?}",
                ready.cmd
            )));
        }
        registry.mark_handed_off();
        send_message(
            connection,
            &Message {
                cmd: CMD_DRAIN_ACK.into(),
                fds: Vec::new(),
            },
        )?;
        drain.cancel();
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        net::TcpListener,
        os::fd::{AsFd, AsRawFd},
        time::Duration,
    };

    use tokio_util::sync::CancellationToken;

    use super::*;

    #[test]
    fn listener_metadata_keys_match_go_contract() {
        assert_eq!(ListenerMetadata::tcp(":4850").key(), "tcp::4850");
        assert_eq!(ListenerMetadata::udp(":4848").key(), "udp::4848");
        assert_eq!(ListenerMetadata::grpc(":4849").key(), "tcp::4849:grpc");
        assert_eq!(
            ListenerMetadata::unix("/tmp/fujin.sock").key(),
            "unix:/tmp/fujin.sock"
        );
    }

    #[test]
    fn fd_response_round_trip_preserves_metadata_and_descriptor() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (sender, receiver) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let metadata = ListenerMetadata::tcp(address.to_string());
        let entries = vec![(
            metadata.clone(),
            listener
                .as_fd()
                .try_clone_to_owned()
                .expect("clone listener fd"),
        )];
        send_fd_response(&sender, &entries).expect("send listener fd");
        let transferred = recv_fd_response(&receiver, 1).expect("receive listener fd");
        assert_eq!(transferred.len(), 1);
        assert_eq!(transferred[0].0, metadata);
        assert_ne!(transferred[0].1.as_raw_fd(), listener.as_raw_fd());
    }

    #[tokio::test]
    async fn complete_upgrade_handshake_requests_drain() {
        let path = std::path::PathBuf::from(format!(
            "/tmp/fujin-upgrade-test-{}.sock",
            std::process::id()
        ));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let metadata =
            ListenerMetadata::tcp(listener.local_addr().expect("listener address").to_string());
        let registry = ListenerRegistry::new(1);
        registry
            .register(
                metadata.clone(),
                listener
                    .as_fd()
                    .try_clone_to_owned()
                    .expect("clone listener fd"),
            )
            .expect("register listener");
        let observed_registry = registry.clone();
        let shutdown = CancellationToken::new();
        let drain = CancellationToken::new();
        let server_path = path.clone();
        let server_shutdown = shutdown.clone();
        let server_drain = drain.clone();
        let server = tokio::spawn(async move {
            listen_for_upgrade(server_path, registry, server_shutdown, server_drain).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("upgrade socket ready");

        let client = request_upgrade(&path).await.expect("request upgrade");
        assert_eq!(client.inherited().keys(), [metadata.key()]);
        client.signal_ready().await.expect("signal readiness");
        assert!(observed_registry.is_handed_off());
        tokio::time::timeout(Duration::from_secs(1), drain.cancelled())
            .await
            .expect("old process drain requested");
        server
            .await
            .expect("join upgrade server")
            .expect("upgrade server result");
        shutdown.cancel();
        let _ = std::fs::remove_file(path);
    }
}
