use std::{fmt, sync::Arc};

#[cfg(unix)]
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::collections::BTreeMap;

/// Stable identity for one listener participating in graceful process handoff.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ListenerMetadata {
    #[serde(rename = "type")]
    pub listener_type: String,
    pub addr: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub grpc: bool,
}

impl ListenerMetadata {
    #[must_use]
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self {
            listener_type: "tcp".into(),
            addr: addr.into(),
            grpc: false,
        }
    }

    #[must_use]
    pub fn grpc(addr: impl Into<String>) -> Self {
        Self {
            listener_type: "tcp".into(),
            addr: addr.into(),
            grpc: true,
        }
    }

    #[must_use]
    pub fn udp(addr: impl Into<String>) -> Self {
        Self {
            listener_type: "udp".into(),
            addr: addr.into(),
            grpc: false,
        }
    }

    #[must_use]
    pub fn unix(addr: impl Into<String>) -> Self {
        Self {
            listener_type: "unix".into(),
            addr: addr.into(),
            grpc: false,
        }
    }

    #[must_use]
    pub fn key(&self) -> String {
        let suffix = if self.grpc { ":grpc" } else { "" };
        format!("{}:{}{suffix}", self.listener_type, self.addr)
    }
}

/// Listener descriptors inherited from the previous Fujin process.
#[derive(Clone, Default)]
pub struct InheritedListeners {
    #[cfg(unix)]
    entries: Arc<Mutex<BTreeMap<String, std::os::fd::OwnedFd>>>,
}

impl fmt::Debug for InheritedListeners {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InheritedListeners")
            .field("keys", &self.keys())
            .finish()
    }
}

impl InheritedListeners {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys().is_empty()
    }

    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        #[cfg(unix)]
        {
            return self.entries.lock().keys().cloned().collect();
        }
        #[cfg(not(unix))]
        Vec::new()
    }

    #[cfg(unix)]
    pub fn take(&self, metadata: &ListenerMetadata) -> Option<std::os::fd::OwnedFd> {
        self.entries.lock().remove(&metadata.key())
    }

    #[cfg(unix)]
    #[doc(hidden)]
    #[must_use]
    pub fn from_entries(entries: Vec<(ListenerMetadata, std::os::fd::OwnedFd)>) -> Self {
        Self {
            entries: Arc::new(Mutex::new(
                entries
                    .into_iter()
                    .map(|(metadata, fd)| (metadata.key(), fd))
                    .collect(),
            )),
        }
    }
}

/// Bound listeners available for a future graceful process handoff.
#[derive(Clone)]
pub struct ListenerRegistry {
    expected: usize,
    handed_off: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(unix)]
    entries: Arc<Mutex<BTreeMap<String, RegisteredListener>>>,
}

impl fmt::Debug for ListenerRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListenerRegistry")
            .field("expected", &self.expected)
            .field("keys", &self.keys())
            .field("handed_off", &self.is_handed_off())
            .finish_non_exhaustive()
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct RegisteredListener {
    metadata: ListenerMetadata,
    fd: std::os::fd::OwnedFd,
}

impl ListenerRegistry {
    #[must_use]
    pub fn new(expected: usize) -> Self {
        Self {
            expected,
            handed_off: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(unix)]
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[must_use]
    pub const fn expected(&self) -> usize {
        self.expected
    }

    #[must_use]
    pub fn is_handed_off(&self) -> bool {
        self.handed_off.load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(unix)]
    #[doc(hidden)]
    pub fn mark_handed_off(&self) {
        self.handed_off
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        #[cfg(unix)]
        {
            return self.entries.lock().keys().cloned().collect();
        }
        #[cfg(not(unix))]
        Vec::new()
    }

    /// Registers one bound listener for a future descriptor handoff.
    ///
    /// # Errors
    ///
    /// Returns [`ListenerRegistryError::DuplicateListener`] when the descriptor key is already
    /// present.
    #[cfg(unix)]
    pub fn register(
        &self,
        metadata: ListenerMetadata,
        fd: std::os::fd::OwnedFd,
    ) -> Result<(), ListenerRegistryError> {
        let key = metadata.key();
        let mut entries = self.entries.lock();
        if entries.contains_key(&key) {
            return Err(ListenerRegistryError::DuplicateListener(key));
        }
        entries.insert(key, RegisteredListener { metadata, fd });
        Ok(())
    }

    #[cfg(unix)]
    #[doc(hidden)]
    pub fn snapshot(
        &self,
    ) -> Result<Vec<(ListenerMetadata, std::os::fd::OwnedFd)>, ListenerRegistryError> {
        use std::os::fd::AsFd;

        let entries = self.entries.lock();
        if entries.len() != self.expected {
            return Err(ListenerRegistryError::ListenersNotReady {
                expected: self.expected,
                registered: entries.len(),
            });
        }
        entries
            .values()
            .map(|entry| {
                Ok((
                    entry.metadata.clone(),
                    entry.fd.as_fd().try_clone_to_owned()?,
                ))
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ListenerRegistryError {
    #[error("listener {0:?} is already registered")]
    DuplicateListener(String),
    #[error("listeners are not ready: registered {registered}/{expected}")]
    ListenersNotReady { expected: usize, registered: usize },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::ListenerMetadata;

    #[test]
    fn listener_metadata_keys_match_upgrade_contract() {
        assert_eq!(ListenerMetadata::tcp(":4850").key(), "tcp::4850");
        assert_eq!(ListenerMetadata::udp(":4848").key(), "udp::4848");
        assert_eq!(ListenerMetadata::grpc(":4849").key(), "tcp::4849:grpc");
        assert_eq!(
            ListenerMetadata::unix("/tmp/fujin.sock").key(),
            "unix:/tmp/fujin.sock"
        );
    }
}
