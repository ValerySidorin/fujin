use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use fujin_error::{CoreError, Result};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AcceptanceGuarantee {
    #[default]
    Unspecified = 0,
    Local = 1,
    Peer = 2,
    Durable = 3,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AckGranularity {
    #[default]
    Unsupported = 0,
    Single = 1,
    Cumulative = 2,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NackEffect {
    #[default]
    Unsupported = 0,
    Requeue = 1,
    Release = 2,
    Drop = 3,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettlementProfile {
    pub ack: AckGranularity,
    pub nack: NackEffect,
}

/// Native-v1-compatible capability bitset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capabilities(u8);

impl Capabilities {
    pub const PRODUCE: Self = Self(0x01);
    pub const HEADERS: Self = Self(0x02);
    pub const TRANSACTIONS: Self = Self(0x04);
    pub const SUBSCRIBE: Self = Self(0x08);
    pub const FETCH: Self = Self(0x10);
    pub const MANUAL_SETTLEMENT: Self = Self(0x20);

    pub const fn from_bits(bits: u8) -> Self {
        Self(bits & 0x3f)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn contains(self, capability: Self) -> bool {
        self.0 & capability.0 == capability.0
    }

    #[must_use]
    pub const fn union(self, capability: Self) -> Self {
        Self(self.0 | capability.0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteProfile {
    pub capabilities: Capabilities,
    pub produce_guarantee: AcceptanceGuarantee,
    pub settlement: SettlementProfile,
}

impl RouteProfile {
    /// Validates that the advertised operations and guarantees are internally consistent.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] for an empty route or contradictory profile.
    pub fn validate(self, route: &str) -> Result<()> {
        if route.is_empty() {
            return Err(CoreError::InvalidConfig("route name is empty".into()));
        }
        let produce = self.capabilities.contains(Capabilities::PRODUCE);
        let headers = self.capabilities.contains(Capabilities::HEADERS);
        let transactions = self.capabilities.contains(Capabilities::TRANSACTIONS);
        let subscribe = self.capabilities.contains(Capabilities::SUBSCRIBE);
        let fetch = self.capabilities.contains(Capabilities::FETCH);
        let manual_settlement = self.capabilities.contains(Capabilities::MANUAL_SETTLEMENT);
        if produce && self.produce_guarantee == AcceptanceGuarantee::Unspecified {
            return Err(CoreError::InvalidConfig(format!(
                "route {route:?}: produce guarantee is required"
            )));
        }
        if !produce && self.produce_guarantee != AcceptanceGuarantee::Unspecified {
            return Err(CoreError::InvalidConfig(format!(
                "route {route:?}: produce guarantee without produce capability"
            )));
        }
        if transactions && !produce {
            return Err(CoreError::InvalidConfig(format!(
                "route {route:?}: transactions require produce capability"
            )));
        }
        if headers && !produce && !subscribe && !fetch {
            return Err(CoreError::InvalidConfig(format!(
                "route {route:?}: headers require a message operation"
            )));
        }
        if manual_settlement {
            if !subscribe && !fetch {
                return Err(CoreError::InvalidConfig(format!(
                    "route {route:?}: manual settlement requires a read capability"
                )));
            }
            if self.settlement.ack == AckGranularity::Unsupported {
                return Err(CoreError::InvalidConfig(format!(
                    "route {route:?}: manual settlement requires ACK semantics"
                )));
            }
        } else if self.settlement != SettlementProfile::default() {
            return Err(CoreError::InvalidConfig(format!(
                "route {route:?}: settlement profile without manual settlement capability"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub key: Bytes,
    pub value: Bytes,
}

pub type Headers = Vec<Header>;

/// Validates Fujin's canonical unordered header multimap.
///
/// # Errors
///
/// Returns [`CoreError::InvalidHeaders`] when a key is empty or is not valid UTF-8.
pub fn validate_headers(headers: &[Header]) -> Result<()> {
    for (index, header) in headers.iter().enumerate() {
        if header.key.is_empty() {
            return Err(CoreError::InvalidHeaders(format!(
                "header key {index} is empty"
            )));
        }
        if std::str::from_utf8(&header.key).is_err() {
            return Err(CoreError::InvalidHeaders(format!(
                "header key {index} is not UTF-8"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub payload: Bytes,
    /// `None` is ordinary PRODUCE; `Some`, including empty, is HPRODUCE.
    pub headers: Option<Headers>,
}

impl Message {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            headers: None,
        }
    }

    pub fn with_headers(payload: Bytes, headers: Headers) -> Self {
        Self {
            payload,
            headers: Some(headers),
        }
    }
}

/// Session-local identifier for an accepted connector operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationToken(u64);

impl OperationToken {
    const INTERNAL_BIT: u64 = 1 << 63;

    /// Creates an adapter-visible token.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] when the reserved internal bit is set.
    pub fn external(value: u64) -> Result<Self> {
        if value & Self::INTERNAL_BIT != 0 {
            return Err(CoreError::InvalidConfig(
                "operation token uses the reserved internal bit".into(),
            ));
        }
        Ok(Self(value))
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn is_internal(self) -> bool {
        self.0 & Self::INTERNAL_BIT != 0
    }

    #[doc(hidden)]
    pub const fn internal(sequence: u64) -> Self {
        Self(Self::INTERNAL_BIT | (sequence & !Self::INTERNAL_BIT))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completion {
    pub token: OperationToken,
    pub result: Result<()>,
}

/// Installed once per lease so hot-path operations carry only a compact token.
pub trait CompletionSink: Send + Sync + 'static {
    fn complete(&self, completion: Completion);
}

/// `Ok(())` means accepted; the sink must receive exactly one completion.
pub trait Writer: Send + Sync + 'static {
    /// Accepts one produce operation.
    ///
    /// # Errors
    ///
    /// Returns an error only when the operation was not accepted.
    fn produce(&self, token: OperationToken, message: Message) -> Result<()>;
    /// Accepts a snapshot flush barrier.
    ///
    /// # Errors
    ///
    /// Returns an error only when the barrier was not accepted.
    fn flush(&self, token: OperationToken) -> Result<()>;
    /// Accepts transaction creation.
    ///
    /// # Errors
    ///
    /// Returns an error only when the operation was not accepted.
    fn begin_transaction(&self, token: OperationToken) -> Result<()>;
    /// Accepts transaction commit.
    ///
    /// # Errors
    ///
    /// Returns an error only when the operation was not accepted.
    fn commit_transaction(&self, token: OperationToken) -> Result<()>;
    /// Accepts transaction rollback.
    ///
    /// # Errors
    ///
    /// Returns an error only when the operation was not accepted.
    fn rollback_transaction(&self, token: OperationToken) -> Result<()>;
    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>>;
    /// Declares native compliance with exactly-once completion, snapshot flush, and deterministic
    /// pending resolution on close. Non-compliant writers are wrapped by Session Core.
    fn writer_contract_compliant(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementKind {
    Ack,
    Nack,
}

pub type ReadyCallback = Box<dyn FnOnce() -> Result<()> + Send + 'static>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub message_id: Option<Bytes>,
    pub headers: Option<Headers>,
    pub payload: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettlementResult {
    pub message_id: Bytes,
    pub result: Result<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReaderEvent {
    /// Push-subscription delivery after readiness.
    Message(Delivery),
    /// One complete bounded fetch result. `reported_count` must equal `messages.len()`.
    FetchComplete {
        token: OperationToken,
        reported_count: u32,
        messages: Vec<Delivery>,
        result: Result<()>,
    },
    SettlementComplete {
        token: OperationToken,
        result: Result<()>,
        messages: Vec<SettlementResult>,
    },
    Terminal(Result<()>),
}
pub trait ReaderEventSink: Send + Sync + 'static {
    fn emit(&self, event: ReaderEvent);
}

/// Reader operations are accepted synchronously and report through the installed sink.
pub trait Reader: Send + Sync + 'static {
    /// Starts the receive lifecycle and invokes `ready` exactly once before message delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the lifecycle cannot be started. An asynchronously terminating
    /// lifecycle reports [`ReaderEvent::Terminal`].
    fn subscribe(&self, with_headers: bool, ready: ReadyCallback) -> Result<()>;
    /// Accepts one bounded fetch operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation cannot be accepted.
    fn fetch(&self, token: OperationToken, maximum: u32, with_headers: bool) -> Result<()>;
    /// Accepts one ACK or NACK operation. `settlements` owns the adapter message IDs and is
    /// returned through [`ReaderEvent::SettlementComplete`] after each result is populated.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation cannot be accepted.
    fn settle(
        &self,
        token: OperationToken,
        kind: SettlementKind,
        settlements: Vec<SettlementResult>,
    ) -> Result<()>;
    fn adapter_message_id_prefix_len(&self) -> usize;
    fn auto_settle(&self) -> bool;
    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>>;
}

pub trait ConnectorRuntime: Send + Sync + 'static {
    /// Opens a session-scoped reader lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the route is invalid or the lease cannot be opened.
    fn open_reader(
        &self,
        route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>>;
    /// Opens a session-scoped writer lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the route is invalid or the lease cannot be opened.
    fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>>;
    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>>;
}

pub trait CompiledConnector: Send + Sync + 'static {
    fn routes(&self) -> &std::collections::BTreeMap<String, RouteProfile>;
    /// Lazily creates the generation-owned runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime resources cannot be initialized.
    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>>;
    fn open_runtime_eagerly(&self) -> bool {
        false
    }
    fn exclusive_runtime_keys(&self) -> &[String] {
        &[]
    }
}

/// Side-effect-free compiler for one statically linked connector type.
pub trait ConnectorDescriptor: Send + Sync + 'static {
    /// Compiles and validates immutable settings without broker I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when the settings or route profiles are invalid.
    fn compile(&self, settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>>;
    /// Converts one whitelisted textual override into a typed JSON value.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported paths or invalid values.
    fn convert_override(&self, path: &str, value: &str) -> Result<serde_json::Value> {
        let _ = (path, value);
        Err(CoreError::OperationUnsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_profile() -> RouteProfile {
        RouteProfile {
            capabilities: Capabilities::PRODUCE
                .union(Capabilities::HEADERS)
                .union(Capabilities::TRANSACTIONS)
                .union(Capabilities::SUBSCRIBE)
                .union(Capabilities::FETCH)
                .union(Capabilities::MANUAL_SETTLEMENT),
            produce_guarantee: AcceptanceGuarantee::Peer,
            settlement: SettlementProfile {
                ack: AckGranularity::Single,
                nack: NackEffect::Requeue,
            },
        }
    }

    #[test]
    fn route_profile_rejects_contradictions() {
        assert!(
            RouteProfile {
                capabilities: Capabilities::PRODUCE,
                ..RouteProfile::default()
            }
            .validate("route")
            .is_err()
        );
        assert!(
            RouteProfile {
                capabilities: Capabilities::TRANSACTIONS,
                ..RouteProfile::default()
            }
            .validate("route")
            .is_err()
        );
        assert!(valid_profile().validate("route").is_ok());
    }

    #[test]
    fn headers_are_an_unordered_utf8_keyed_multimap() {
        assert!(validate_headers(&[]).is_ok());
        assert!(
            validate_headers(&[
                Header {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(&[0xff]),
                },
                Header {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                },
            ])
            .is_ok()
        );
        assert!(
            validate_headers(&[Header {
                key: Bytes::new(),
                value: Bytes::new(),
            }])
            .is_err()
        );
        assert!(
            validate_headers(&[Header {
                key: Bytes::from_static(&[0xff]),
                value: Bytes::new(),
            }])
            .is_err()
        );
    }
}
