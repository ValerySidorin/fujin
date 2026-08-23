mod reader;

pub use reader::{FetchResult, NoSessionEvents, SessionEventSink};

use reader::{FetchKey, ReaderSlot};

use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

use crate::{
    BindMiddlewareRunner, Binding, Capabilities, Catalog, Completion, CompletionSink, CoreError,
    Message, OperationToken, Result, RouteProfile, Writer, validate_headers,
};

const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const WRITER_POOL_SIZE_PER_ROUTE: usize = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum SessionState {
    #[default]
    Unbound = 0,
    Connected = 1,
    InTransaction = 2,
    Closed = 3,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindResult {
    pub routes: BTreeMap<String, RouteProfile>,
}

struct WriterSlot {
    profile: RouteProfile,
    writer: Arc<dyn Writer>,
}

struct Transaction {
    route: String,
    profile: RouteProfile,
    writer: Arc<dyn Writer>,
}

#[derive(Default)]
struct WriterPool {
    idle: BTreeMap<String, Vec<WriterSlot>>,
}

impl WriterPool {
    fn take(&mut self, route: &str) -> Option<WriterSlot> {
        let writers = self.idle.get_mut(route)?;
        let writer = writers.pop();
        if writers.is_empty() {
            self.idle.remove(route);
        }
        writer
    }

    fn put(&mut self, route: String, writer: WriterSlot) -> Option<WriterSlot> {
        let writers = self.idle.entry(route).or_default();
        if writers.len() == WRITER_POOL_SIZE_PER_ROUTE {
            Some(writer)
        } else {
            writers.push(writer);
            None
        }
    }
}

struct CompletionRouter {
    external: Arc<dyn CompletionSink>,
    next_internal: std::sync::atomic::AtomicU64,
    pending: Mutex<BTreeMap<OperationToken, oneshot::Sender<Result<()>>>>,
}

impl fmt::Debug for CompletionRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionRouter")
            .field("pending", &self.pending.lock().len())
            .finish_non_exhaustive()
    }
}

impl CompletionRouter {
    fn new(external: Arc<dyn CompletionSink>) -> Arc<Self> {
        Arc::new(Self {
            external,
            next_internal: std::sync::atomic::AtomicU64::new(1),
            pending: Mutex::new(BTreeMap::new()),
        })
    }

    async fn submit(&self, operation: impl FnOnce(OperationToken) -> Result<()>) -> Result<()> {
        let sequence = self
            .next_internal
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let token = OperationToken::internal(sequence);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(token, sender);
        if let Err(error) = operation(token) {
            self.pending.lock().remove(&token);
            return Err(error);
        }
        receiver
            .await
            .map_err(|_| CoreError::Internal("connector dropped an accepted completion".into()))?
    }
}

impl CompletionSink for CompletionRouter {
    fn complete(&self, completion: Completion) {
        if !completion.token.is_internal() {
            self.external.complete(completion);
            return;
        }
        if let Some(sender) = self.pending.lock().remove(&completion.token) {
            let _ = sender.send(completion.result);
        }
    }
}

/// Transport-neutral owner of one Fujin session.
///
/// A protocol adapter owns this value on one task and calls its methods sequentially. Connector
/// callbacks remain concurrent and enter only through the installed completion and reader sinks.
pub struct SessionCore {
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    completions: Arc<CompletionRouter>,
    events: Arc<dyn SessionEventSink>,
    state: SessionState,
    binding: Option<Binding>,
    writers: BTreeMap<String, WriterSlot>,
    writer_pool: WriterPool,
    transaction: Option<Transaction>,
    readers: BTreeMap<u8, ReaderSlot>,
    fetch_readers: BTreeMap<FetchKey, u8>,
    subscription_ids: [bool; 256],
    next_incarnation: u32,
    cleanup: Option<JoinHandle<Result<()>>>,
}

impl fmt::Debug for SessionCore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionCore")
            .field("state", &self.state)
            .field("binding", &self.binding)
            .field("writers", &self.writers.keys())
            .field("readers", &self.readers.keys())
            .field(
                "transaction_route",
                &self
                    .transaction
                    .as_ref()
                    .map(|transaction| &transaction.route),
            )
            .finish_non_exhaustive()
    }
}

impl SessionCore {
    pub fn new(
        catalog: Arc<Catalog>,
        bind_middlewares: Arc<dyn BindMiddlewareRunner>,
        completions: Arc<dyn CompletionSink>,
    ) -> Self {
        Self::new_with_events(
            catalog,
            bind_middlewares,
            completions,
            Arc::new(NoSessionEvents),
        )
    }

    pub fn new_with_events(
        catalog: Arc<Catalog>,
        bind_middlewares: Arc<dyn BindMiddlewareRunner>,
        completions: Arc<dyn CompletionSink>,
        events: Arc<dyn SessionEventSink>,
    ) -> Self {
        Self {
            catalog,
            bind_middlewares,
            completions: CompletionRouter::new(completions),
            events,
            state: SessionState::Unbound,
            binding: None,
            writers: BTreeMap::new(),
            writer_pool: WriterPool::default(),
            transaction: None,
            readers: BTreeMap::new(),
            fetch_readers: BTreeMap::new(),
            subscription_ids: [false; 256],
            next_incarnation: 0,
            cleanup: None,
        }
    }

    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// Pins the currently published connector generation and applies allowed private overrides.
    ///
    /// # Errors
    ///
    /// Returns an error when already bound or closed, the connector is absent, middleware rejects
    /// the request, or configuration override compilation fails. Failure leaves the session unbound.
    pub async fn bind(
        &mut self,
        connector_name: &str,
        metadata: &mut BTreeMap<String, String>,
        overrides: &BTreeMap<String, String>,
    ) -> Result<BindResult> {
        match self.state {
            SessionState::Unbound => {}
            SessionState::Closed => return Err(CoreError::Closed),
            SessionState::Connected | SessionState::InTransaction => {
                return Err(CoreError::AlreadyBound);
            }
        }
        let generation = self
            .catalog
            .current()
            .ok_or_else(|| CoreError::ConnectorNotFound(connector_name.into()))?;
        let config = generation
            .connector_config(connector_name)
            .ok_or_else(|| CoreError::ConnectorNotFound(connector_name.into()))?;
        self.bind_middlewares
            .run(connector_name, &config, metadata)?;
        let binding = generation
            .acquire_with_overrides(connector_name, overrides)
            .await?;
        let routes = binding.route_profiles();
        self.binding = Some(binding);
        self.state = SessionState::Connected;
        Ok(BindResult { routes })
    }

    /// Accepts one non-transactional produce operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the session state, route capability, headers, writer acquisition, or
    /// connector acceptance fails. Accepted operations complete exactly once through the sink.
    pub fn produce(&mut self, token: OperationToken, route: &str, message: Message) -> Result<()> {
        self.require_connected()?;
        if self.transaction.is_some() {
            return Err(CoreError::TransactionCommandRequired);
        }
        if let Some(slot) = self.writers.get(route) {
            validate_produce(slot.profile, &message)?;
            return slot.writer.produce(token, message);
        }
        let profile = self.route_profile(route)?;
        validate_produce(profile, &message)?;
        let writer = self.acquire_writer(route)?;
        let slot = match self.writers.entry(route.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(writer),
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        };
        slot.writer.produce(token, message)
    }

    /// Accepts one produce operation on the active transaction route.
    ///
    /// # Errors
    ///
    /// Returns an error when no transaction is active, headers are unsupported or invalid, or the
    /// connector rejects the operation.
    pub fn transaction_produce(&mut self, token: OperationToken, message: Message) -> Result<()> {
        self.require_bound()?;
        let transaction = self.transaction.as_ref().ok_or(CoreError::NoTransaction)?;
        validate_produce(transaction.profile, &message)?;
        transaction.writer.produce(token, message)
    }

    /// Flushes ordinary writers and begins one concrete connector transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid state or route, a flush failure, writer acquisition failure,
    /// or rejected/failed transaction begin. A failed begin leaves the session connected.
    pub async fn begin_transaction(&mut self, route: &str) -> Result<()> {
        self.require_connected()?;
        if self.transaction.is_some() {
            return Err(CoreError::TransactionActive);
        }
        let profile = self.route_profile(route)?;
        if !profile.capabilities.contains(Capabilities::TRANSACTIONS) {
            return Err(CoreError::OperationUnsupported);
        }
        let ordinary_writers = std::mem::take(&mut self.writers);
        let mut cleanup_errors = Vec::new();
        for (writer_route, slot) in ordinary_writers {
            if let Err(error) = self
                .completions
                .submit(|token| slot.writer.flush(token))
                .await
            {
                cleanup_errors.push(format!("flush writer {writer_route:?}: {error}"));
                if let Err(error) = slot.writer.close().await {
                    cleanup_errors.push(format!("close writer {writer_route:?}: {error}"));
                }
            } else if let Some(overflow) = self.writer_pool.put(writer_route.clone(), slot)
                && let Err(error) = overflow.writer.close().await
            {
                cleanup_errors.push(format!("close pooled writer {writer_route:?}: {error}"));
            }
        }
        if !cleanup_errors.is_empty() {
            return Err(CoreError::Unavailable(cleanup_errors.join("; ")));
        }
        let slot = self.acquire_writer(route)?;
        if let Err(error) = self
            .completions
            .submit(|token| slot.writer.begin_transaction(token))
            .await
        {
            let _ = Arc::clone(&slot.writer).close().await;
            return Err(error);
        }
        self.transaction = Some(Transaction {
            route: route.to_owned(),
            profile: slot.profile,
            writer: slot.writer,
        });
        self.state = SessionState::InTransaction;
        Ok(())
    }

    /// Flushes and commits the active transaction, always ending local transaction state.
    ///
    /// # Errors
    ///
    /// Flush failure returns [`CoreError::TransactionAborted`]; commit failure returns
    /// [`CoreError::CommitOutcomeUnknown`]. Either failure closes the poisoned writer.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        self.require_bound()?;
        let transaction = self.transaction.take().ok_or(CoreError::NoTransaction)?;
        self.state = SessionState::Connected;
        if let Err(error) = self
            .completions
            .submit(|token| transaction.writer.flush(token))
            .await
        {
            let _ = self
                .completions
                .submit(|token| transaction.writer.rollback_transaction(token))
                .await;
            let _ = Arc::clone(&transaction.writer).close().await;
            return Err(CoreError::TransactionAborted(error.to_string()));
        }
        if let Err(error) = self
            .completions
            .submit(|token| transaction.writer.commit_transaction(token))
            .await
        {
            let _ = Arc::clone(&transaction.writer).close().await;
            return Err(CoreError::CommitOutcomeUnknown(error.to_string()));
        }
        let route = transaction.route;
        let overflow = self.writer_pool.put(
            route.clone(),
            WriterSlot {
                profile: transaction.profile,
                writer: transaction.writer,
            },
        );
        if let Some(writer) = overflow {
            writer.writer.close().await.map_err(|error| {
                CoreError::Unavailable(format!("close pooled writer {route:?}: {error}"))
            })?;
        }
        Ok(())
    }

    /// Rolls back the active transaction, always ending local transaction state.
    ///
    /// # Errors
    ///
    /// Returns the connector rollback failure after closing the poisoned writer.
    pub async fn rollback_transaction(&mut self) -> Result<()> {
        self.require_bound()?;
        let transaction = self.transaction.take().ok_or(CoreError::NoTransaction)?;
        self.state = SessionState::Connected;
        if let Err(error) = self
            .completions
            .submit(|token| transaction.writer.rollback_transaction(token))
            .await
        {
            let _ = Arc::clone(&transaction.writer).close().await;
            return Err(error);
        }
        let route = transaction.route;
        let overflow = self.writer_pool.put(
            route.clone(),
            WriterSlot {
                profile: transaction.profile,
                writer: transaction.writer,
            },
        );
        if let Some(writer) = overflow {
            writer.writer.close().await.map_err(|error| {
                CoreError::Unavailable(format!("close pooled writer {route:?}: {error}"))
            })?;
        }
        Ok(())
    }

    /// Closes every writer, then releases the generation binding.
    ///
    /// # Errors
    ///
    /// Returns aggregated flush, rollback, and writer-close failures after attempting all cleanup.
    pub async fn close(&mut self) -> Result<()> {
        self.close_with_timeout(DEFAULT_CLEANUP_TIMEOUT).await
    }

    /// Detaches session resources and waits up to `cleanup_timeout` for cleanup.
    /// Timed-out cleanup continues in its owned task and can be awaited by a later call.
    ///
    /// # Errors
    ///
    /// Returns aggregated cleanup failures or [`CoreError::Unavailable`] when the deadline expires.
    pub async fn close_with_timeout(&mut self, cleanup_timeout: Duration) -> Result<()> {
        if self.cleanup.is_none() {
            if self.state == SessionState::Closed {
                return Ok(());
            }
            self.state = SessionState::Closed;
            let transaction = self.transaction.take();
            let writers = std::mem::take(&mut self.writers);
            let writer_pool = std::mem::take(&mut self.writer_pool);
            let readers = std::mem::take(&mut self.readers);
            self.fetch_readers.clear();
            self.subscription_ids.fill(false);
            for slot in readers.values() {
                slot.router.deactivate();
            }
            let binding = self.binding.take();
            let completions = Arc::clone(&self.completions);
            self.cleanup = Some(tokio::spawn(async move {
                cleanup_session_resources(
                    transaction,
                    writers,
                    writer_pool,
                    readers,
                    binding,
                    completions,
                )
                .await
            }));
        }
        let Some(cleanup) = self.cleanup.as_mut() else {
            return Ok(());
        };
        match timeout(cleanup_timeout, cleanup).await {
            Ok(joined) => {
                self.cleanup = None;
                joined.map_err(|error| {
                    CoreError::Internal(format!("session cleanup task: {error}"))
                })?
            }
            Err(_) => Err(CoreError::Unavailable(format!(
                "session cleanup timed out after {cleanup_timeout:?}"
            ))),
        }
    }

    fn require_bound(&self) -> Result<()> {
        match self.state {
            SessionState::Unbound => Err(CoreError::NotBound),
            SessionState::Closed => Err(CoreError::Closed),
            SessionState::Connected | SessionState::InTransaction => Ok(()),
        }
    }

    fn require_connected(&self) -> Result<()> {
        self.require_bound()
    }

    fn binding(&self) -> Result<&Binding> {
        self.binding.as_ref().ok_or(CoreError::NotBound)
    }

    fn route_profile(&self, route: &str) -> Result<RouteProfile> {
        self.binding()?.route_profile(route)
    }

    fn acquire_writer(&mut self, route: &str) -> Result<WriterSlot> {
        if let Some(writer) = self.writer_pool.take(route) {
            return Ok(writer);
        }
        let profile = self.route_profile(route)?;
        let writer = self.binding()?.open_writer(
            route,
            Arc::clone(&self.completions) as Arc<dyn CompletionSink>,
        )?;
        Ok(WriterSlot { profile, writer })
    }
}

async fn cleanup_session_resources(
    transaction: Option<Transaction>,
    writers: BTreeMap<String, WriterSlot>,
    writer_pool: WriterPool,
    readers: BTreeMap<u8, ReaderSlot>,
    binding: Option<Binding>,
    completions: Arc<CompletionRouter>,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Some(transaction) = transaction {
        if let Err(error) = completions
            .submit(|token| transaction.writer.rollback_transaction(token))
            .await
        {
            errors.push(format!("rollback transaction: {error}"));
        }
        if let Err(error) = transaction.writer.close().await {
            errors.push(format!("close transaction writer: {error}"));
        }
    }
    for (route, slot) in writers {
        if let Err(error) = completions.submit(|token| slot.writer.flush(token)).await {
            errors.push(format!("flush writer {route:?}: {error}"));
        }
        if let Err(error) = slot.writer.close().await {
            errors.push(format!("close writer {route:?}: {error}"));
        }
    }
    for (route, writers) in writer_pool.idle {
        for slot in writers {
            if let Err(error) = slot.writer.close().await {
                errors.push(format!("close pooled writer {route:?}: {error}"));
            }
        }
    }
    for (id, slot) in readers {
        if let Err(error) = slot.reader.close().await {
            errors.push(format!("close reader {id}: {error}"));
        }
    }
    drop(binding);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CoreError::Internal(errors.join("; ")))
    }
}

fn validate_produce(profile: RouteProfile, message: &Message) -> Result<()> {
    if !profile.capabilities.contains(Capabilities::PRODUCE) {
        return Err(CoreError::OperationUnsupported);
    }
    if let Some(headers) = message.headers.as_deref() {
        if !profile.capabilities.contains(Capabilities::HEADERS) {
            return Err(CoreError::OperationUnsupported);
        }
        validate_headers(headers)?;
    }
    Ok(())
}
