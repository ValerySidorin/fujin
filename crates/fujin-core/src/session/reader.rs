use crate::{
    AckGranularity, Capabilities, CoreError, Delivery, NackEffect, Reader, ReaderEvent,
    ReaderEventSink, Result, RouteProfile, SettlementKind, SettlementResult, validate_headers,
};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::sync::oneshot;

use super::SessionCore;

const MESSAGE_ID_VERSION: u8 = 1;
const MESSAGE_ID_ENVELOPE_LEN: usize = 9;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResult {
    pub subscription_id: u8,
    pub messages: Vec<Delivery>,
}

type SettlementReceiver = oneshot::Receiver<Result<Vec<SettlementResult>>>;

pub trait SessionEventSink: Send + Sync + 'static {
    fn delivery(&self, subscription_id: u8, delivery: Delivery);
    fn subscription_terminal(&self, subscription_id: u8, error: CoreError);
}

#[derive(Debug, Default)]
pub struct NoSessionEvents;

impl SessionEventSink for NoSessionEvents {
    fn delivery(&self, _subscription_id: u8, _delivery: Delivery) {}
    fn subscription_terminal(&self, _subscription_id: u8, _error: CoreError) {}
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FetchKey {
    pub(super) route: String,
    pub(super) auto_settle: bool,
    pub(super) with_headers: bool,
}
pub(super) struct ReaderSlot {
    pub reader: Arc<dyn Reader>,
    pub profile: RouteProfile,
    pub router: Arc<ReaderRouter>,
    pub fetch_key: Option<FetchKey>,
}

impl fmt::Debug for ReaderSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderSlot")
            .field("profile", &self.profile)
            .field("router", &self.router)
            .field("fetch_key", &self.fetch_key)
            .finish_non_exhaustive()
    }
}

struct FetchWait {
    token: crate::OperationToken,
    maximum: u32,
    sender: oneshot::Sender<Result<Vec<Delivery>>>,
}

struct SettlementWait {
    token: crate::OperationToken,
    originals: Vec<Bytes>,
    states: Vec<u8>,
    sender: oneshot::Sender<Result<Vec<SettlementResult>>>,
}

#[derive(Clone, Copy, Debug)]
struct SettlementRange {
    first: u64,
    last: u64,
}

#[derive(Default)]
struct SettlementTracker {
    settled_through: u64,
    settled_ranges: Vec<SettlementRange>,
}

impl SettlementTracker {
    fn contains(&self, sequence: u64) -> bool {
        sequence <= self.settled_through
            || self
                .settled_ranges
                .iter()
                .any(|range| sequence >= range.first && sequence <= range.last)
    }

    fn mark_settled(&mut self, sequence: u64) {
        if sequence <= self.settled_through {
            return;
        }
        if sequence == self.settled_through + 1 {
            self.settled_through = sequence;
            while self
                .settled_ranges
                .first()
                .is_some_and(|range| range.first == self.settled_through + 1)
            {
                self.settled_through = self.settled_ranges.remove(0).last;
            }
            return;
        }
        for index in 0..self.settled_ranges.len() {
            let range = self.settled_ranges[index];
            if sequence >= range.first && sequence <= range.last {
                return;
            }
            if sequence + 1 == range.first {
                self.settled_ranges[index].first = sequence;
                return;
            }
            if sequence == range.last + 1 {
                self.settled_ranges[index].last = sequence;
                if index + 1 < self.settled_ranges.len()
                    && sequence + 1 == self.settled_ranges[index + 1].first
                {
                    let next = self.settled_ranges.remove(index + 1);
                    self.settled_ranges[index].last = next.last;
                }
                return;
            }
            if sequence < range.first {
                self.settled_ranges.insert(
                    index,
                    SettlementRange {
                        first: sequence,
                        last: sequence,
                    },
                );
                return;
            }
        }
        self.settled_ranges.push(SettlementRange {
            first: sequence,
            last: sequence,
        });
    }
}

pub(super) struct ReaderRouter {
    subscription_id: u8,
    auto_settle: bool,
    with_headers: bool,
    incarnation: u32,
    adapter_prefix_len: AtomicUsize,
    active: AtomicBool,
    next_sequence: AtomicU32,
    next_operation: AtomicU64,
    events: Arc<dyn SessionEventSink>,
    fetch: Mutex<Option<FetchWait>>,
    settlement: Mutex<Option<SettlementWait>>,
    tracker: Mutex<SettlementTracker>,
}

impl fmt::Debug for ReaderRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderRouter")
            .field("subscription_id", &self.subscription_id)
            .field("auto_settle", &self.auto_settle)
            .field("with_headers", &self.with_headers)
            .field("incarnation", &self.incarnation)
            .field("active", &self.active.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ReaderRouter {
    pub(super) fn new(
        subscription_id: u8,
        auto_settle: bool,
        with_headers: bool,
        incarnation: u32,
        events: Arc<dyn SessionEventSink>,
    ) -> Arc<Self> {
        Arc::new(Self {
            subscription_id,
            auto_settle,
            with_headers,
            incarnation,
            adapter_prefix_len: AtomicUsize::new(0),
            active: AtomicBool::new(false),
            next_sequence: AtomicU32::new(1),
            next_operation: AtomicU64::new(1),
            events,
            fetch: Mutex::new(None),
            settlement: Mutex::new(None),
            tracker: Mutex::new(SettlementTracker::default()),
        })
    }

    pub(super) fn set_adapter_prefix_len(&self, prefix_len: usize) {
        self.adapter_prefix_len.store(prefix_len, Ordering::Release);
    }

    pub(super) fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }

    pub(super) fn deactivate(&self) {
        if !self.active.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(wait) = self.fetch.lock().take() {
            let _ = wait.sender.send(Err(CoreError::Closed));
        }
        if let Some(wait) = self.settlement.lock().take() {
            let _ = wait.sender.send(Err(CoreError::Closed));
        }
    }

    pub(super) fn next_operation(&self) -> crate::OperationToken {
        crate::OperationToken::internal(self.next_operation.fetch_add(1, Ordering::Relaxed))
    }

    pub(super) fn prepare_fetch(
        &self,
        token: crate::OperationToken,
        maximum: u32,
    ) -> Result<oneshot::Receiver<Result<Vec<Delivery>>>> {
        let mut fetch = self.fetch.lock();
        if fetch.is_some() {
            return Err(CoreError::FetchBusy);
        }
        let (sender, receiver) = oneshot::channel();
        *fetch = Some(FetchWait {
            token,
            maximum,
            sender,
        });
        Ok(receiver)
    }

    pub(super) fn cancel_fetch(&self, token: crate::OperationToken) {
        let mut fetch = self.fetch.lock();
        if fetch.as_ref().is_some_and(|wait| wait.token == token) {
            fetch.take();
        }
    }

    pub(super) fn prepare_settlement(
        &self,
        token: crate::OperationToken,
        message_ids: Vec<Bytes>,
    ) -> Result<(Vec<SettlementResult>, SettlementReceiver)> {
        let mut settlement = self.settlement.lock();
        if settlement.is_some() {
            return Err(CoreError::Internal(
                "settlement already in progress for reader".into(),
            ));
        }
        let mut adapters = Vec::with_capacity(message_ids.len());
        let mut sequences = Vec::with_capacity(message_ids.len());
        let tracker = self.tracker.lock();
        let mut strictly_increasing = true;
        let mut previous = None;
        for message_id in &message_ids {
            let (sequence, adapter) = self.decode_message_id(message_id)?;
            if tracker.contains(sequence) {
                return Err(CoreError::InvalidMessageId(
                    "message ID already settled or in progress".into(),
                ));
            }
            if previous.is_some_and(|value| sequence <= value) {
                strictly_increasing = false;
            }
            previous = Some(sequence);
            sequences.push(sequence);
            adapters.push(SettlementResult {
                message_id: adapter,
                result: Ok(()),
            });
        }
        drop(tracker);
        if !strictly_increasing {
            let mut request = HashSet::with_capacity(sequences.len());
            if sequences.iter().any(|sequence| !request.insert(*sequence)) {
                return Err(CoreError::InvalidMessageId(
                    "duplicate message ID in request".into(),
                ));
            }
        }
        let (sender, receiver) = oneshot::channel();
        *settlement = Some(SettlementWait {
            token,
            states: vec![0; message_ids.len()],
            originals: message_ids,
            sender,
        });
        Ok((adapters, receiver))
    }

    pub(super) fn cancel_settlement(&self, token: crate::OperationToken) {
        let mut settlement = self.settlement.lock();
        if settlement.as_ref().is_some_and(|wait| wait.token == token) {
            settlement.take();
        }
    }

    fn scope_delivery(&self, delivery: &mut Delivery) -> Result<()> {
        match (&delivery.headers, self.with_headers) {
            (Some(headers), true) => validate_headers(headers)?,
            (None, false) => {}
            _ => {
                return Err(CoreError::Internal(
                    "connector delivery header variant does not match reader".into(),
                ));
            }
        }
        if self.auto_settle {
            delivery.message_id = None;
        } else {
            let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
            if sequence == 0 {
                return Err(CoreError::SubscriptionIdsExhausted);
            }
            let adapter_message_id = delivery.message_id.take().unwrap_or_default();
            let mut encoded =
                BytesMut::with_capacity(MESSAGE_ID_ENVELOPE_LEN + adapter_message_id.len());
            encoded.put_u8(MESSAGE_ID_VERSION);
            encoded.put_u64((u64::from(self.incarnation) << 32) | u64::from(sequence));
            encoded.extend_from_slice(&adapter_message_id);
            delivery.message_id = Some(encoded.freeze());
        }
        Ok(())
    }

    fn decode_message_id(&self, message_id: &Bytes) -> Result<(u64, Bytes)> {
        if message_id.len()
            < MESSAGE_ID_ENVELOPE_LEN + self.adapter_prefix_len.load(Ordering::Acquire)
            || message_id[0] != MESSAGE_ID_VERSION
        {
            return Err(CoreError::InvalidMessageId("malformed envelope".into()));
        }
        let token = u64::from_be_bytes(
            message_id[1..MESSAGE_ID_ENVELOPE_LEN]
                .try_into()
                .expect("fixed message ID token length"),
        );
        if u32::try_from(token >> 32) != Ok(self.incarnation) {
            return Err(CoreError::InvalidMessageId(
                "stale reader incarnation".into(),
            ));
        }
        let sequence = token & u64::from(u32::MAX);
        if sequence == 0 {
            return Err(CoreError::InvalidMessageId("zero sequence".into()));
        }
        Ok((sequence, message_id.slice(MESSAGE_ID_ENVELOPE_LEN..)))
    }
}

impl ReaderRouter {
    fn complete_fetch(
        &self,
        token: crate::OperationToken,
        reported_count: u32,
        mut messages: Vec<Delivery>,
        result: Result<()>,
    ) {
        let mut fetch = self.fetch.lock();
        let Some(wait) = fetch.take() else {
            return;
        };
        if wait.token != token {
            *fetch = Some(wait);
            return;
        }
        drop(fetch);
        let outcome = result.and_then(|()| {
            let delivered = u32::try_from(messages.len()).map_err(|_| {
                CoreError::Internal("fetch delivered too many messages".into())
            })?;
            if delivered != reported_count || delivered > wait.maximum {
                return Err(CoreError::Internal(format!(
                    "fetch contract violated: reported={reported_count} delivered={delivered} maximum={}",
                    wait.maximum
                )));
            }
            for message in &mut messages {
                self.scope_delivery(message)?;
            }
            Ok(messages)
        });
        let _ = wait.sender.send(outcome);
    }

    fn complete_settlement(
        &self,
        token: crate::OperationToken,
        result: Result<()>,
        mut messages: Vec<SettlementResult>,
    ) {
        const MATCHED: u8 = 1;
        const SUCCESSFUL: u8 = 2;

        let mut settlement = self.settlement.lock();
        let Some(mut wait) = settlement.take() else {
            return;
        };
        if wait.token != token {
            *settlement = Some(wait);
            return;
        }
        drop(settlement);
        if let Err(error) = result {
            let _ = wait.sender.send(Err(error));
            return;
        }
        for (position, message) in messages.iter_mut().enumerate() {
            let adapter = &message.message_id;
            let index = if position < wait.originals.len()
                && wait.states[position] & MATCHED == 0
                && &wait.originals[position][MESSAGE_ID_ENVELOPE_LEN..] == adapter.as_ref()
            {
                Some(position)
            } else {
                wait.originals
                    .iter()
                    .enumerate()
                    .position(|(index, original)| {
                        wait.states[index] & MATCHED == 0
                            && &original[MESSAGE_ID_ENVELOPE_LEN..] == adapter.as_ref()
                    })
            };
            let Some(index) = index else {
                let _ = wait.sender.send(Err(CoreError::Internal(
                    "settlement result did not match a requested message ID".into(),
                )));
                return;
            };
            wait.states[index] |= MATCHED;
            if message.result.is_ok() {
                wait.states[index] |= SUCCESSFUL;
            }
            message.message_id = wait.originals[index].clone();
        }
        if wait.states.iter().any(|state| state & MATCHED == 0) {
            let _ = wait.sender.send(Err(CoreError::Internal(
                "settlement response omitted requested message IDs".into(),
            )));
            return;
        }
        let mut tracker = self.tracker.lock();
        for (message_id, state) in wait.originals.iter().zip(&wait.states) {
            if state & SUCCESSFUL != 0 {
                tracker.mark_settled(Self::message_id_sequence(message_id));
            }
        }
        drop(tracker);
        let _ = wait.sender.send(Ok(messages));
    }

    fn message_id_sequence(message_id: &Bytes) -> u64 {
        u64::from_be_bytes(
            message_id[1..MESSAGE_ID_ENVELOPE_LEN]
                .try_into()
                .expect("validated message ID token length"),
        ) & u64::from(u32::MAX)
    }
}

impl ReaderEventSink for ReaderRouter {
    fn emit(&self, event: ReaderEvent) {
        match event {
            ReaderEvent::Message(mut message) => match self.scope_delivery(&mut message) {
                Ok(()) => self.events.delivery(self.subscription_id, message),
                Err(error) => self
                    .events
                    .subscription_terminal(self.subscription_id, error),
            },
            ReaderEvent::FetchComplete {
                token,
                reported_count,
                messages,
                result,
            } => self.complete_fetch(token, reported_count, messages, result),
            ReaderEvent::SettlementComplete {
                token,
                result,
                messages,
            } => self.complete_settlement(token, result, messages),
            ReaderEvent::Terminal(result) => {
                let error = result.err().unwrap_or_else(|| {
                    CoreError::SubscriptionEnded("connector lifecycle returned".into())
                });
                self.events
                    .subscription_terminal(self.subscription_id, error);
            }
        }
    }
}

struct PendingFetch {
    router: Arc<ReaderRouter>,
    token: crate::OperationToken,
    armed: bool,
}

impl Drop for PendingFetch {
    fn drop(&mut self) {
        if self.armed {
            self.router.cancel_fetch(self.token);
        }
    }
}

struct PendingSettlement {
    router: Arc<ReaderRouter>,
    token: crate::OperationToken,
    armed: bool,
}

impl Drop for PendingSettlement {
    fn drop(&mut self) {
        if self.armed {
            self.router.cancel_settlement(self.token);
        }
    }
}

impl SessionCore {
    /// Creates one push subscription and returns only after connector readiness succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid session state or capabilities, reader acquisition failure,
    /// missing readiness notification, or adapter readiness rejection.
    pub async fn subscribe<F>(
        &mut self,
        route: &str,
        auto_settle: bool,
        with_headers: bool,
        ready: F,
    ) -> Result<u8>
    where
        F: FnOnce(u8) -> Result<()> + Send + 'static,
    {
        self.require_bound()?;
        let profile = self.route_profile(route)?;
        validate_read_profile(profile, true, auto_settle, with_headers)?;
        let id = self
            .create_reader(route, profile, auto_settle, with_headers, None)
            .await?;
        let (reader, router) = self.reader_parts(id)?;
        let result = Arc::new(Mutex::new(None));
        let callback_result = Arc::clone(&result);
        let callback_router = Arc::clone(&router);
        let callback: crate::ReadyCallback = Box::new(move || {
            let outcome = ready(id);
            if outcome.is_ok() {
                callback_router.activate();
            }
            *callback_result.lock() = Some(outcome.clone());
            outcome
        });
        let accepted = reader.subscribe(with_headers, callback);
        let readiness = result.lock().take().ok_or_else(|| {
            CoreError::SubscriptionEnded("connector returned before readiness".into())
        });
        let outcome = accepted.and(readiness).and_then(|result| result);
        if let Err(error) = outcome {
            return Err(self.cleanup_reader_error(id, error).await);
        }
        Ok(id)
    }

    /// Fetches up to `maximum` messages through a cached implicit reader.
    ///
    /// # Errors
    ///
    /// Returns an error for zero maximum, invalid capabilities, concurrent fetch, connector
    /// rejection, cancellation, or a violated count/order contract.
    pub async fn fetch(
        &mut self,
        route: &str,
        auto_settle: bool,
        with_headers: bool,
        maximum: u32,
    ) -> Result<FetchResult> {
        self.require_bound()?;
        if maximum == 0 {
            return Err(CoreError::InvalidBatchSize);
        }
        let profile = self.route_profile(route)?;
        validate_read_profile(profile, false, auto_settle, with_headers)?;
        let key = FetchKey {
            route: route.to_owned(),
            auto_settle,
            with_headers,
        };
        let id = if let Some(id) = self.fetch_readers.get(&key).copied() {
            id
        } else {
            self.create_reader(route, profile, auto_settle, with_headers, Some(key.clone()))
                .await?
        };
        let (reader, router) = self.reader_parts(id)?;
        let token = router.next_operation();
        let receiver = router.prepare_fetch(token, maximum)?;
        let mut pending = PendingFetch {
            router,
            token,
            armed: true,
        };
        reader.fetch(token, maximum, with_headers)?;
        let messages = receiver.await.map_err(|_| {
            CoreError::Internal("connector dropped an accepted fetch completion".into())
        })??;
        pending.armed = false;
        Ok(FetchResult {
            subscription_id: id,
            messages,
        })
    }

    /// Acknowledges manual-settlement message IDs.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid session/reader state, unsupported ACK semantics, malformed,
    /// stale, duplicate, consumed, or in-progress message IDs, or connector failure.
    pub async fn ack(
        &mut self,
        subscription_id: u8,
        message_ids: Vec<Bytes>,
    ) -> Result<Vec<SettlementResult>> {
        self.settle(subscription_id, SettlementKind::Ack, message_ids)
            .await
    }

    /// Negatively acknowledges manual-settlement message IDs.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid session/reader state, unsupported NACK semantics, malformed,
    /// stale, duplicate, consumed, or in-progress message IDs, or connector failure.
    pub async fn nack(
        &mut self,
        subscription_id: u8,
        message_ids: Vec<Bytes>,
    ) -> Result<Vec<SettlementResult>> {
        self.settle(subscription_id, SettlementKind::Nack, message_ids)
            .await
    }

    async fn settle(
        &mut self,
        subscription_id: u8,
        kind: SettlementKind,
        message_ids: Vec<Bytes>,
    ) -> Result<Vec<SettlementResult>> {
        self.require_bound()?;
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let slot = self
            .readers
            .get(&subscription_id)
            .ok_or(CoreError::SubscriptionNotFound(subscription_id))?;
        if slot.router.auto_settle
            || !slot
                .profile
                .capabilities
                .contains(Capabilities::MANUAL_SETTLEMENT)
        {
            return Err(CoreError::OperationUnsupported);
        }
        match kind {
            SettlementKind::Ack if slot.profile.settlement.ack == AckGranularity::Unsupported => {
                return Err(CoreError::OperationUnsupported);
            }
            SettlementKind::Nack if slot.profile.settlement.nack == NackEffect::Unsupported => {
                return Err(CoreError::OperationUnsupported);
            }
            SettlementKind::Ack | SettlementKind::Nack => {}
        }
        let reader = Arc::clone(&slot.reader);
        let router = Arc::clone(&slot.router);
        let token = router.next_operation();
        let (adapter_ids, receiver) = router.prepare_settlement(token, message_ids)?;
        let mut pending = PendingSettlement {
            router,
            token,
            armed: true,
        };
        reader.settle(token, kind, adapter_ids)?;
        let results = receiver.await.map_err(|_| {
            CoreError::Internal("connector dropped an accepted settlement completion".into())
        })??;
        pending.armed = false;
        Ok(results)
    }

    /// Removes one explicit or implicit reader and invalidates its outstanding message IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unavailable, the ID is absent, or reader close fails.
    pub async fn unsubscribe(&mut self, subscription_id: u8) -> Result<()> {
        self.require_bound()?;
        self.remove_reader(subscription_id).await
    }

    async fn create_reader(
        &mut self,
        route: &str,
        profile: RouteProfile,
        auto_settle: bool,
        with_headers: bool,
        fetch_key: Option<FetchKey>,
    ) -> Result<u8> {
        let id = self.allocate_subscription_id()?;
        self.next_incarnation = self.next_incarnation.wrapping_add(1);
        if self.next_incarnation == 0 {
            self.next_incarnation = 1;
        }
        let router = ReaderRouter::new(
            id,
            auto_settle,
            with_headers,
            self.next_incarnation,
            Arc::clone(&self.events),
        );
        let events: Arc<dyn ReaderEventSink> = Arc::clone(&router) as Arc<dyn ReaderEventSink>;
        let reader = match self.binding()?.open_reader(route, auto_settle, events) {
            Ok(reader) => reader,
            Err(error) => {
                self.release_subscription_id(id);
                return Err(error);
            }
        };
        router.set_adapter_prefix_len(reader.adapter_message_id_prefix_len());
        if reader.auto_settle() != auto_settle {
            router.deactivate();
            let _ = reader.close().await;
            self.release_subscription_id(id);
            return Err(CoreError::Internal(
                "connector reader settlement mode mismatch".into(),
            ));
        }
        if fetch_key.is_some() {
            router.activate();
        }
        if let Some(key) = fetch_key.clone() {
            self.fetch_readers.insert(key, id);
        }
        self.readers.insert(
            id,
            ReaderSlot {
                reader,
                profile,
                router,
                fetch_key,
            },
        );
        Ok(id)
    }

    fn reader_parts(&self, id: u8) -> Result<(Arc<dyn Reader>, Arc<ReaderRouter>)> {
        let slot = self
            .readers
            .get(&id)
            .ok_or(CoreError::SubscriptionNotFound(id))?;
        Ok((Arc::clone(&slot.reader), Arc::clone(&slot.router)))
    }

    async fn remove_reader(&mut self, id: u8) -> Result<()> {
        let slot = self
            .readers
            .remove(&id)
            .ok_or(CoreError::SubscriptionNotFound(id))?;
        if let Some(key) = &slot.fetch_key {
            self.fetch_readers.remove(key);
        }
        self.release_subscription_id(id);
        slot.router.deactivate();
        slot.reader.close().await
    }

    async fn cleanup_reader_error(&mut self, id: u8, error: CoreError) -> CoreError {
        match self.remove_reader(id).await {
            Ok(()) => error,
            Err(cleanup) => CoreError::Internal(format!("{error}; cleanup reader: {cleanup}")),
        }
    }

    fn allocate_subscription_id(&mut self) -> Result<u8> {
        let Some(index) = self.subscription_ids.iter().position(|used| !used) else {
            return Err(CoreError::SubscriptionIdsExhausted);
        };
        self.subscription_ids[index] = true;
        u8::try_from(index).map_err(|_| {
            CoreError::Internal("subscription ID exceeded one-byte protocol range".into())
        })
    }

    fn release_subscription_id(&mut self, id: u8) {
        self.subscription_ids[usize::from(id)] = false;
    }
}

fn validate_read_profile(
    profile: RouteProfile,
    subscribe: bool,
    auto_settle: bool,
    with_headers: bool,
) -> Result<()> {
    let capability = if subscribe {
        Capabilities::SUBSCRIBE
    } else {
        Capabilities::FETCH
    };
    if !profile.capabilities.contains(capability)
        || with_headers && !profile.capabilities.contains(Capabilities::HEADERS)
        || !auto_settle
            && !profile
                .capabilities
                .contains(Capabilities::MANUAL_SETTLEMENT)
    {
        return Err(CoreError::OperationUnsupported);
    }
    Ok(())
}
