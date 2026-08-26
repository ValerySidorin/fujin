//! Support shared by the feature-gated no-broker performance benchmark binaries.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use fujin_connector::{
    AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, Catalog, CompiledConnector,
    Completion, CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorRegistry,
    ConnectorRuntime, Delivery, GenerationCompiler, Header, Message, NackEffect, OperationToken,
    Reader, ReaderEvent, ReaderEventSink, ReadyCallback, RouteProfile, SettlementKind,
    SettlementProfile, SettlementResult, Writer,
};
use fujin_error::{CoreError, Result};
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

const SESSION_BENCH_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
static SESSION_BENCH_PAYLOAD: [u8; SESSION_BENCH_MAX_PAYLOAD_BYTES] =
    [0; SESSION_BENCH_MAX_PAYLOAD_BYTES];

/// Builds the one-route, locally-acknowledged connector used by protocol benchmarks.
///
/// # Errors
///
/// Returns an error if the in-process Nop connector cannot be registered or compiled.
pub async fn nop_catalog() -> Result<Arc<Catalog>> {
    let registry = Arc::new(ConnectorRegistry::default());
    registry.register("nop", Arc::new(NopDescriptor))?;
    let compiler = Arc::new(GenerationCompiler::without_middlewares(registry));
    let configs = BTreeMap::from([(
        "connector".into(),
        ConnectorConfig {
            connector_type: "nop".into(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::Value::Null,
        },
    )]);
    Ok(Arc::new(Catalog::compile(&configs, compiler).await?))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkOperation {
    Produce,
    HProduce,
    Fetch,
    HFetch,
    Subscribe,
    HSubscribe,
    Ack,
    Nack,
    Transaction,
}

impl BenchmarkOperation {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::HProduce => "hproduce",
            Self::Fetch => "fetch",
            Self::HFetch => "hfetch",
            Self::Subscribe => "subscribe",
            Self::HSubscribe => "hsubscribe",
            Self::Ack => "ack",
            Self::Nack => "nack",
            Self::Transaction => "transaction",
        }
    }

    pub const fn uses_batch(self) -> bool {
        matches!(self, Self::Fetch | Self::HFetch | Self::Ack | Self::Nack)
    }

    pub const fn is_subscription(self) -> bool {
        matches!(self, Self::Subscribe | Self::HSubscribe)
    }
}

impl std::str::FromStr for BenchmarkOperation {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "produce" => Ok(Self::Produce),
            "hproduce" => Ok(Self::HProduce),
            "fetch" => Ok(Self::Fetch),
            "hfetch" => Ok(Self::HFetch),
            "subscribe" => Ok(Self::Subscribe),
            "hsubscribe" => Ok(Self::HSubscribe),
            "ack" => Ok(Self::Ack),
            "nack" => Ok(Self::Nack),
            "transaction" => Ok(Self::Transaction),
            _ => Err(format!("unknown benchmark operation {value:?}")),
        }
    }
}

pub const BENCHMARK_MAX_BATCH_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Validates one benchmark cell against the shared Go performance matrix.
///
/// # Errors
///
/// Returns a descriptive error when the operation shape exceeds the approved matrix bounds.
pub fn validate_benchmark_shape(
    operation: BenchmarkOperation,
    payload: usize,
    batch: usize,
    concurrency: usize,
    operations: usize,
) -> Result<(), String> {
    if !operation.uses_batch() && batch != 1 {
        return Err(format!("operation {} requires batch=1", operation.label()));
    }
    let batch_payload = payload
        .checked_mul(batch)
        .ok_or_else(|| "batch payload size overflow".to_owned())?;
    if batch_payload > BENCHMARK_MAX_BATCH_PAYLOAD_BYTES {
        return Err(format!(
            "payload*batch is {batch_payload} bytes, maximum is {BENCHMARK_MAX_BATCH_PAYLOAD_BYTES}"
        ));
    }
    if operations < concurrency {
        return Err(format!(
            "operations ({operations}) must be at least concurrency ({concurrency})"
        ));
    }
    Ok(())
}

pub fn benchmark_subscription_plan(
    operation: BenchmarkOperation,
    operation_counts: &[usize],
) -> (Vec<usize>, Vec<Arc<Semaphore>>) {
    if !operation.is_subscription() {
        return (Vec::new(), Vec::new());
    }
    (
        operation_counts.to_vec(),
        operation_counts
            .iter()
            .map(|_| Arc::new(Semaphore::new(1)))
            .collect(),
    )
}

/// Start barrier shared by synthetic subscription readers and the benchmark harness.
#[derive(Debug, Default)]
pub struct SubscribeGate {
    started: AtomicBool,
    ready: Notify,
}

impl SubscribeGate {
    pub fn start(&self) {
        if !self.started.swap(true, Ordering::AcqRel) {
            self.ready.notify_waiters();
        }
    }

    async fn wait(&self) {
        while !self.started.load(Ordering::Acquire) {
            self.ready.notified().await;
        }
    }
}

#[derive(Debug)]
struct SessionBenchPlan {
    payload: Bytes,
    subscribe_limits: Mutex<VecDeque<usize>>,
    subscribe_permits: Mutex<VecDeque<Arc<Semaphore>>>,
    subscribe_gate: Arc<SubscribeGate>,
}

/// Builds the three-route synthetic connector used for full Session Core benchmarks.
///
/// # Errors
///
/// Returns an error if the descriptor cannot be registered or the immutable catalog cannot be
/// compiled.
pub async fn session_bench_catalog(
    payload_size: usize,
    subscribe_limits: Vec<usize>,
    subscribe_permits: Vec<Arc<Semaphore>>,
    subscribe_gate: Arc<SubscribeGate>,
) -> Result<Arc<Catalog>> {
    let payload = SESSION_BENCH_PAYLOAD
        .get(..payload_size)
        .ok_or_else(|| {
            CoreError::InvalidConfig(format!(
                "session benchmark payload is {payload_size} bytes, maximum is {SESSION_BENCH_MAX_PAYLOAD_BYTES}"
            ))
        })?;
    let registry = Arc::new(ConnectorRegistry::default());
    registry.register(
        "session_bench",
        Arc::new(SessionBenchDescriptor {
            plan: Arc::new(SessionBenchPlan {
                payload: Bytes::from_static(payload),
                subscribe_limits: Mutex::new(subscribe_limits.into()),
                subscribe_permits: Mutex::new(subscribe_permits.into()),
                subscribe_gate,
            }),
        }),
    )?;
    let compiler = Arc::new(GenerationCompiler::without_middlewares(registry));
    let configs = BTreeMap::from([(
        "connector".into(),
        ConnectorConfig {
            connector_type: "session_bench".into(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::Value::Null,
        },
    )]);
    Ok(Arc::new(Catalog::compile(&configs, compiler).await?))
}

struct SessionBenchDescriptor {
    plan: Arc<SessionBenchPlan>,
}

impl ConnectorDescriptor for SessionBenchDescriptor {
    fn compile(&self, _settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        Ok(Arc::new(SessionBenchCompiled {
            plan: Arc::clone(&self.plan),
            routes: BTreeMap::from([
                (
                    "pub".into(),
                    RouteProfile {
                        capabilities: Capabilities::PRODUCE.union(Capabilities::HEADERS),
                        produce_guarantee: AcceptanceGuarantee::Local,
                        settlement: SettlementProfile::default(),
                    },
                ),
                (
                    "tx".into(),
                    RouteProfile {
                        capabilities: Capabilities::PRODUCE
                            .union(Capabilities::HEADERS)
                            .union(Capabilities::TRANSACTIONS),
                        produce_guarantee: AcceptanceGuarantee::Local,
                        settlement: SettlementProfile::default(),
                    },
                ),
                (
                    "sub".into(),
                    RouteProfile {
                        capabilities: Capabilities::HEADERS
                            .union(Capabilities::SUBSCRIBE)
                            .union(Capabilities::FETCH)
                            .union(Capabilities::MANUAL_SETTLEMENT),
                        produce_guarantee: AcceptanceGuarantee::Unspecified,
                        settlement: SettlementProfile {
                            ack: AckGranularity::Single,
                            nack: NackEffect::Drop,
                        },
                    },
                ),
            ]),
        }))
    }
}

struct SessionBenchCompiled {
    plan: Arc<SessionBenchPlan>,
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for SessionBenchCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        Ok(Arc::new(SessionBenchRuntime {
            plan: Arc::clone(&self.plan),
        }))
    }
}

struct SessionBenchRuntime {
    plan: Arc<SessionBenchPlan>,
}

impl ConnectorRuntime for SessionBenchRuntime {
    fn open_reader(
        &self,
        route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        if route != "sub" {
            return Err(CoreError::RouteNotFound(route.into()));
        }
        Ok(Arc::new(SessionBenchReader {
            payload: self.plan.payload.clone(),
            subscribe_limit: self.plan.subscribe_limits.lock().pop_front().unwrap_or(0),
            subscribe_permit: self.plan.subscribe_permits.lock().pop_front(),
            subscribe_gate: Arc::clone(&self.plan.subscribe_gate),
            auto_settle,
            events,
            closed: CancellationToken::new(),
        }))
    }

    fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        if route != "pub" && route != "tx" {
            return Err(CoreError::RouteNotFound(route.into()));
        }
        Ok(Arc::new(NopWriter { completions }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct SessionBenchReader {
    payload: Bytes,
    subscribe_limit: usize,
    subscribe_permit: Option<Arc<Semaphore>>,
    subscribe_gate: Arc<SubscribeGate>,
    auto_settle: bool,
    events: Arc<dyn ReaderEventSink>,
    closed: CancellationToken,
}

impl SessionBenchReader {
    fn message(&self, with_headers: bool) -> Delivery {
        Delivery {
            payload: self.payload.clone(),
            headers: with_headers.then(|| {
                vec![Header {
                    key: Bytes::from_static(b"content-type"),
                    value: Bytes::from_static(b"application/octet-stream"),
                }]
            }),
            message_id: (!self.auto_settle).then(|| Bytes::from_static(b"sub")),
        }
    }
}

impl Reader for SessionBenchReader {
    fn subscribe(&self, with_headers: bool, ready: ReadyCallback) -> Result<()> {
        ready()?;
        let events = Arc::clone(&self.events);
        let payload = self.payload.clone();
        let subscribe_gate = Arc::clone(&self.subscribe_gate);
        let closed = self.closed.clone();
        let limit = self.subscribe_limit;
        let subscribe_permit = self.subscribe_permit.clone();
        tokio::spawn(async move {
            subscribe_gate.wait().await;
            for _ in 0..limit {
                if let Some(permit) = &subscribe_permit {
                    tokio::select! {
                        () = closed.cancelled() => return,
                        acquired = permit.acquire() => match acquired {
                            Ok(acquired) => acquired.forget(),
                            Err(_) => return,
                        },
                    }
                } else if closed.is_cancelled() {
                    return;
                }
                events.emit(ReaderEvent::Message(Delivery {
                    payload: payload.clone(),
                    headers: with_headers.then(|| {
                        vec![Header {
                            key: Bytes::from_static(b"content-type"),
                            value: Bytes::from_static(b"application/octet-stream"),
                        }]
                    }),
                    message_id: Some(Bytes::from_static(b"sub")),
                }));
            }
        });
        Ok(())
    }

    fn fetch(&self, token: OperationToken, maximum: u32, with_headers: bool) -> Result<()> {
        let count = usize::try_from(maximum)
            .map_err(|_| CoreError::InvalidConfig("fetch size overflow".into()))?;
        self.events.emit(ReaderEvent::FetchComplete {
            token,
            reported_count: maximum,
            messages: (0..count).map(|_| self.message(with_headers)).collect(),
            result: Ok(()),
        });
        Ok(())
    }

    fn settle(
        &self,
        token: OperationToken,
        _kind: SettlementKind,
        settlements: Vec<SettlementResult>,
    ) -> Result<()> {
        self.events.emit(ReaderEvent::SettlementComplete {
            token,
            result: Ok(()),
            messages: settlements,
        });
        Ok(())
    }

    fn adapter_message_id_prefix_len(&self) -> usize {
        3
    }

    fn auto_settle(&self) -> bool {
        self.auto_settle
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.closed.cancel();
            Ok(())
        })
    }
}

struct NopDescriptor;

impl ConnectorDescriptor for NopDescriptor {
    fn compile(&self, _settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        Ok(Arc::new(NopCompiled {
            routes: BTreeMap::from([(
                "pub".into(),
                RouteProfile {
                    capabilities: Capabilities::PRODUCE
                        .union(Capabilities::HEADERS)
                        .union(Capabilities::TRANSACTIONS),
                    produce_guarantee: AcceptanceGuarantee::Local,
                    settlement: SettlementProfile::default(),
                },
            )]),
        }))
    }
}

struct NopCompiled {
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for NopCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        Ok(Arc::new(NopRuntime))
    }
}

struct NopRuntime;

impl ConnectorRuntime for NopRuntime {
    fn open_reader(
        &self,
        _route: &str,
        _auto_settle: bool,
        _events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        Err(CoreError::OperationUnsupported)
    }

    fn open_writer(
        &self,
        _route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        Ok(Arc::new(NopWriter { completions }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct NopWriter {
    completions: Arc<dyn CompletionSink>,
}

impl NopWriter {
    fn complete(&self, token: OperationToken) {
        self.completions.complete(Completion {
            token,
            result: Ok(()),
        });
    }
}

impl Writer for NopWriter {
    fn produce(&self, token: OperationToken, _message: Message) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn begin_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn commit_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_shape_matches_go_matrix_bounds() {
        assert!(validate_benchmark_shape(BenchmarkOperation::Fetch, 1024, 256, 128, 1000).is_ok());
        assert!(
            validate_benchmark_shape(BenchmarkOperation::Fetch, 1024 * 1024, 32, 1, 1000).is_err()
        );
        assert!(validate_benchmark_shape(BenchmarkOperation::Produce, 128, 32, 1, 1000).is_err());
        assert!(validate_benchmark_shape(BenchmarkOperation::Produce, 128, 1, 128, 100).is_err());
    }

    #[tokio::test]
    async fn session_benchmark_payload_uses_static_storage_with_a_bounded_size() {
        let catalog = session_bench_catalog(
            SESSION_BENCH_MAX_PAYLOAD_BYTES,
            Vec::new(),
            Vec::new(),
            Arc::new(SubscribeGate::default()),
        )
        .await;
        assert!(catalog.is_ok());

        let error = session_bench_catalog(
            SESSION_BENCH_MAX_PAYLOAD_BYTES + 1,
            Vec::new(),
            Vec::new(),
            Arc::new(SubscribeGate::default()),
        )
        .await
        .expect_err("oversized benchmark payload must fail");
        assert!(error.to_string().contains("maximum is 1048576"));
    }

    #[test]
    fn subscription_plan_assigns_one_demand_per_worker() {
        let (limits, permits) = benchmark_subscription_plan(BenchmarkOperation::Subscribe, &[7, 6]);
        assert_eq!(limits, [7, 6]);
        assert_eq!(permits.len(), 2);
        assert!(permits.iter().all(|permit| permit.available_permits() == 1));

        let (limits, permits) = benchmark_subscription_plan(BenchmarkOperation::Produce, &[7, 6]);
        assert!(limits.is_empty());
        assert!(permits.is_empty());
    }
}
