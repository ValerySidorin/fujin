#![allow(dead_code)]

use std::{
    collections::{BTreeMap, VecDeque},
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use fujin_core::{
    AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, Catalog, CompiledConnector,
    Completion, CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorRuntime,
    ConnectorsConfig, CoreError, Delivery, DescriptorRegistry, GenerationCompiler, Message,
    NackEffect, NoConnectorMiddleware, OperationToken, Reader, ReaderEvent, ReaderEventSink,
    ReaderMessage, ReadyCallback, Result, RouteProfile, SessionEventSink, SettlementKind, Writer,
};
use parking_lot::Mutex;
use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct WriterPlan {
    pub produce: Option<CoreError>,
    pub flush: Option<CoreError>,
    pub begin: Option<CoreError>,
    pub commit: Option<CoreError>,
    pub rollback: Option<CoreError>,
    pub close: Option<CoreError>,
    pub hang_close: bool,
}

#[derive(Clone, Debug)]
pub struct FetchPlan {
    pub reported_count: u32,
    pub messages: Vec<ReaderMessage>,
    pub error: Option<CoreError>,
}

impl FetchPlan {
    pub fn success(messages: Vec<ReaderMessage>) -> Self {
        Self {
            reported_count: u32::try_from(messages.len()).expect("test fetch count"),
            messages,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SettlementPlan {
    pub top_error: Option<CoreError>,
    pub results: Vec<Option<CoreError>>,
}

#[derive(Clone, Debug, Default)]
pub struct ReaderPlan {
    pub ready_error: Option<CoreError>,
    pub subscription_messages: Vec<ReaderMessage>,
    pub terminal: Option<CoreError>,
    pub fetches: VecDeque<FetchPlan>,
    pub settlements: VecDeque<SettlementPlan>,
    pub close_error: Option<CoreError>,
}

#[derive(Debug, Default)]
pub struct TestState {
    runtime_opens: AtomicUsize,
    runtime_closes: AtomicUsize,
    writers: Mutex<Vec<Arc<TestWriter>>>,
    readers: Mutex<Vec<Arc<TestReader>>>,
    plans: Mutex<VecDeque<WriterPlan>>,
    reader_plans: Mutex<VecDeque<ReaderPlan>>,
    compiled_settings: Mutex<Vec<Value>>,
}

impl TestState {
    pub fn push_plan(&self, plan: WriterPlan) {
        self.plans.lock().push_back(plan);
    }

    pub fn push_reader_plan(&self, plan: ReaderPlan) {
        self.reader_plans.lock().push_back(plan);
    }

    pub fn readers(&self) -> Vec<Arc<TestReader>> {
        self.readers.lock().clone()
    }

    pub fn writers(&self) -> Vec<Arc<TestWriter>> {
        self.writers.lock().clone()
    }

    pub fn runtime_opens(&self) -> usize {
        self.runtime_opens.load(Ordering::Acquire)
    }

    pub fn runtime_closes(&self) -> usize {
        self.runtime_closes.load(Ordering::Acquire)
    }

    pub fn compiled_settings(&self) -> Vec<Value> {
        self.compiled_settings.lock().clone()
    }
}

pub struct TestDescriptor {
    state: Arc<TestState>,
    routes: BTreeMap<String, RouteProfile>,
}

impl TestDescriptor {
    fn new(state: Arc<TestState>) -> Self {
        let producer = RouteProfile {
            capabilities: Capabilities::PRODUCE.union(Capabilities::HEADERS),
            produce_guarantee: AcceptanceGuarantee::Peer,
            ..RouteProfile::default()
        };
        let transaction = RouteProfile {
            capabilities: Capabilities::PRODUCE
                .union(Capabilities::HEADERS)
                .union(Capabilities::TRANSACTIONS),
            produce_guarantee: AcceptanceGuarantee::Peer,
            ..RouteProfile::default()
        };
        let plain = RouteProfile {
            capabilities: Capabilities::PRODUCE,
            produce_guarantee: AcceptanceGuarantee::Peer,
            ..RouteProfile::default()
        };
        let reader = RouteProfile {
            capabilities: Capabilities::HEADERS
                .union(Capabilities::SUBSCRIBE)
                .union(Capabilities::FETCH)
                .union(Capabilities::MANUAL_SETTLEMENT),
            settlement: fujin_core::SettlementProfile {
                ack: AckGranularity::Cumulative,
                nack: NackEffect::Requeue,
            },
            ..RouteProfile::default()
        };
        let reader_no_nack = RouteProfile {
            settlement: fujin_core::SettlementProfile {
                ack: AckGranularity::Cumulative,
                nack: NackEffect::Unsupported,
            },
            ..reader
        };
        Self {
            state,
            routes: BTreeMap::from([
                ("topic".into(), producer),
                ("tx".into(), transaction),
                ("plain".into(), plain),
                ("read".into(), reader),
                ("read_no_nack".into(), reader_no_nack),
            ]),
        }
    }
}

impl ConnectorDescriptor for TestDescriptor {
    fn compile(&self, settings: &Value) -> Result<Arc<dyn CompiledConnector>> {
        self.state.compiled_settings.lock().push(settings.clone());
        Ok(Arc::new(TestCompiled {
            state: Arc::clone(&self.state),
            routes: self.routes.clone(),
        }))
    }

    fn convert_override(&self, path: &str, value: &str) -> Result<Value> {
        if path.rsplit('.').next() == Some("topic") {
            Ok(Value::String(value.to_owned()))
        } else {
            Err(CoreError::OperationUnsupported)
        }
    }
}

struct TestCompiled {
    state: Arc<TestState>,
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for TestCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        self.state.runtime_opens.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(TestRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct TestRuntime {
    state: Arc<TestState>,
}

impl ConnectorRuntime for TestRuntime {
    fn open_reader(
        &self,
        route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        let reader = Arc::new(TestReader {
            route: route.to_owned(),
            auto_settle,
            events,
            plan: Mutex::new(
                self.state
                    .reader_plans
                    .lock()
                    .pop_front()
                    .unwrap_or_default(),
            ),
            subscribe_count: AtomicUsize::new(0),
            fetch_count: AtomicUsize::new(0),
            settlement_count: AtomicUsize::new(0),
            close_count: AtomicUsize::new(0),
        });
        self.state.readers.lock().push(Arc::clone(&reader));
        Ok(reader)
    }

    fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        let writer = Arc::new(TestWriter {
            route: route.to_owned(),
            completions,
            plan: self.state.plans.lock().pop_front().unwrap_or_default(),
            messages: Mutex::new(Vec::new()),
            produce_count: AtomicUsize::new(0),
            flush_count: AtomicUsize::new(0),
            begin_count: AtomicUsize::new(0),
            commit_count: AtomicUsize::new(0),
            rollback_count: AtomicUsize::new(0),
            close_count: AtomicUsize::new(0),
        });
        self.state.writers.lock().push(Arc::clone(&writer));
        Ok(writer)
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.state.runtime_closes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct WriterSnapshot {
    pub route: String,
    pub messages: Vec<Message>,
    pub produce: usize,
    pub flush: usize,
    pub begin: usize,
    pub commit: usize,
    pub rollback: usize,
    pub close: usize,
}

pub struct TestWriter {
    route: String,
    completions: Arc<dyn CompletionSink>,
    plan: WriterPlan,
    messages: Mutex<Vec<Message>>,
    produce_count: AtomicUsize,
    flush_count: AtomicUsize,
    begin_count: AtomicUsize,
    commit_count: AtomicUsize,
    rollback_count: AtomicUsize,
    close_count: AtomicUsize,
}

impl std::fmt::Debug for TestWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestWriter")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl TestWriter {
    pub fn snapshot(&self) -> WriterSnapshot {
        WriterSnapshot {
            route: self.route.clone(),
            messages: self.messages.lock().clone(),
            produce: self.produce_count.load(Ordering::Acquire),
            flush: self.flush_count.load(Ordering::Acquire),
            begin: self.begin_count.load(Ordering::Acquire),
            commit: self.commit_count.load(Ordering::Acquire),
            rollback: self.rollback_count.load(Ordering::Acquire),
            close: self.close_count.load(Ordering::Acquire),
        }
    }

    fn finish(&self, token: OperationToken, result: Result<()>) {
        self.completions.complete(Completion { token, result });
    }
}

impl Writer for TestWriter {
    fn produce(&self, token: OperationToken, message: Message) -> Result<()> {
        self.produce_count.fetch_add(1, Ordering::Relaxed);
        self.messages.lock().push(message);
        self.finish(token, self.plan.produce.clone().map_or(Ok(()), Err));
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        self.finish(token, self.plan.flush.clone().map_or(Ok(()), Err));
        Ok(())
    }

    fn begin_transaction(&self, token: OperationToken) -> Result<()> {
        self.begin_count.fetch_add(1, Ordering::Relaxed);
        self.finish(token, self.plan.begin.clone().map_or(Ok(()), Err));
        Ok(())
    }

    fn commit_transaction(&self, token: OperationToken) -> Result<()> {
        self.commit_count.fetch_add(1, Ordering::Relaxed);
        self.finish(token, self.plan.commit.clone().map_or(Ok(()), Err));
        Ok(())
    }

    fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
        self.rollback_count.fetch_add(1, Ordering::Relaxed);
        self.finish(token, self.plan.rollback.clone().map_or(Ok(()), Err));
        Ok(())
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.close_count.fetch_add(1, Ordering::Relaxed);
            if self.plan.hang_close {
                pending::<()>().await;
            }
            self.plan.close.clone().map_or(Ok(()), Err)
        })
    }
}

#[derive(Debug, Default)]
pub struct CompletionRecorder {
    values: Mutex<Vec<Completion>>,
}

impl CompletionRecorder {
    pub fn values(&self) -> Vec<Completion> {
        self.values.lock().clone()
    }
}

impl CompletionSink for CompletionRecorder {
    fn complete(&self, completion: Completion) {
        self.values.lock().push(completion);
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ReaderSnapshot {
    pub route: String,
    pub auto_settle: bool,
    pub subscribe: usize,
    pub fetch: usize,
    pub settlement: usize,
    pub close: usize,
}

pub struct TestReader {
    route: String,
    auto_settle: bool,
    events: Arc<dyn ReaderEventSink>,
    plan: Mutex<ReaderPlan>,
    subscribe_count: AtomicUsize,
    fetch_count: AtomicUsize,
    settlement_count: AtomicUsize,
    close_count: AtomicUsize,
}

impl std::fmt::Debug for TestReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestReader")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl TestReader {
    pub fn snapshot(&self) -> ReaderSnapshot {
        ReaderSnapshot {
            route: self.route.clone(),
            auto_settle: self.auto_settle,
            subscribe: self.subscribe_count.load(Ordering::Acquire),
            fetch: self.fetch_count.load(Ordering::Acquire),
            settlement: self.settlement_count.load(Ordering::Acquire),
            close: self.close_count.load(Ordering::Acquire),
        }
    }
}

impl Reader for TestReader {
    fn subscribe(&self, _with_headers: bool, ready: ReadyCallback) -> Result<()> {
        self.subscribe_count.fetch_add(1, Ordering::Relaxed);
        let (ready_error, messages, terminal) = {
            let mut plan = self.plan.lock();
            (
                plan.ready_error.take(),
                std::mem::take(&mut plan.subscription_messages),
                plan.terminal.take(),
            )
        };
        if let Some(error) = ready_error {
            return Err(error);
        }
        ready()?;
        for message in messages {
            self.events.emit(ReaderEvent::Message(message));
        }
        if let Some(error) = terminal {
            self.events.emit(ReaderEvent::Terminal(Err(error)));
        }
        Ok(())
    }

    fn fetch(&self, token: OperationToken, _maximum: u32, _with_headers: bool) -> Result<()> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let plan = self.plan.lock().fetches.pop_front().unwrap_or(FetchPlan {
            reported_count: 0,
            messages: Vec::new(),
            error: None,
        });
        self.events.emit(ReaderEvent::FetchComplete {
            token,
            reported_count: plan.reported_count,
            messages: plan.messages,
            result: plan.error.map_or(Ok(()), Err),
        });
        Ok(())
    }

    fn settle(
        &self,
        token: OperationToken,
        _kind: SettlementKind,
        mut settlements: Vec<fujin_core::SettlementResult>,
    ) -> Result<()> {
        self.settlement_count.fetch_add(1, Ordering::Relaxed);
        let plan = self.plan.lock().settlements.pop_front().unwrap_or_default();
        settlements.truncate(plan.results.len());
        for (settlement, error) in settlements.iter_mut().zip(plan.results) {
            settlement.result = error.map_or(Ok(()), Err);
        }
        self.events.emit(ReaderEvent::SettlementComplete {
            token,
            result: plan.top_error.map_or(Ok(()), Err),
            messages: settlements,
        });
        Ok(())
    }

    fn adapter_message_id_prefix_len(&self) -> usize {
        1
    }

    fn auto_settle(&self) -> bool {
        self.auto_settle
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.close_count.fetch_add(1, Ordering::Relaxed);
            self.plan.lock().close_error.clone().map_or(Ok(()), Err)
        })
    }
}

#[derive(Debug, Default)]
pub struct SessionRecorder {
    deliveries: Mutex<Vec<Delivery>>,
    terminals: Mutex<Vec<(u8, CoreError)>>,
}

impl SessionRecorder {
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.deliveries.lock().clone()
    }

    pub fn terminals(&self) -> Vec<(u8, CoreError)> {
        self.terminals.lock().clone()
    }
}

impl SessionEventSink for SessionRecorder {
    fn delivery(&self, delivery: Delivery) {
        self.deliveries.lock().push(delivery);
    }

    fn subscription_terminal(&self, subscription_id: u8, error: CoreError) {
        self.terminals.lock().push((subscription_id, error));
    }
}

pub async fn catalog_and_state() -> (Arc<Catalog>, Arc<TestState>, ConnectorsConfig) {
    let state = Arc::new(TestState::default());
    let registry = Arc::new(DescriptorRegistry::default());
    registry
        .register("test", Arc::new(TestDescriptor::new(Arc::clone(&state))))
        .expect("register test descriptor");
    let compiler = Arc::new(GenerationCompiler::new(
        registry,
        Arc::new(NoConnectorMiddleware),
    ));
    let configs = BTreeMap::from([(
        "connector".into(),
        ConnectorConfig {
            connector_type: "test".into(),
            overridable: vec!["routes.*.topic".into()],
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::json!({"routes":{"pub":{"topic":"before"}}}),
        },
    )]);
    let catalog = Arc::new(
        Catalog::compile(&configs, compiler)
            .await
            .expect("compile test catalog"),
    );
    (catalog, state, configs)
}
