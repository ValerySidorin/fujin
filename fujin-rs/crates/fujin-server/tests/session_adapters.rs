#![cfg(feature = "grpc")]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use fujin_core::{
    AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, Catalog, CompiledConnector,
    Completion, CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorRegistry,
    ConnectorRuntime, Delivery, GenerationCompiler, Header, NackEffect, NoBindMiddleware,
    OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback, Result, RouteProfile,
    SettlementKind, SettlementProfile, Writer,
};
use fujin_native::{RequestCode, ResponseCode};
use fujin_proto::fujin::v1 as pb;
use fujin_server::GrpcService;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream},
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tonic::transport::Server;

#[derive(Default)]
struct TestState {
    pending_produces: Mutex<Vec<(Arc<dyn CompletionSink>, OperationToken)>>,
    writer_closes: AtomicUsize,
    reader_closes: AtomicUsize,
    runtime_closes: AtomicUsize,
}

impl TestState {
    fn pending_produces(&self) -> usize {
        self.pending_produces.lock().expect("pending lock").len()
    }

    fn complete_produces(&self) {
        let pending = std::mem::take(&mut *self.pending_produces.lock().expect("pending lock"));
        for (sink, token) in pending {
            sink.complete(Completion {
                token,
                result: Ok(()),
            });
        }
    }
}

struct TestDescriptor {
    state: Arc<TestState>,
    routes: BTreeMap<String, RouteProfile>,
}

impl TestDescriptor {
    fn new(state: Arc<TestState>) -> Self {
        Self {
            state,
            routes: BTreeMap::from([(
                "route".into(),
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
                },
            )]),
        }
    }
}

impl ConnectorDescriptor for TestDescriptor {
    fn compile(&self, _settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        Ok(Arc::new(TestCompiled {
            state: Arc::clone(&self.state),
            routes: self.routes.clone(),
        }))
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
        _route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        Ok(Arc::new(TestReader {
            state: Arc::clone(&self.state),
            auto_settle,
            events,
        }))
    }

    fn open_writer(
        &self,
        _route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        Ok(Arc::new(TestWriter {
            state: Arc::clone(&self.state),
            completions,
        }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.state.runtime_closes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

struct TestWriter {
    state: Arc<TestState>,
    completions: Arc<dyn CompletionSink>,
}

impl Writer for TestWriter {
    fn produce(&self, token: OperationToken, _message: fujin_core::Message) -> Result<()> {
        self.state
            .pending_produces
            .lock()
            .expect("pending lock")
            .push((Arc::clone(&self.completions), token));
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.completions.complete(Completion {
            token,
            result: Ok(()),
        });
        Ok(())
    }

    fn begin_transaction(&self, token: OperationToken) -> Result<()> {
        self.flush(token)
    }

    fn commit_transaction(&self, token: OperationToken) -> Result<()> {
        self.flush(token)
    }

    fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
        self.flush(token)
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.state.writer_closes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

struct TestReader {
    state: Arc<TestState>,
    auto_settle: bool,
    events: Arc<dyn ReaderEventSink>,
}

impl Reader for TestReader {
    fn subscribe(&self, with_headers: bool, ready: ReadyCallback) -> Result<()> {
        ready()?;
        self.events.emit(ReaderEvent::Message(reader_message(
            b"subscribed",
            with_headers,
        )));
        Ok(())
    }

    fn fetch(&self, token: OperationToken, _maximum: u32, with_headers: bool) -> Result<()> {
        self.events.emit(ReaderEvent::FetchComplete {
            token,
            reported_count: 1,
            messages: vec![reader_message(b"fetched", with_headers)],
            result: Ok(()),
        });
        Ok(())
    }

    fn settle(
        &self,
        token: OperationToken,
        _kind: SettlementKind,
        settlements: Vec<fujin_core::SettlementResult>,
    ) -> Result<()> {
        self.events.emit(ReaderEvent::SettlementComplete {
            token,
            result: Ok(()),
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
            self.state.reader_closes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

fn reader_message(payload: &'static [u8], with_headers: bool) -> Delivery {
    Delivery {
        payload: Bytes::from_static(payload),
        headers: with_headers.then(|| {
            vec![Header {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            }]
        }),
        message_id: Some(Bytes::from_static(b"a")),
    }
}

async fn catalog() -> (Arc<Catalog>, Arc<TestState>) {
    let state = Arc::new(TestState::default());
    let registry = Arc::new(ConnectorRegistry::default());
    registry
        .register("test", Arc::new(TestDescriptor::new(Arc::clone(&state))))
        .expect("register descriptor");
    let compiler = Arc::new(GenerationCompiler::without_middlewares(registry));
    let configs = BTreeMap::from([(
        "connector".into(),
        ConnectorConfig {
            connector_type: "test".into(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::Value::Null,
        },
    )]);
    let catalog = Arc::new(
        Catalog::compile(&configs, compiler)
            .await
            .expect("compile catalog"),
    );
    (catalog, state)
}

fn append_bytes(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(
        &u32::try_from(value.len())
            .expect("test length")
            .to_be_bytes(),
    );
    buffer.extend_from_slice(value);
}

fn hello_frame() -> Vec<u8> {
    let mut frame = vec![RequestCode::Hello as u8, 1, 1, 1];
    append_bytes(&mut frame, b"test-client");
    append_bytes(&mut frame, b"test-build");
    frame
}

fn bind_frame() -> Vec<u8> {
    let mut frame = vec![RequestCode::Bind as u8];
    append_bytes(&mut frame, b"connector");
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame
}

fn produce_frame(correlation_id: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::Produce as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    append_bytes(&mut frame, b"route");
    append_bytes(&mut frame, b"message");
    frame
}

fn fetch_frame(correlation_id: u32, maximum: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::Fetch as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.push(0);
    append_bytes(&mut frame, b"route");
    frame.extend_from_slice(&maximum.to_be_bytes());
    frame
}

fn ack_frame(correlation_id: u32, subscription_id: u8, message_id: &[u8]) -> Vec<u8> {
    let mut frame = vec![RequestCode::Ack as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.push(subscription_id);
    frame.extend_from_slice(&1_u32.to_be_bytes());
    append_bytes(&mut frame, message_id);
    frame
}

fn subscribe_frame(correlation_id: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::Subscribe as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.push(1);
    append_bytes(&mut frame, b"route");
    frame
}

fn begin_transaction_frame(correlation_id: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::BeginTransaction as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    append_bytes(&mut frame, b"route");
    frame
}

fn transaction_produce_frame(correlation_id: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::TransactionProduce as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    append_bytes(&mut frame, b"transaction-message");
    frame
}

fn commit_transaction_frame(correlation_id: u32) -> Vec<u8> {
    let mut frame = vec![RequestCode::CommitTransaction as u8];
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame
}

async fn read_u32<R: AsyncRead + Unpin>(reader: &mut R) -> u32 {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes).await.expect("read u32");
    u32::from_be_bytes(bytes)
}

async fn read_bytes<R: AsyncRead + Unpin>(reader: &mut R) -> Vec<u8> {
    let length = usize::try_from(read_u32(reader).await).expect("byte length");
    let mut value = vec![0; length];
    reader.read_exact(&mut value).await.expect("read bytes");
    value
}

async fn read_native_success_header(
    stream: &mut DuplexStream,
    code: ResponseCode,
    correlation_id: u32,
) {
    let mut header = [0_u8; 6];
    stream
        .read_exact(&mut header)
        .await
        .expect("read response header");
    assert_eq!(header[0], code as u8);
    assert_eq!(
        u32::from_be_bytes(header[1..5].try_into().expect("correlation bytes")),
        correlation_id
    );
    assert_eq!(header[5], 0);
}

async fn wait_for_pending(state: &TestState) {
    timeout(Duration::from_secs(1), async {
        while state.pending_produces() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("produce accepted");
}

async fn exercise_native_hello_bind_and_produce(client: &mut DuplexStream, state: &TestState) {
    for byte in hello_frame() {
        client.write_all(&[byte]).await.expect("write HELLO byte");
    }
    let mut hello = [0_u8; 4];
    client
        .read_exact(&mut hello)
        .await
        .expect("read HELLO response");
    assert_eq!(hello, [ResponseCode::Hello as u8, 0, 1, 1]);
    assert_eq!(read_bytes(client).await, b"test-server");

    let mut coalesced = bind_frame();
    coalesced.extend_from_slice(&produce_frame(7));
    client
        .write_all(&coalesced)
        .await
        .expect("write BIND and PRODUCE");
    let mut bind_header = [0_u8; 6];
    client
        .read_exact(&mut bind_header)
        .await
        .expect("read BIND header");
    assert_eq!(bind_header[0], ResponseCode::Bind as u8);
    assert_eq!(bind_header[1], 0);
    assert_eq!(
        u32::from_be_bytes(bind_header[2..6].try_into().expect("route count")),
        1
    );
    assert_eq!(read_bytes(client).await, b"route");
    let mut profile = [0_u8; 4];
    client
        .read_exact(&mut profile)
        .await
        .expect("read route profile");
    assert_eq!(profile, [0x3f, 2, 1, 1]);

    wait_for_pending(state).await;
    state.complete_produces();
    read_native_success_header(client, ResponseCode::Produce, 7).await;
}

async fn exercise_native_fetch_and_ack(client: &mut DuplexStream) {
    client
        .write_all(&fetch_frame(8, 1))
        .await
        .expect("write FETCH");
    read_native_success_header(client, ResponseCode::Fetch, 8).await;
    let mut fetch_header = [0_u8; 5];
    client
        .read_exact(&mut fetch_header)
        .await
        .expect("read FETCH body header");
    let subscription_id = fetch_header[0];
    assert_eq!(
        u32::from_be_bytes(fetch_header[1..5].try_into().expect("message count")),
        1
    );
    let message_id = read_bytes(client).await;
    assert!(!message_id.is_empty());
    assert_eq!(read_bytes(client).await, b"fetched");

    client
        .write_all(&ack_frame(9, subscription_id, &message_id))
        .await
        .expect("write ACK");
    read_native_success_header(client, ResponseCode::Ack, 9).await;
    assert_eq!(read_u32(client).await, 1);
    assert_eq!(read_bytes(client).await, message_id);
    let mut result_status = [0_u8; 1];
    client
        .read_exact(&mut result_status)
        .await
        .expect("read ACK result");
    assert_eq!(result_status, [0]);
}

async fn exercise_native_subscribe_and_transaction(client: &mut DuplexStream, state: &TestState) {
    client
        .write_all(&subscribe_frame(10))
        .await
        .expect("write SUBSCRIBE");
    read_native_success_header(client, ResponseCode::Subscribe, 10).await;
    let mut subscribed = [0_u8; 1];
    client
        .read_exact(&mut subscribed)
        .await
        .expect("read subscription ID");
    let mut message_prefix = [0_u8; 2];
    client
        .read_exact(&mut message_prefix)
        .await
        .expect("read subscription message prefix");
    assert_eq!(message_prefix, [ResponseCode::Message as u8, subscribed[0]]);
    assert_eq!(read_bytes(client).await, b"subscribed");

    client
        .write_all(&begin_transaction_frame(11))
        .await
        .expect("write BEGIN");
    read_native_success_header(client, ResponseCode::BeginTransaction, 11).await;
    client
        .write_all(&transaction_produce_frame(12))
        .await
        .expect("write TX_PRODUCE");
    wait_for_pending(state).await;
    state.complete_produces();
    read_native_success_header(client, ResponseCode::TransactionProduce, 12).await;
    client
        .write_all(&commit_transaction_frame(13))
        .await
        .expect("write COMMIT");
    read_native_success_header(client, ResponseCode::CommitTransaction, 13).await;
}

#[tokio::test]
async fn native_adapter_runs_session_core_over_fragmented_stream() {
    let (catalog, state) = catalog().await;
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let server_catalog = Arc::clone(&catalog);
    let server_task = tokio::spawn(async move {
        fujin_native::run(
            server,
            server_catalog,
            Arc::new(NoBindMiddleware),
            "test-server",
        )
        .await
    });

    exercise_native_hello_bind_and_produce(&mut client, &state).await;
    exercise_native_fetch_and_ack(&mut client).await;
    exercise_native_subscribe_and_transaction(&mut client, &state).await;
    client
        .write_all(&[RequestCode::Disconnect as u8])
        .await
        .expect("write DISCONNECT");
    let mut disconnect = [0_u8; 1];
    client
        .read_exact(&mut disconnect)
        .await
        .expect("read DISCONNECT");
    assert_eq!(disconnect, [ResponseCode::Disconnect as u8]);
    server_task
        .await
        .expect("native task")
        .expect("native session");

    assert_eq!(state.writer_closes.load(Ordering::Acquire), 1);
    assert_eq!(state.reader_closes.load(Ordering::Acquire), 2);
    catalog.close().await.expect("close catalog");
    assert_eq!(state.runtime_closes.load(Ordering::Acquire), 1);
}

fn request(request: pb::fujin_request::Request) -> pb::FujinRequest {
    pb::FujinRequest {
        request: Some(request),
    }
}

async fn receive_grpc(stream: &mut tonic::Streaming<pb::FujinResponse>) -> pb::FujinResponse {
    timeout(Duration::from_secs(1), stream.message())
        .await
        .expect("gRPC response timeout")
        .expect("gRPC stream status")
        .expect("gRPC response")
}

type GrpcRequestSender = mpsc::UnboundedSender<pb::FujinRequest>;

async fn exercise_grpc_bind(
    request_sender: &GrpcRequestSender,
    stream: &mut tonic::Streaming<pb::FujinResponse>,
) {
    request_sender
        .send(request(pb::fujin_request::Request::Bind(pb::BindRequest {
            connector: "connector".into(),
            meta: HashMap::default(),
            config_overrides: HashMap::default(),
        })))
        .expect("send BIND");
    let bind = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Bind(bind)) = bind.response else {
        panic!("expected BIND response");
    };
    assert!(bind.error.is_none());
    let profile = &bind.routes["route"];
    assert!(profile.produce && profile.fetch && profile.manual_settlement);
    assert_eq!(
        profile.produce_guarantee,
        pb::ProduceGuarantee::PeerAccept as i32
    );
}

async fn exercise_grpc_delayed_produce(
    request_sender: &GrpcRequestSender,
    stream: &mut tonic::Streaming<pb::FujinResponse>,
    state: &TestState,
) {
    request_sender
        .send(request(pb::fujin_request::Request::Produce(
            pb::ProduceRequest {
                correlation_id: 7,
                route: "route".into(),
                message: b"message".to_vec(),
            },
        )))
        .expect("send PRODUCE");
    wait_for_pending(state).await;
    state.complete_produces();
    let produced = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Produce(produced)) = produced.response else {
        panic!("expected PRODUCE response");
    };
    assert_eq!(produced.correlation_id, 7);
    assert!(produced.error.is_none());
}

async fn exercise_grpc_fetch_and_ack(
    request_sender: &GrpcRequestSender,
    stream: &mut tonic::Streaming<pb::FujinResponse>,
) {
    request_sender
        .send(request(pb::fujin_request::Request::Fetch(
            pb::FetchRequest {
                correlation_id: 8,
                route: "route".into(),
                auto_commit: false,
                batch_size: 1,
            },
        )))
        .expect("send FETCH");
    let fetched = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Fetch(fetched)) = fetched.response else {
        panic!("expected FETCH response");
    };
    assert!(fetched.error.is_none());
    assert_eq!(fetched.messages.len(), 1);
    assert_eq!(fetched.messages[0].payload, b"fetched");
    assert!(!fetched.messages[0].message_id.is_empty());

    request_sender
        .send(request(pb::fujin_request::Request::Ack(pb::AckRequest {
            correlation_id: 9,
            message_ids: vec![fetched.messages[0].message_id.clone()],
            subscription_id: fetched.subscription_id,
        })))
        .expect("send ACK");
    let acked = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Ack(acked)) = acked.response else {
        panic!("expected ACK response");
    };
    assert!(acked.error.is_none());
    assert_eq!(acked.results.len(), 1);
    assert!(acked.results[0].error.is_none());
}

async fn exercise_grpc_subscribe_and_transaction(
    request_sender: &GrpcRequestSender,
    stream: &mut tonic::Streaming<pb::FujinResponse>,
    state: &TestState,
) {
    request_sender
        .send(request(pb::fujin_request::Request::Subscribe(
            pb::SubscribeRequest {
                correlation_id: 10,
                route: "route".into(),
                auto_commit: true,
            },
        )))
        .expect("send SUBSCRIBE");
    let subscribed = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Subscribe(subscribed)) = subscribed.response else {
        panic!("expected SUBSCRIBE response before delivery");
    };
    assert!(subscribed.error.is_none());
    let delivered = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::Message(delivered)) = delivered.response else {
        panic!("expected subscription delivery after readiness");
    };
    assert_eq!(delivered.subscription_id, subscribed.subscription_id);
    assert_eq!(delivered.payload, b"subscribed");
    assert!(delivered.message_id.is_empty());

    request_sender
        .send(request(pb::fujin_request::Request::BeginTx(
            pb::BeginTxRequest {
                correlation_id: 11,
                route: "route".into(),
            },
        )))
        .expect("send BEGIN");
    let begun = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::BeginTx(begun)) = begun.response else {
        panic!("expected BEGIN response");
    };
    assert!(begun.error.is_none());
    request_sender
        .send(request(pb::fujin_request::Request::TxProduce(
            pb::TxProduceRequest {
                correlation_id: 12,
                message: b"transaction-message".to_vec(),
            },
        )))
        .expect("send TX_PRODUCE");
    wait_for_pending(state).await;
    state.complete_produces();
    let produced = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::TxProduce(produced)) = produced.response else {
        panic!("expected TX_PRODUCE response");
    };
    assert!(produced.error.is_none());
    request_sender
        .send(request(pb::fujin_request::Request::CommitTx(
            pb::CommitTxRequest { correlation_id: 13 },
        )))
        .expect("send COMMIT");
    let committed = receive_grpc(stream).await;
    let Some(pb::fujin_response::Response::CommitTx(committed)) = committed.response else {
        panic!("expected COMMIT response");
    };
    assert!(committed.error.is_none());
}

#[tokio::test]
async fn grpc_adapter_runs_same_core_and_emits_delayed_completion_without_next_request() {
    let (catalog, state) = catalog().await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gRPC listener");
    let address = listener.local_addr().expect("gRPC address");
    let service = GrpcService::new(Arc::clone(&catalog), Arc::new(NoBindMiddleware));
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::fujin_service_server::FujinServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    let mut client =
        pb::fujin_service_client::FujinServiceClient::connect(format!("http://{address}"))
            .await
            .expect("connect gRPC client");
    let (request_sender, request_receiver) = mpsc::unbounded_channel();
    let mut stream = client
        .stream(UnboundedReceiverStream::new(request_receiver))
        .await
        .expect("open gRPC stream")
        .into_inner();

    exercise_grpc_bind(&request_sender, &mut stream).await;
    exercise_grpc_delayed_produce(&request_sender, &mut stream, &state).await;
    exercise_grpc_fetch_and_ack(&request_sender, &mut stream).await;
    exercise_grpc_subscribe_and_transaction(&request_sender, &mut stream, &state).await;
    drop(request_sender);
    while stream
        .message()
        .await
        .expect("finish gRPC stream")
        .is_some()
    {}
    shutdown_sender.send(()).expect("shutdown gRPC server");
    server_task
        .await
        .expect("gRPC server task")
        .expect("gRPC server");

    assert_eq!(state.writer_closes.load(Ordering::Acquire), 1);
    assert_eq!(state.reader_closes.load(Ordering::Acquire), 2);
    catalog.close().await.expect("close catalog");
    assert_eq!(state.runtime_closes.load(Ordering::Acquire), 1);
}
