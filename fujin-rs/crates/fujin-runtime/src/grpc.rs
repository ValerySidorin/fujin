use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt,
    sync::Arc,
};

use bytes::Bytes;
use fujin_core::{
    AcceptanceGuarantee, AckGranularity, BindMiddlewareRunner, Capabilities, Catalog, Completion,
    CompletionSink, CoreError, Delivery, Header, Message, NackEffect, OperationError,
    OperationOutcome, OperationToken, Result as CoreResult, RouteProfile, SessionCore,
    SessionEventSink, SettlementResult, StatusCode,
};
use fujin_proto::fujin::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub type GrpcOutput = Result<pb::FujinResponse, Status>;

const MAXIMUM_PENDING_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
// Keep enough completed messages ready for Tonic's streaming encoder to coalesce them into its
// 32 KiB yield threshold. A capacity of one forces one tiny HTTP/2 DATA frame per response.
const MAXIMUM_RESPONSE_CHANNEL_MESSAGES: usize = 4096;

#[derive(Debug, Default)]
struct GrpcQueueState {
    queue: VecDeque<GrpcOutput>,
    pending_bytes: usize,
    terminal: bool,
}

#[derive(Debug)]
struct GrpcOutputQueue {
    state: Mutex<GrpcQueueState>,
    ready: Notify,
}

impl GrpcOutputQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GrpcQueueState {
                queue: VecDeque::with_capacity(16),
                ..GrpcQueueState::default()
            }),
            ready: Notify::new(),
        })
    }

    fn push(&self, output: GrpcOutput) -> Result<(), Status> {
        let size = grpc_output_len(&output);
        let mut state = self.state.lock();
        if state.terminal {
            return Err(Status::cancelled("gRPC output stream closed"));
        }
        let exceeds_limit = state
            .pending_bytes
            .checked_add(size)
            .is_none_or(|next| next > MAXIMUM_PENDING_OUTPUT_BYTES);
        if exceeds_limit {
            let error = Status::resource_exhausted("gRPC session output exceeded 4 MiB");
            state.terminal = true;
            state.queue.push_back(Err(error.clone()));
            drop(state);
            self.ready.notify_one();
            return Err(error);
        }
        state.pending_bytes += size;
        state.terminal = output.is_err();
        state.queue.push_back(output);
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    fn try_pop(&self) -> Option<GrpcOutput> {
        let mut state = self.state.lock();
        let output = state.queue.pop_front()?;
        state.pending_bytes = state.pending_bytes.saturating_sub(grpc_output_len(&output));
        Some(output)
    }

    async fn pop(&self) -> GrpcOutput {
        loop {
            let notified = self.ready.notified();
            if let Some(output) = self.try_pop() {
                return output;
            }
            notified.await;
        }
    }
}

fn grpc_output_len(output: &GrpcOutput) -> usize {
    output.as_ref().map_or(0, prost::Message::encoded_len)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum ProduceKind {
    Produce = 1,
    HProduce = 2,
    TransactionProduce = 3,
    TransactionHProduce = 4,
}

#[derive(Debug)]
struct GrpcSink {
    output: Arc<GrpcOutputQueue>,
}

impl GrpcSink {
    fn send(&self, output: GrpcOutput) {
        let _ = self.output.push(output);
    }

    fn send_response(&self, response: pb::fujin_response::Response) {
        self.send(Ok(pb::FujinResponse {
            response: Some(response),
        }));
    }
}

impl CompletionSink for GrpcSink {
    fn complete(&self, completion: Completion) {
        let Some((kind, correlation_id)) = decode_produce_token(completion.token) else {
            self.send(Err(Status::internal("invalid gRPC completion token")));
            return;
        };
        self.send_response(produce_response(
            kind,
            correlation_id,
            completion.result.as_ref().err(),
        ));
    }
}

impl SessionEventSink for GrpcSink {
    fn delivery(&self, subscription_id: u8, delivery: Delivery) {
        self.send_response(delivery_response(subscription_id, delivery));
    }

    fn subscription_terminal(&self, _subscription_id: u8, error: CoreError) {
        self.send(Err(Status::unavailable(error.to_string())));
    }
}

pub struct GrpcSession {
    core: SessionCore,
    output: Arc<GrpcOutputQueue>,
}

impl fmt::Debug for GrpcSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcSession")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl GrpcSession {
    #[must_use]
    pub fn new(catalog: Arc<Catalog>, bind_middlewares: Arc<dyn BindMiddlewareRunner>) -> Self {
        let output = GrpcOutputQueue::new();
        let sink = Arc::new(GrpcSink {
            output: Arc::clone(&output),
        });
        Self {
            core: SessionCore::new_with_events(
                catalog,
                bind_middlewares,
                Arc::clone(&sink) as Arc<dyn CompletionSink>,
                sink as Arc<dyn SessionEventSink>,
            ),
            output,
        }
    }

    /// Applies one protobuf request to the owned Session Core.
    ///
    /// Responses and asynchronous deliveries are available through [`Self::next_output`].
    ///
    /// # Errors
    ///
    /// Returns `INVALID_ARGUMENT` when the request oneof is absent and `CANCELLED` when the output
    /// stream has already been dropped. Operation failures are encoded in response payloads.
    pub async fn handle(&mut self, request: pb::FujinRequest) -> Result<(), Status> {
        let request = request
            .request
            .ok_or_else(|| Status::invalid_argument("request oneof is required"))?;
        match request {
            pb::fujin_request::Request::Bind(request) => self.bind(request).await,
            pb::fujin_request::Request::Produce(request) => self.produce(request, false),
            pb::fujin_request::Request::Hproduce(request) => self.hproduce(request),
            pb::fujin_request::Request::TxProduce(request) => self.tx_produce(request, false),
            pb::fujin_request::Request::TxHproduce(request) => self.tx_hproduce(request),
            pb::fujin_request::Request::BeginTx(request) => self.begin_transaction(request).await,
            pb::fujin_request::Request::CommitTx(request) => self.commit_transaction(request).await,
            pb::fujin_request::Request::RollbackTx(request) => {
                self.rollback_transaction(request).await
            }
            pb::fujin_request::Request::Subscribe(request) => {
                self.subscribe(
                    request.correlation_id,
                    request.route,
                    request.auto_commit,
                    false,
                )
                .await
            }
            pb::fujin_request::Request::Hsubscribe(request) => {
                self.subscribe(
                    request.correlation_id,
                    request.route,
                    request.auto_commit,
                    true,
                )
                .await
            }
            pb::fujin_request::Request::Fetch(request) => {
                self.fetch(
                    request.correlation_id,
                    request.route,
                    request.auto_commit,
                    false,
                    request.batch_size,
                )
                .await
            }
            pb::fujin_request::Request::Hfetch(request) => {
                self.fetch(
                    request.correlation_id,
                    request.route,
                    request.auto_commit,
                    true,
                    request.batch_size,
                )
                .await
            }
            pb::fujin_request::Request::Ack(request) => self.settle(request, false).await,
            pb::fujin_request::Request::Nack(request) => self.nack(request).await,
            pb::fujin_request::Request::Unsubscribe(request) => self.unsubscribe(request).await,
        }
    }

    pub async fn next_output(&mut self) -> Option<GrpcOutput> {
        Some(self.output.pop().await)
    }

    pub fn try_next_output(&mut self) -> Option<GrpcOutput> {
        self.output.try_pop()
    }

    /// Closes Session Core and releases every pinned connector resource.
    ///
    /// # Errors
    ///
    /// Returns the aggregated Session Core cleanup failure after attempting all cleanup.
    pub async fn close(&mut self) -> CoreResult<()> {
        self.core.close().await
    }

    async fn bind(&mut self, request: pb::BindRequest) -> Result<(), Status> {
        let mut metadata: BTreeMap<_, _> = request.meta.into_iter().collect();
        let overrides: BTreeMap<_, _> = request.config_overrides.into_iter().collect();
        let result = self
            .core
            .bind(&request.connector, &mut metadata, &overrides)
            .await;
        let (error, routes) = match result {
            Ok(bound) => (None, route_capabilities(bound.routes)),
            Err(error) => (Some(operation_error(&error)), HashMap::default()),
        };
        self.send(pb::fujin_response::Response::Bind(pb::BindResponse {
            error,
            routes,
        }))
    }

    fn produce(&mut self, request: pb::ProduceRequest, with_headers: bool) -> Result<(), Status> {
        debug_assert!(!with_headers);
        self.submit_produce(
            ProduceKind::Produce,
            request.correlation_id,
            Some(request.route),
            request.message,
            None,
        )
    }

    fn hproduce(&mut self, request: pb::HProduceRequest) -> Result<(), Status> {
        self.submit_produce(
            ProduceKind::HProduce,
            request.correlation_id,
            Some(request.route),
            request.message,
            Some(headers_from_proto(request.headers)),
        )
    }

    fn tx_produce(
        &mut self,
        request: pb::TxProduceRequest,
        with_headers: bool,
    ) -> Result<(), Status> {
        debug_assert!(!with_headers);
        self.submit_produce(
            ProduceKind::TransactionProduce,
            request.correlation_id,
            None,
            request.message,
            None,
        )
    }

    fn tx_hproduce(&mut self, request: pb::TxHProduceRequest) -> Result<(), Status> {
        self.submit_produce(
            ProduceKind::TransactionHProduce,
            request.correlation_id,
            None,
            request.message,
            Some(headers_from_proto(request.headers)),
        )
    }

    fn submit_produce(
        &mut self,
        kind: ProduceKind,
        correlation_id: u32,
        route: Option<String>,
        payload: Vec<u8>,
        headers: Option<Vec<Header>>,
    ) -> Result<(), Status> {
        let token = produce_token(kind, correlation_id)?;
        let message = message_from_proto(payload, headers);
        let result = match route {
            Some(route) => self.core.produce(token, &route, message),
            None => self.core.transaction_produce(token, message),
        };
        if let Err(error) = result {
            self.send(produce_response(kind, correlation_id, Some(&error)))?;
        }
        Ok(())
    }

    async fn begin_transaction(&mut self, request: pb::BeginTxRequest) -> Result<(), Status> {
        let result = self.core.begin_transaction(&request.route).await;
        self.send(control_response(
            ControlKind::Begin,
            request.correlation_id,
            result.as_ref().err(),
        ))
    }

    async fn commit_transaction(&mut self, request: pb::CommitTxRequest) -> Result<(), Status> {
        let result = self.core.commit_transaction().await;
        self.send(control_response(
            ControlKind::Commit,
            request.correlation_id,
            result.as_ref().err(),
        ))
    }

    async fn rollback_transaction(&mut self, request: pb::RollbackTxRequest) -> Result<(), Status> {
        let result = self.core.rollback_transaction().await;
        self.send(control_response(
            ControlKind::Rollback,
            request.correlation_id,
            result.as_ref().err(),
        ))
    }

    async fn subscribe(
        &mut self,
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
    ) -> Result<(), Status> {
        let output = Arc::clone(&self.output);
        let result = self
            .core
            .subscribe(&route, auto_settle, with_headers, move |subscription_id| {
                output
                    .push(Ok(subscribe_response(
                        with_headers,
                        correlation_id,
                        Some(subscription_id),
                        None,
                    )))
                    .map_err(|_| CoreError::Closed)
            })
            .await;
        if let Err(error) = result {
            self.send_envelope(subscribe_response(
                with_headers,
                correlation_id,
                None,
                Some(&error),
            ))?;
        }
        Ok(())
    }

    async fn fetch(
        &mut self,
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
        maximum: u32,
    ) -> Result<(), Status> {
        let result = self
            .core
            .fetch(&route, auto_settle, with_headers, maximum)
            .await;
        self.send(fetch_response(with_headers, correlation_id, result))
    }

    async fn settle(&mut self, request: pb::AckRequest, nack: bool) -> Result<(), Status> {
        debug_assert!(!nack);
        let message_ids = request.message_ids.into_iter().map(Bytes::from).collect();
        let result = self
            .core
            .ack(subscription_id(request.subscription_id), message_ids)
            .await;
        self.send(ack_response(request.correlation_id, result))
    }

    async fn nack(&mut self, request: pb::NackRequest) -> Result<(), Status> {
        let message_ids = request.message_ids.into_iter().map(Bytes::from).collect();
        let result = self
            .core
            .nack(subscription_id(request.subscription_id), message_ids)
            .await;
        self.send(nack_response(request.correlation_id, result))
    }

    async fn unsubscribe(&mut self, request: pb::UnsubscribeRequest) -> Result<(), Status> {
        let result = self
            .core
            .unsubscribe(subscription_id(request.subscription_id))
            .await;
        self.send(pb::fujin_response::Response::Unsubscribe(
            pb::UnsubscribeResponse {
                correlation_id: request.correlation_id,
                error: result.err().as_ref().map(operation_error),
            },
        ))
    }

    fn send(&self, response: pb::fujin_response::Response) -> Result<(), Status> {
        self.send_envelope(pb::FujinResponse {
            response: Some(response),
        })
    }

    fn send_envelope(&self, response: pb::FujinResponse) -> Result<(), Status> {
        self.output.push(Ok(response))
    }
}

#[derive(Clone)]
pub struct GrpcService {
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
}

impl fmt::Debug for GrpcService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcService")
            .finish_non_exhaustive()
    }
}

impl GrpcService {
    #[must_use]
    pub fn new(catalog: Arc<Catalog>, bind_middlewares: Arc<dyn BindMiddlewareRunner>) -> Self {
        Self {
            catalog,
            bind_middlewares,
        }
    }
}

#[tonic::async_trait]
impl pb::fujin_service_server::FujinService for GrpcService {
    type StreamStream = ReceiverStream<GrpcOutput>;

    async fn stream(
        &self,
        request: Request<tonic::Streaming<pb::FujinRequest>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let mut session = GrpcSession::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.bind_middlewares),
        );
        let (sender, receiver) = mpsc::channel(MAXIMUM_RESPONSE_CHANNEL_MESSAGES);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    inbound_result = inbound.message() => match inbound_result {
                        Ok(Some(request)) => {
                            if let Err(error) = session.handle(request).await {
                                let _ = sender.send(Err(error)).await;
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    },
                    output = session.next_output() => {
                        let Some(output) = output else {
                            break;
                        };
                        let terminal = output.is_err();
                        if sender.send(output).await.is_err() || terminal {
                            break;
                        }
                    }
                }
            }
            if let Err(error) = session.close().await {
                let _ = sender.send(Err(Status::internal(error.to_string()))).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlKind {
    Begin,
    Commit,
    Rollback,
}

fn produce_token(kind: ProduceKind, correlation_id: u32) -> Result<OperationToken, Status> {
    OperationToken::external((u64::from(kind as u8) << 32) | u64::from(correlation_id))
        .map_err(|error| Status::internal(error.to_string()))
}

fn decode_produce_token(token: OperationToken) -> Option<(ProduceKind, u32)> {
    if token.is_internal() {
        return None;
    }
    let value = token.value();
    let kind = match u8::try_from(value >> 32).ok()? {
        1 => ProduceKind::Produce,
        2 => ProduceKind::HProduce,
        3 => ProduceKind::TransactionProduce,
        4 => ProduceKind::TransactionHProduce,
        _ => return None,
    };
    Some((kind, u32::try_from(value & u64::from(u32::MAX)).ok()?))
}

const fn subscription_id(value: u32) -> u8 {
    value.to_be_bytes()[3]
}

fn message_from_proto(payload: Vec<u8>, headers: Option<Vec<Header>>) -> Message {
    let payload = Bytes::from(payload);
    match headers {
        Some(headers) => Message::with_headers(payload, headers),
        None => Message::new(payload),
    }
}

fn headers_from_proto(headers: Vec<pb::Kv>) -> Vec<Header> {
    headers
        .into_iter()
        .map(|header| Header {
            key: Bytes::from(header.key),
            value: Bytes::from(header.value),
        })
        .collect()
}

fn headers_to_proto(headers: Vec<Header>) -> Vec<pb::Kv> {
    headers
        .into_iter()
        .map(|header| pb::Kv {
            key: header.key.to_vec(),
            value: header.value.to_vec(),
        })
        .collect()
}

fn route_capabilities(
    routes: BTreeMap<String, RouteProfile>,
) -> std::collections::HashMap<String, pb::RouteCapabilities> {
    routes
        .into_iter()
        .map(|(route, profile)| (route, route_capability(profile)))
        .collect()
}

fn route_capability(profile: RouteProfile) -> pb::RouteCapabilities {
    pb::RouteCapabilities {
        produce: profile.capabilities.contains(Capabilities::PRODUCE),
        headers: profile.capabilities.contains(Capabilities::HEADERS),
        transactions: profile.capabilities.contains(Capabilities::TRANSACTIONS),
        subscribe: profile.capabilities.contains(Capabilities::SUBSCRIBE),
        fetch: profile.capabilities.contains(Capabilities::FETCH),
        manual_settlement: profile
            .capabilities
            .contains(Capabilities::MANUAL_SETTLEMENT),
        produce_guarantee: produce_guarantee(profile.produce_guarantee),
        ack_granularity: ack_granularity(profile.settlement.ack),
        nack_effect: nack_effect(profile.settlement.nack),
    }
}

fn produce_response(
    kind: ProduceKind,
    correlation_id: u32,
    error: Option<&CoreError>,
) -> pb::fujin_response::Response {
    let error = error.map(operation_error);
    match kind {
        ProduceKind::Produce => pb::fujin_response::Response::Produce(pb::ProduceResponse {
            correlation_id,
            error,
        }),
        ProduceKind::HProduce => pb::fujin_response::Response::Hproduce(pb::HProduceResponse {
            correlation_id,
            error,
        }),
        ProduceKind::TransactionProduce => {
            pb::fujin_response::Response::TxProduce(pb::TxProduceResponse {
                correlation_id,
                error,
            })
        }
        ProduceKind::TransactionHProduce => {
            pb::fujin_response::Response::TxHproduce(pb::TxHProduceResponse {
                correlation_id,
                error,
            })
        }
    }
}

fn control_response(
    kind: ControlKind,
    correlation_id: u32,
    error: Option<&CoreError>,
) -> pb::fujin_response::Response {
    let error = error.map(operation_error);
    match kind {
        ControlKind::Begin => pb::fujin_response::Response::BeginTx(pb::BeginTxResponse {
            correlation_id,
            error,
        }),
        ControlKind::Commit => pb::fujin_response::Response::CommitTx(pb::CommitTxResponse {
            correlation_id,
            error,
        }),
        ControlKind::Rollback => pb::fujin_response::Response::RollbackTx(pb::RollbackTxResponse {
            correlation_id,
            error,
        }),
    }
}

fn subscribe_response(
    with_headers: bool,
    correlation_id: u32,
    subscription_id: Option<u8>,
    error: Option<&CoreError>,
) -> pb::FujinResponse {
    let error = error.map(operation_error);
    let subscription_id = subscription_id.map_or(0, u32::from);
    let response = if with_headers {
        pb::fujin_response::Response::Hsubscribe(pb::HSubscribeResponse {
            correlation_id,
            error,
            subscription_id,
        })
    } else {
        pb::fujin_response::Response::Subscribe(pb::SubscribeResponse {
            correlation_id,
            error,
            subscription_id,
        })
    };
    pb::FujinResponse {
        response: Some(response),
    }
}

fn delivery_response(subscription_id: u8, delivery: Delivery) -> pb::fujin_response::Response {
    match delivery.headers {
        Some(headers) => pb::fujin_response::Response::Hmessage(pb::HMessage {
            subscription_id: u32::from(subscription_id),
            message_id: delivery
                .message_id
                .map_or_else(Vec::new, |value| value.to_vec()),
            headers: headers_to_proto(headers),
            payload: delivery.payload.to_vec(),
        }),
        None => pb::fujin_response::Response::Message(pb::Message {
            subscription_id: u32::from(subscription_id),
            message_id: delivery
                .message_id
                .map_or_else(Vec::new, |value| value.to_vec()),
            payload: delivery.payload.to_vec(),
        }),
    }
}

fn fetch_response(
    with_headers: bool,
    correlation_id: u32,
    result: CoreResult<fujin_core::FetchResult>,
) -> pb::fujin_response::Response {
    match result {
        Ok(fetched) if with_headers => pb::fujin_response::Response::Hfetch(pb::HFetchResponse {
            correlation_id,
            error: None,
            subscription_id: u32::from(fetched.subscription_id),
            messages: fetched
                .messages
                .into_iter()
                .map(|message| pb::HFetchMessage {
                    message_id: message
                        .message_id
                        .map_or_else(Vec::new, |value| value.to_vec()),
                    headers: headers_to_proto(message.headers.unwrap_or_default()),
                    payload: message.payload.to_vec(),
                })
                .collect(),
        }),
        Ok(fetched) => pb::fujin_response::Response::Fetch(pb::FetchResponse {
            correlation_id,
            error: None,
            subscription_id: u32::from(fetched.subscription_id),
            messages: fetched
                .messages
                .into_iter()
                .map(|message| pb::FetchMessage {
                    message_id: message
                        .message_id
                        .map_or_else(Vec::new, |value| value.to_vec()),
                    payload: message.payload.to_vec(),
                })
                .collect(),
        }),
        Err(error) if with_headers => pb::fujin_response::Response::Hfetch(pb::HFetchResponse {
            correlation_id,
            error: Some(operation_error(&error)),
            subscription_id: 0,
            messages: Vec::new(),
        }),
        Err(error) => pb::fujin_response::Response::Fetch(pb::FetchResponse {
            correlation_id,
            error: Some(operation_error(&error)),
            subscription_id: 0,
            messages: Vec::new(),
        }),
    }
}

fn ack_response(
    correlation_id: u32,
    result: CoreResult<Vec<SettlementResult>>,
) -> pb::fujin_response::Response {
    match result {
        Ok(results) => pb::fujin_response::Response::Ack(pb::AckResponse {
            correlation_id,
            error: None,
            results: results
                .into_iter()
                .map(|result| pb::AckMessageResult {
                    message_id: result.message_id.to_vec(),
                    error: result.result.err().as_ref().map(operation_error),
                })
                .collect(),
        }),
        Err(error) => pb::fujin_response::Response::Ack(pb::AckResponse {
            correlation_id,
            error: Some(operation_error(&error)),
            results: Vec::new(),
        }),
    }
}

fn nack_response(
    correlation_id: u32,
    result: CoreResult<Vec<SettlementResult>>,
) -> pb::fujin_response::Response {
    match result {
        Ok(results) => pb::fujin_response::Response::Nack(pb::NackResponse {
            correlation_id,
            error: None,
            results: results
                .into_iter()
                .map(|result| pb::NackMessageResult {
                    message_id: result.message_id.to_vec(),
                    error: result.result.err().as_ref().map(operation_error),
                })
                .collect(),
        }),
        Err(error) => pb::fujin_response::Response::Nack(pb::NackResponse {
            correlation_id,
            error: Some(operation_error(&error)),
            results: Vec::new(),
        }),
    }
}

fn operation_error(error: &CoreError) -> pb::OperationError {
    let error = OperationError::from(error);
    pb::OperationError {
        code: status_code(error.code),
        outcome: operation_outcome(error.outcome),
        reason: error.reason,
        message: error.message,
        details: error.details.into_iter().collect(),
    }
}

const fn status_code(code: StatusCode) -> i32 {
    match code {
        StatusCode::Ok => pb::StatusCode::StatusOk as i32,
        StatusCode::Canceled => pb::StatusCode::StatusCanceled as i32,
        StatusCode::Unknown => pb::StatusCode::StatusUnknown as i32,
        StatusCode::InvalidArgument => pb::StatusCode::StatusInvalidArgument as i32,
        StatusCode::DeadlineExceeded => pb::StatusCode::StatusDeadlineExceeded as i32,
        StatusCode::NotFound => pb::StatusCode::StatusNotFound as i32,
        StatusCode::AlreadyExists => pb::StatusCode::StatusAlreadyExists as i32,
        StatusCode::PermissionDenied => pb::StatusCode::StatusPermissionDenied as i32,
        StatusCode::ResourceExhausted => pb::StatusCode::StatusResourceExhausted as i32,
        StatusCode::FailedPrecondition => pb::StatusCode::StatusFailedPrecondition as i32,
        StatusCode::Aborted => pb::StatusCode::StatusAborted as i32,
        StatusCode::OutOfRange => pb::StatusCode::StatusOutOfRange as i32,
        StatusCode::Unimplemented => pb::StatusCode::StatusUnimplemented as i32,
        StatusCode::Internal => pb::StatusCode::StatusInternal as i32,
        StatusCode::Unavailable => pb::StatusCode::StatusUnavailable as i32,
        StatusCode::DataLoss => pb::StatusCode::StatusDataLoss as i32,
        StatusCode::Unauthenticated => pb::StatusCode::StatusUnauthenticated as i32,
    }
}

const fn operation_outcome(outcome: OperationOutcome) -> i32 {
    match outcome {
        OperationOutcome::Unspecified => pb::OperationOutcome::OutcomeUnspecified as i32,
        OperationOutcome::NotApplied => pb::OperationOutcome::OutcomeNotApplied as i32,
        OperationOutcome::Applied => pb::OperationOutcome::OutcomeApplied as i32,
        OperationOutcome::Unknown => pb::OperationOutcome::OutcomeUnknown as i32,
    }
}

const fn produce_guarantee(guarantee: AcceptanceGuarantee) -> i32 {
    match guarantee {
        AcceptanceGuarantee::Unspecified => pb::ProduceGuarantee::Unspecified as i32,
        AcceptanceGuarantee::Local => pb::ProduceGuarantee::LocalAccept as i32,
        AcceptanceGuarantee::Peer => pb::ProduceGuarantee::PeerAccept as i32,
        AcceptanceGuarantee::Durable => pb::ProduceGuarantee::DurableAccept as i32,
    }
}

const fn ack_granularity(granularity: AckGranularity) -> i32 {
    match granularity {
        AckGranularity::Unsupported => pb::AckGranularity::Unsupported as i32,
        AckGranularity::Single => pb::AckGranularity::Single as i32,
        AckGranularity::Cumulative => pb::AckGranularity::Cumulative as i32,
    }
}

const fn nack_effect(effect: NackEffect) -> i32 {
    match effect {
        NackEffect::Unsupported => pb::NackEffect::Unsupported as i32,
        NackEffect::Requeue => pb::NackEffect::Requeue as i32,
        NackEffect::Release => pb::NackEffect::Release as i32,
        NackEffect::Drop => pb::NackEffect::Drop as i32,
    }
}
