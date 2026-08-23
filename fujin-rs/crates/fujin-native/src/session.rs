use std::{
    collections::VecDeque,
    fmt,
    future::{Future, pending},
    io::{self, IoSlice},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use fujin_connector::{
    Catalog, Completion, CompletionSink, Delivery, Header, Message, OperationToken,
};
use fujin_core::{SessionCore, SessionEventSink, SessionState};
use fujin_error::{CoreError, Result as CoreResult, StatusCode};
use fujin_middleware::BindMiddlewareRunner;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Notify,
};

use crate::{Decoder, HELLO_FORMAT, NativeError, Request, ResponseCode, WIRE_VERSION, encode};

const MAXIMUM_PENDING_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const INITIAL_INPUT_BUFFER_BYTES: usize = 512 * 1024;
const MAXIMUM_RETAINED_INPUT_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const MAXIMUM_VECTORED_OUTPUT_SLICES: usize = 64;
const INLINE_OUTPUT_BATCH_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionAction {
    Continue,
    Disconnect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionOutput {
    Frame(Bytes),
    InlineOperation([u8; 6]),
    Terminal(CoreError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    ExpectHello,
    ExpectBind,
    Active,
    Closed,
}

enum RequestDispatch {
    Handled(SessionAction),
    Deferred(Request),
}

#[derive(Debug, Default)]
struct OutputState {
    queue: VecDeque<SessionOutput>,
    pending_bytes: usize,
    overflowed: bool,
}

#[derive(Debug)]
struct OutputQueue {
    state: Mutex<OutputState>,
    ready: Notify,
}

impl OutputQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OutputState {
                queue: VecDeque::with_capacity(16),
                ..OutputState::default()
            }),
            ready: Notify::new(),
        })
    }

    fn push(&self, output: SessionOutput) {
        let size = output.wire_len();
        let mut state = self.state.lock();
        if state.overflowed {
            return;
        }
        let notify = state.queue.is_empty();
        let Some(next_bytes) = state.pending_bytes.checked_add(size) else {
            state.overflowed = true;
            state.queue.push_back(output_exhausted());
            drop(state);
            if notify {
                self.ready.notify_one();
            }
            return;
        };
        if next_bytes > MAXIMUM_PENDING_OUTPUT_BYTES {
            state.overflowed = true;
            state.queue.push_back(output_exhausted());
        } else {
            state.pending_bytes = next_bytes;
            state.queue.push_back(output);
        }
        drop(state);
        if notify {
            self.ready.notify_one();
        }
    }

    fn try_pop(&self) -> Option<SessionOutput> {
        let mut state = self.state.lock();
        let output = state.queue.pop_front()?;
        state.pending_bytes = state.pending_bytes.saturating_sub(output.wire_len());
        Some(output)
    }

    fn drain_into(&self, output: &mut OutputBatch) -> Result<(), NativeError> {
        let mut state = self.state.lock();
        while let Some(next) = state.queue.pop_front() {
            state.pending_bytes = state.pending_bytes.saturating_sub(next.wire_len());
            output.push(next)?;
        }
        Ok(())
    }

    async fn pop(&self) -> SessionOutput {
        loop {
            let notified = self.ready.notified();
            if let Some(output) = self.try_pop() {
                return output;
            }
            notified.await;
        }
    }
}

impl SessionOutput {
    fn wire_len(&self) -> usize {
        match self {
            Self::Frame(frame) => frame.len(),
            Self::InlineOperation(frame) => frame.len(),
            Self::Terminal(_) => 0,
        }
    }
}

fn output_exhausted() -> SessionOutput {
    SessionOutput::Terminal(CoreError::ResourceExhausted(
        "native session output exceeded 4 MiB".into(),
    ))
}

#[derive(Debug)]
struct AdapterSink {
    output: Arc<OutputQueue>,
}

impl AdapterSink {
    fn send(&self, output: SessionOutput) {
        self.output.push(output);
    }

    fn send_frame(&self, frame: Result<Bytes, NativeError>) {
        match frame {
            Ok(frame) => self.send(SessionOutput::Frame(frame)),
            Err(error) => self.send(SessionOutput::Terminal(CoreError::Internal(
                error.to_string(),
            ))),
        }
    }
}

impl CompletionSink for AdapterSink {
    fn complete(&self, completion: Completion) {
        let Some((response_code, correlation_id)) = decode_operation_token(completion.token) else {
            self.send(SessionOutput::Terminal(CoreError::Internal(
                "invalid native completion token".into(),
            )));
            return;
        };
        if completion.result.is_ok() {
            let mut frame = [0; 6];
            frame[0] = response_code as u8;
            frame[1..5].copy_from_slice(&correlation_id.to_be_bytes());
            self.send(SessionOutput::InlineOperation(frame));
        } else {
            self.send_frame(encode::operation(
                response_code,
                correlation_id,
                &completion.result,
            ));
        }
    }
}

impl SessionEventSink for AdapterSink {
    fn delivery(&self, subscription_id: u8, delivery: Delivery) {
        self.send_frame(encode::delivery(subscription_id, &delivery));
    }

    fn subscription_terminal(&self, _subscription_id: u8, error: CoreError) {
        self.send(SessionOutput::Terminal(error));
    }
}

pub struct NativeSession {
    core: SessionCore,
    output: Arc<OutputQueue>,
    phase: Phase,
    server_build: String,
}

impl fmt::Debug for NativeSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeSession")
            .field("core", &self.core)
            .field("phase", &self.phase)
            .field("server_build", &self.server_build)
            .finish_non_exhaustive()
    }
}

impl NativeSession {
    #[must_use]
    pub fn new(
        catalog: Arc<Catalog>,
        bind_middlewares: Arc<dyn BindMiddlewareRunner>,
        server_build: impl Into<String>,
    ) -> Self {
        let output = OutputQueue::new();
        let sink = Arc::new(AdapterSink {
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
            phase: Phase::ExpectHello,
            server_build: server_build.into(),
        }
    }

    /// Applies one decoded native request to the owned Session Core.
    ///
    /// Responses and asynchronous deliveries are available through [`Self::next_output`].
    ///
    /// # Errors
    ///
    /// Returns [`NativeError::Malformed`] when the opcode is illegal for the current native
    /// protocol phase, or a native encoding/session error when a response cannot be produced.
    pub async fn handle(&mut self, request: Request) -> Result<SessionAction, NativeError> {
        match self.phase {
            Phase::ExpectHello => self.handle_hello(request),
            Phase::ExpectBind => self.handle_unbound(request).await,
            Phase::Active => self.handle_active(request).await,
            Phase::Closed => Err(NativeError::Malformed("session is closed")),
        }
    }

    fn dispatch_immediate(&mut self, request: Request) -> Result<RequestDispatch, NativeError> {
        match request {
            Request::Produce {
                correlation_id,
                route,
                message,
                headers,
            } if self.phase == Phase::Active && self.core.state() == SessionState::Connected => {
                let route = std::str::from_utf8(&route)
                    .map_err(|_| NativeError::Malformed("route is not UTF-8"))?;
                self.handle_produce(correlation_id, route, message, headers)?;
                Ok(RequestDispatch::Handled(SessionAction::Continue))
            }
            request => Ok(RequestDispatch::Deferred(request)),
        }
    }

    pub async fn next_output(&mut self) -> Option<SessionOutput> {
        Some(self.output.pop().await)
    }

    pub fn try_next_output(&mut self) -> Option<SessionOutput> {
        self.output.try_pop()
    }

    /// Closes Session Core and releases every pinned connector resource.
    ///
    /// # Errors
    ///
    /// Returns the aggregated Session Core cleanup failure after attempting all cleanup.
    pub async fn close(&mut self) -> CoreResult<()> {
        self.phase = Phase::Closed;
        self.core.close().await
    }

    fn handle_hello(&mut self, request: Request) -> Result<SessionAction, NativeError> {
        let Request::Hello(hello) = request else {
            return Err(NativeError::Malformed("HELLO must be the first frame"));
        };
        if hello.format != HELLO_FORMAT {
            self.send_frame(encode::hello_failure(
                StatusCode::Unimplemented,
                "HELLO_FORMAT_UNSUPPORTED",
                "unsupported HELLO format",
            ))?;
            self.phase = Phase::Closed;
            return Ok(SessionAction::Disconnect);
        }
        if hello.versions.is_empty()
            || hello.versions.len() > 16
            || hello.versions.contains(&0)
            || hello.client_name.is_empty()
            || hello.client_name.len() > 256
            || hello.client_build.is_empty()
            || hello.client_build.len() > 256
        {
            self.send_frame(encode::hello_failure(
                StatusCode::InvalidArgument,
                "INVALID_HELLO",
                "invalid HELLO request",
            ))?;
            self.phase = Phase::Closed;
            return Ok(SessionAction::Disconnect);
        }
        if !hello.versions.contains(&WIRE_VERSION) {
            self.send_frame(encode::hello_failure(
                StatusCode::Unimplemented,
                "PROTOCOL_VERSION_UNSUPPORTED",
                "no mutually supported protocol version",
            ))?;
            self.phase = Phase::Closed;
            return Ok(SessionAction::Disconnect);
        }
        self.send_frame(encode::hello_success(&self.server_build))?;
        self.phase = Phase::ExpectBind;
        Ok(SessionAction::Continue)
    }

    async fn handle_unbound(&mut self, request: Request) -> Result<SessionAction, NativeError> {
        match request {
            Request::Bind {
                connector,
                mut metadata,
                overrides,
            } => {
                let result = self.core.bind(&connector, &mut metadata, &overrides).await;
                if result.is_ok() {
                    self.phase = Phase::Active;
                }
                self.send_frame(encode::bind(&result))?;
                Ok(SessionAction::Continue)
            }
            Request::Subscribe {
                correlation_id,
                route,
                auto_settle,
                with_headers: false,
            } => {
                self.subscribe(correlation_id, route, auto_settle, false)
                    .await?;
                Ok(SessionAction::Continue)
            }
            Request::Pong => Ok(SessionAction::Continue),
            _ => Err(NativeError::Malformed("BIND required before operation")),
        }
    }

    async fn handle_active(&mut self, request: Request) -> Result<SessionAction, NativeError> {
        if !request_allowed(self.core.state(), &request) {
            return Err(NativeError::Malformed(
                "opcode is invalid for session state",
            ));
        }
        match request {
            Request::Produce {
                correlation_id,
                route,
                message,
                headers,
            } => {
                let route = std::str::from_utf8(&route)
                    .map_err(|_| NativeError::Malformed("route is not UTF-8"))?;
                self.handle_produce(correlation_id, route, message, headers)
            }
            Request::TransactionProduce {
                correlation_id,
                message,
                headers,
            } => self.handle_transaction_produce(correlation_id, message, headers),
            Request::BeginTransaction {
                correlation_id,
                route,
            } => {
                let result = self.core.begin_transaction(&route).await;
                self.respond_operation(ResponseCode::BeginTransaction, correlation_id, &result)
            }
            Request::CommitTransaction { correlation_id } => {
                let result = self.core.commit_transaction().await;
                self.respond_operation(ResponseCode::CommitTransaction, correlation_id, &result)
            }
            Request::RollbackTransaction { correlation_id } => {
                let result = self.core.rollback_transaction().await;
                self.respond_operation(ResponseCode::RollbackTransaction, correlation_id, &result)
            }
            Request::Subscribe {
                correlation_id,
                route,
                auto_settle,
                with_headers,
            } => {
                self.subscribe(correlation_id, route, auto_settle, with_headers)
                    .await?;
                Ok(SessionAction::Continue)
            }
            Request::Fetch {
                correlation_id,
                route,
                auto_settle,
                with_headers,
                maximum,
            } => {
                self.handle_fetch(correlation_id, route, auto_settle, with_headers, maximum)
                    .await
            }
            Request::Ack {
                correlation_id,
                subscription_id,
                message_ids,
            } => {
                self.handle_settlement(correlation_id, subscription_id, message_ids, false)
                    .await
            }
            Request::Nack {
                correlation_id,
                subscription_id,
                message_ids,
            } => {
                self.handle_settlement(correlation_id, subscription_id, message_ids, true)
                    .await
            }
            Request::Unsubscribe {
                correlation_id,
                subscription_id,
            } => {
                let result = self.core.unsubscribe(subscription_id).await;
                self.respond_operation(ResponseCode::Unsubscribe, correlation_id, &result)
            }
            Request::Disconnect => {
                let _ = self.close().await;
                self.send_frame(Ok(encode::disconnect()))?;
                Ok(SessionAction::Disconnect)
            }
            Request::Pong => Ok(SessionAction::Continue),
            Request::Hello(_) | Request::Bind { .. } => {
                Err(NativeError::Malformed("opcode is invalid after BIND"))
            }
        }
    }

    fn handle_produce(
        &mut self,
        correlation_id: u32,
        route: &str,
        message: Bytes,
        headers: Option<Vec<Header>>,
    ) -> Result<SessionAction, NativeError> {
        let code = response_code(false, headers.is_some());
        let token = operation_token(code, correlation_id)?;
        if let Err(error) = self
            .core
            .produce(token, route, message_from(message, headers))
        {
            self.send_frame(encode::operation(code, correlation_id, &Err(error)))?;
        }
        Ok(SessionAction::Continue)
    }

    fn handle_transaction_produce(
        &mut self,
        correlation_id: u32,
        message: Bytes,
        headers: Option<Vec<Header>>,
    ) -> Result<SessionAction, NativeError> {
        let code = response_code(true, headers.is_some());
        let token = operation_token(code, correlation_id)?;
        if let Err(error) = self
            .core
            .transaction_produce(token, message_from(message, headers))
        {
            self.send_frame(encode::operation(code, correlation_id, &Err(error)))?;
        }
        Ok(SessionAction::Continue)
    }

    async fn handle_fetch(
        &mut self,
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
        maximum: u32,
    ) -> Result<SessionAction, NativeError> {
        let code = if with_headers {
            ResponseCode::HFetch
        } else {
            ResponseCode::Fetch
        };
        let result = self
            .core
            .fetch(&route, auto_settle, with_headers, maximum)
            .await;
        self.send_frame(encode::fetch(code, correlation_id, &result))?;
        Ok(SessionAction::Continue)
    }

    async fn handle_settlement(
        &mut self,
        correlation_id: u32,
        subscription_id: u8,
        message_ids: Vec<Bytes>,
        nack: bool,
    ) -> Result<SessionAction, NativeError> {
        let (code, result) = if nack {
            (
                ResponseCode::Nack,
                self.core.nack(subscription_id, message_ids).await,
            )
        } else {
            (
                ResponseCode::Ack,
                self.core.ack(subscription_id, message_ids).await,
            )
        };
        self.send_frame(encode::settlement(code, correlation_id, &result))?;
        Ok(SessionAction::Continue)
    }

    fn respond_operation(
        &self,
        code: ResponseCode,
        correlation_id: u32,
        result: &CoreResult<()>,
    ) -> Result<SessionAction, NativeError> {
        self.send_frame(encode::operation(code, correlation_id, result))?;
        Ok(SessionAction::Continue)
    }

    async fn subscribe(
        &mut self,
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
    ) -> Result<(), NativeError> {
        let code = if with_headers {
            ResponseCode::HSubscribe
        } else {
            ResponseCode::Subscribe
        };
        let output = Arc::clone(&self.output);
        let result = self
            .core
            .subscribe(&route, auto_settle, with_headers, move |subscription_id| {
                let frame = encode::subscribe(code, correlation_id, &Ok(subscription_id))
                    .map_err(|error| CoreError::Internal(error.to_string()))?;
                output.push(SessionOutput::Frame(frame));
                Ok(())
            })
            .await;
        if let Err(error) = result {
            self.send_frame(encode::subscribe(code, correlation_id, &Err(error)))?;
        }
        Ok(())
    }

    fn send_frame(&self, frame: Result<Bytes, NativeError>) -> Result<(), NativeError> {
        self.output.push(SessionOutput::Frame(frame?));
        Ok(())
    }
}

fn message_from(payload: Bytes, headers: Option<Vec<Header>>) -> Message {
    match headers {
        Some(values) => Message::with_headers(payload, values),
        None => Message::new(payload),
    }
}

fn response_code(transactional: bool, with_headers: bool) -> ResponseCode {
    match (transactional, with_headers) {
        (false, false) => ResponseCode::Produce,
        (false, true) => ResponseCode::HProduce,
        (true, false) => ResponseCode::TransactionProduce,
        (true, true) => ResponseCode::TransactionHProduce,
    }
}

fn operation_token(code: ResponseCode, correlation_id: u32) -> Result<OperationToken, NativeError> {
    OperationToken::external((u64::from(code as u8) << 32) | u64::from(correlation_id))
        .map_err(NativeError::Session)
}

fn decode_operation_token(token: OperationToken) -> Option<(ResponseCode, u32)> {
    if token.is_internal() {
        return None;
    }
    let value = token.value();
    let code = u8::try_from(value >> 32).ok()?;
    let correlation_id = u32::try_from(value & u64::from(u32::MAX)).ok()?;
    response_code_from_byte(code).map(|response_code| (response_code, correlation_id))
}

const fn response_code_from_byte(code: u8) -> Option<ResponseCode> {
    match code {
        3 => Some(ResponseCode::Produce),
        4 => Some(ResponseCode::HProduce),
        17 => Some(ResponseCode::TransactionProduce),
        18 => Some(ResponseCode::TransactionHProduce),
        _ => None,
    }
}

fn request_allowed(state: SessionState, request: &Request) -> bool {
    match state {
        SessionState::Connected => matches!(
            request,
            Request::Produce { .. }
                | Request::BeginTransaction { .. }
                | Request::CommitTransaction { .. }
                | Request::RollbackTransaction { .. }
                | Request::Subscribe { .. }
                | Request::Fetch { .. }
                | Request::Ack { .. }
                | Request::Nack { .. }
                | Request::Unsubscribe { .. }
                | Request::Disconnect
                | Request::Pong
        ),
        SessionState::InTransaction => matches!(
            request,
            Request::TransactionProduce { .. }
                | Request::BeginTransaction { .. }
                | Request::CommitTransaction { .. }
                | Request::RollbackTransaction { .. }
                | Request::Subscribe { .. }
                | Request::Fetch { .. }
                | Request::Ack { .. }
                | Request::Nack { .. }
                | Request::Unsubscribe { .. }
                | Request::Disconnect
                | Request::Pong
        ),
        SessionState::Unbound | SessionState::Closed => false,
    }
}

/// Runs one native protocol session over an arbitrary asynchronous byte stream.
///
/// # Errors
///
/// Returns a framing, I/O, terminal subscription, or Session Core cleanup error. The function
/// always attempts Session Core cleanup before returning.
pub async fn run<S>(
    stream: S,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    server_build: impl Into<String>,
) -> Result<(), NativeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    run_with_shutdown(stream, catalog, bind_middlewares, server_build, pending()).await
}

/// Runs one native session and initiates bounded Session Core cleanup when shutdown resolves.
///
/// # Errors
///
/// Returns a framing, I/O, terminal subscription, or Session Core cleanup error.
pub async fn run_with_shutdown<S, F>(
    stream: S,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
    server_build: impl Into<String>,
    shutdown: F,
) -> Result<(), NativeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: Future<Output = ()>,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut session = NativeSession::new(catalog, bind_middlewares, server_build);
    let mut decoder = Decoder::default();
    let mut input = BytesMut::with_capacity(INITIAL_INPUT_BUFFER_BYTES);
    let mut output = OutputBatch::default();
    tokio::pin!(shutdown);
    let result = tokio::select! {
        result = run_loop(
            &mut reader,
            &mut writer,
            &mut session,
            &mut decoder,
            &mut input,
            &mut output,
        ) => result,
        () = &mut shutdown => {
            writer.shutdown().await.map_err(NativeError::from)
        },
    };
    let cleanup = session.close().await;
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(NativeError::Session(error)),
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[derive(Debug)]
enum BatchedOutput {
    Frame(Bytes),
    Inline(BytesMut),
}

impl BatchedOutput {
    fn wire_len(&self) -> usize {
        match self {
            Self::Frame(bytes) => bytes.len(),
            Self::Inline(bytes) => bytes.len(),
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::Frame(bytes) => bytes.as_ref(),
            Self::Inline(bytes) => bytes.as_ref(),
        }
    }
}

#[derive(Debug, Default)]
struct OutputBatch {
    queue: VecDeque<BatchedOutput>,
    offset: usize,
    inline_spare: Option<BytesMut>,
}

impl OutputBatch {
    fn push(&mut self, output: SessionOutput) -> Result<(), NativeError> {
        match output {
            SessionOutput::Terminal(error) => Err(NativeError::Session(error)),
            SessionOutput::Frame(frame) => {
                self.queue.push_back(BatchedOutput::Frame(frame));
                Ok(())
            }
            SessionOutput::InlineOperation(frame) => {
                if let Some(BatchedOutput::Inline(batch)) = self.queue.back_mut()
                    && batch.len() + frame.len() <= INLINE_OUTPUT_BATCH_BYTES
                {
                    batch.extend_from_slice(&frame);
                    return Ok(());
                }
                let mut batch = self
                    .inline_spare
                    .take()
                    .unwrap_or_else(|| BytesMut::with_capacity(INLINE_OUTPUT_BATCH_BYTES));
                batch.extend_from_slice(&frame);
                self.queue.push_back(BatchedOutput::Inline(batch));
                Ok(())
            }
        }
    }

    fn advance(&mut self, mut written: usize) {
        while written > 0 {
            let Some(front) = self.queue.front() else {
                self.offset = 0;
                return;
            };
            let remaining = front.wire_len() - self.offset;
            if written < remaining {
                self.offset += written;
                return;
            }
            written -= remaining;
            if let Some(BatchedOutput::Inline(mut batch)) = self.queue.pop_front() {
                batch.clear();
                self.inline_spare = Some(batch);
            }
            self.offset = 0;
        }
    }
}

async fn run_loop<R, W>(
    reader: &mut R,
    writer: &mut W,
    session: &mut NativeSession,
    decoder: &mut Decoder,
    input: &mut BytesMut,
    output: &mut OutputBatch,
) -> Result<(), NativeError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        while let Some(request) = decoder.decode(input)? {
            let action = match session.dispatch_immediate(request)? {
                RequestDispatch::Handled(action) => action,
                RequestDispatch::Deferred(request) => session.handle(request).await?,
            };
            if action == SessionAction::Disconnect {
                drain_outputs(session, output)?;
                flush_outputs(writer, output).await?;
                writer.shutdown().await?;
                return Ok(());
            }
        }
        drain_outputs(session, output)?;
        prepare_input(input);
        flush_outputs(writer, output).await?;
        tokio::select! {
            read = reader.read_buf(input) => {
                if read? == 0 {
                    return Ok(());
                }
            }
            next = session.next_output() => {
                let Some(next) = next else {
                    return Err(NativeError::OutputClosed);
                };
                output.push(next)?;
                drain_outputs(session, output)?;
                flush_outputs(writer, output).await?;
            }
        }
    }
}

fn prepare_input(input: &mut BytesMut) {
    if input.is_empty() && input.capacity() > MAXIMUM_RETAINED_INPUT_BUFFER_BYTES {
        *input = BytesMut::with_capacity(INITIAL_INPUT_BUFFER_BYTES);
    } else if input.capacity().saturating_sub(input.len()) < INITIAL_INPUT_BUFFER_BYTES {
        input.reserve(INITIAL_INPUT_BUFFER_BYTES);
    }
}

fn drain_outputs(session: &mut NativeSession, output: &mut OutputBatch) -> Result<(), NativeError> {
    session.output.drain_into(output)
}

async fn flush_outputs<W>(writer: &mut W, output: &mut OutputBatch) -> Result<(), NativeError>
where
    W: AsyncWrite + Unpin,
{
    while !output.queue.is_empty() {
        let written = {
            let empty = IoSlice::new(&[]);
            let mut slices = [empty; MAXIMUM_VECTORED_OUTPUT_SLICES];
            let mut count = 0;
            for (index, frame) in output.queue.iter().enumerate() {
                if count == slices.len() {
                    break;
                }
                let offset = if index == 0 { output.offset } else { 0 };
                slices[count] = IoSlice::new(&frame.bytes()[offset..]);
                count += 1;
            }
            writer.write_vectored(&slices[..count]).await?
        };
        if written == 0 {
            return Err(NativeError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write native output",
            )));
        }
        output.advance(written);
    }
    Ok(())
}

#[cfg(test)]
mod output_tests {
    use super::*;

    #[test]
    fn output_queue_terminates_without_exceeding_byte_limit() {
        let queue = OutputQueue::new();
        queue.push(SessionOutput::Frame(Bytes::from(
            vec![0; MAXIMUM_PENDING_OUTPUT_BYTES],
        )));
        queue.push(SessionOutput::InlineOperation([0; 6]));

        assert_eq!(
            queue.try_pop().expect("queued frame").wire_len(),
            MAXIMUM_PENDING_OUTPUT_BYTES
        );
        assert!(matches!(
            queue.try_pop(),
            Some(SessionOutput::Terminal(CoreError::ResourceExhausted(_)))
        ));
        assert!(queue.try_pop().is_none());
    }

    #[test]
    fn oversized_empty_input_returns_to_initial_capacity() {
        let mut input = BytesMut::with_capacity(MAXIMUM_RETAINED_INPUT_BUFFER_BYTES * 2);
        prepare_input(&mut input);

        assert_eq!(input.capacity(), INITIAL_INPUT_BUFFER_BYTES);
    }

    #[test]
    fn consumed_input_reclaims_capacity_before_the_next_read() {
        let mut input = BytesMut::with_capacity(1024 * 1024);
        input.extend_from_slice(&vec![0; 1024 * 1024]);
        let retained = input.split().freeze();
        drop(retained);

        prepare_input(&mut input);

        assert!(input.capacity() >= INITIAL_INPUT_BUFFER_BYTES);
    }

    #[test]
    fn output_batch_tracks_partial_write_across_frame_boundaries() {
        let mut output = OutputBatch::default();
        output
            .push(SessionOutput::Frame(Bytes::from_static(b"abc")))
            .expect("queue first frame");
        output
            .push(SessionOutput::InlineOperation(*b"defghi"))
            .expect("queue second frame");

        output.advance(4);

        assert_eq!(output.queue.len(), 1);
        assert_eq!(output.offset, 1);
    }

    #[tokio::test]
    async fn vectored_flush_preserves_bytes_across_partial_writes() {
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let mut output = OutputBatch::default();
        output
            .push(SessionOutput::Frame(Bytes::from_static(b"abc")))
            .expect("queue first frame");
        output
            .push(SessionOutput::InlineOperation(*b"defghi"))
            .expect("queue second frame");
        let read = tokio::spawn(async move {
            let mut bytes = [0; 9];
            reader
                .read_exact(&mut bytes)
                .await
                .expect("read flushed output");
            bytes
        });

        flush_outputs(&mut writer, &mut output)
            .await
            .expect("flush output");

        assert_eq!(read.await.expect("join reader"), *b"abcdefghi");
        assert!(output.queue.is_empty());
        assert_eq!(output.offset, 0);
    }
}
