use std::{
    env, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use fujin_core::{AcceptanceGuarantee, NoBindMiddleware};
use fujin_native::{RequestCode, ResponseCode};
use fujin_server::{
    NativeWebSocketStream,
    bench_support::{
        BenchmarkOperation, SubscribeGate, benchmark_subscription_plan, session_bench_catalog,
        validate_benchmark_shape,
    },
};
use quinn::{Connection, Endpoint};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::RootCertStore;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{Barrier, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "bench-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
#[cfg(feature = "bench-alloc")]
use std::alloc::System;

#[cfg(feature = "bench-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const TCP_ADDRESS: &str = "127.0.0.1:4850";
const QUIC_ADDRESS: &str = "127.0.0.1:4848";
const WEBSOCKET_ADDRESS: &str = "127.0.0.1:4851";
const UNIX_PATH: &str = "/tmp/fujin-rust-bench.sock";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Tcp,
    Quic,
    Unix,
    WebSocket,
}

impl Transport {
    fn from_env() -> Result<Self> {
        match env::var("FUJIN_BENCH_TRANSPORT") {
            Ok(value) => match value.as_str() {
                "tcp" => Ok(Self::Tcp),
                "quic" => Ok(Self::Quic),
                "unix" => Ok(Self::Unix),
                "websocket" => Ok(Self::WebSocket),
                _ => bail!("invalid FUJIN_BENCH_TRANSPORT={value:?}"),
            },
            Err(env::VarError::NotPresent) => Ok(Self::Tcp),
            Err(error) => Err(error.into()),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Quic => "quic",
            Self::Unix => "unix",
            Self::WebSocket => "websocket",
        }
    }
}

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    operation: BenchmarkOperation,
    transport: Transport,
    payload: usize,
    payload_label: String,
    batch: usize,
    concurrency: usize,
    operations: usize,
    deadline: Duration,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        let operation = env::var("FUJIN_BENCH_OPERATION")
            .unwrap_or_else(|_| "produce".into())
            .parse::<BenchmarkOperation>()
            .map_err(anyhow::Error::msg)?;
        let payload_label = env::var("FUJIN_BENCH_PAYLOAD").unwrap_or_else(|_| "128B".into());
        let payload = parse_size(&payload_label)?;
        let batch = parse_positive("FUJIN_BENCH_BATCH", 1)?;
        let concurrency = parse_positive("FUJIN_BENCH_CONCURRENCY", 1)?;
        let operations = parse_positive("FUJIN_BENCH_OPERATIONS", 10_000)?;
        validate_benchmark_shape(operation, payload, batch, concurrency, operations)
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            operation,
            transport: Transport::from_env()?,
            payload,
            payload_label,
            batch,
            concurrency,
            operations,
            deadline: parse_duration("FUJIN_BENCH_DEADLINE", Duration::from_secs(30))?,
        })
    }
}

#[derive(Debug)]
struct BenchmarkResult {
    elapsed: Duration,
    latencies: Vec<u64>,
    bytes_per_operation: Option<u128>,
    allocations_hundredths_per_operation: Option<u128>,
}

impl BenchmarkResult {
    fn report(&mut self, config: &BenchmarkConfig) {
        self.latencies.sort_unstable();
        let p99_index = (99 * self.latencies.len()).div_ceil(100) - 1;
        let p99 = self.latencies[p99_index];
        let operations = u128::try_from(config.operations).expect("operation count fits u128");
        let ns_per_operation = self.elapsed.as_nanos() / operations;
        let measured_bytes = config
            .payload
            .checked_mul(if config.operation.uses_batch() {
                config.batch
            } else {
                1
            })
            .expect("benchmark byte count");
        let megabytes_per_second =
            f64::from(u32::try_from(measured_bytes).expect("measured bytes fit benchmark metric"))
                * f64::from(
                    u32::try_from(config.operations)
                        .expect("operation count fits benchmark metric"),
                )
                / self.elapsed.as_secs_f64()
                / 1_000_000.0;
        println!(
            "rust/native/{operation} transport={transport} payload={payload} batch={batch} concurrency={concurrency} operations={operations} ns/op={ns_per_operation} MB/s={megabytes_per_second:.2} p99-ns={p99} B/op={bytes} allocs/op={allocations}",
            operation = config.operation.label(),
            transport = config.transport.label(),
            payload = config.payload_label,
            batch = config.batch,
            concurrency = config.concurrency,
            operations = config.operations,
            bytes = format_optional(self.bytes_per_operation),
            allocations = format_hundredths(self.allocations_hundredths_per_operation),
        );
    }
}

fn format_optional(value: Option<u128>) -> String {
    value.map_or_else(|| "n/a".into(), |value| value.to_string())
}

fn format_hundredths(value: Option<u128>) -> String {
    value.map_or_else(
        || "n/a".into(),
        |value| format!("{}.{:02}", value / 100, value % 100),
    )
}

fn parse_positive(name: &str, default: usize) -> Result<usize> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("invalid {name}={value:?}"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        bail!("{name} must be positive");
    }
    Ok(value)
}

fn parse_duration(name: &str, default: Duration) -> Result<Duration> {
    match env::var(name) {
        Ok(value) => {
            let value = value
                .strip_suffix('s')
                .context("FUJIN_BENCH_DEADLINE must use an s suffix")?;
            Ok(Duration::from_secs(value.parse::<u64>()?))
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn parse_size(value: &str) -> Result<usize> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("MiB") {
        (number, 1024 * 1024)
    } else if let Some(number) = value.strip_suffix("KiB") {
        (number, 1024)
    } else if let Some(number) = value.strip_suffix('B') {
        (number, 1)
    } else {
        bail!("invalid payload size {value:?}");
    };
    number
        .parse::<usize>()?
        .checked_mul(multiplier)
        .context("payload size overflow")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let mut result = timeout(config.deadline, run_benchmark(&config))
        .await
        .context("Rust native matrix benchmark deadline exceeded")??;
    result.report(&config);
    Ok(())
}

async fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkResult> {
    let worker_operations: Vec<_> = (0..config.concurrency)
        .map(|worker| operation_count(worker, config.concurrency, config.operations))
        .collect();
    let (subscribe_limits, subscribe_permits) =
        benchmark_subscription_plan(config.operation, &worker_operations);
    let subscribe_gate = Arc::new(SubscribeGate::default());
    let catalog = session_bench_catalog(
        config.payload,
        subscribe_limits,
        subscribe_permits.clone(),
        Arc::clone(&subscribe_gate),
    )
    .await?;
    let server = ServerHandle::start(config.transport, Arc::clone(&catalog)).await?;
    let quic = if config.transport == Transport::Quic {
        Some(Arc::new(connect_quic(server.quic_certificate()?).await?))
    } else {
        None
    };
    let prepared = prepare_native_workers(
        config,
        quic.as_deref(),
        worker_operations,
        &subscribe_permits,
    )
    .await?;
    let start = Arc::new(Barrier::new(config.concurrency + 1));
    let finish = Arc::new(Barrier::new(config.concurrency + 1));
    let (ready_sender, mut ready_receiver) = mpsc::channel(config.concurrency);
    let (done_sender, mut done_receiver) = mpsc::channel(config.concurrency);
    let mut workers = JoinSet::new();

    for (operation, operations) in prepared {
        workers.spawn(run_worker(WorkerPlan {
            operation,
            operations,
            start: Arc::clone(&start),
            finish: Arc::clone(&finish),
            ready: ready_sender.clone(),
            done: done_sender.clone(),
        }));
    }
    drop(ready_sender);
    drop(done_sender);
    wait_for_worker_signals(
        &mut ready_receiver,
        &mut workers,
        config.concurrency,
        "before benchmark start",
    )
    .await?;

    #[cfg(feature = "bench-alloc")]
    let allocation_region = Region::new(GLOBAL);
    let started = Instant::now();
    start.wait().await;
    subscribe_gate.start();
    wait_for_worker_signals(
        &mut done_receiver,
        &mut workers,
        config.concurrency,
        "during benchmark",
    )
    .await?;
    let elapsed = started.elapsed();
    #[cfg(feature = "bench-alloc")]
    let allocation_stats = allocation_region.change();
    finish.wait().await;

    let mut latencies = Vec::with_capacity(config.operations);
    while let Some(result) = workers.join_next().await {
        latencies.extend(result.context("native matrix worker panicked")??);
    }
    drop(quic);
    server.stop().await?;
    catalog.close().await?;
    if latencies.len() != config.operations {
        bail!(
            "recorded {} operations, expected {}",
            latencies.len(),
            config.operations
        );
    }

    #[cfg(feature = "bench-alloc")]
    let (bytes_per_operation, allocations_hundredths_per_operation) = {
        let operations = u128::try_from(config.operations).expect("operation count fits u128");
        let allocations = allocation_stats.allocations + allocation_stats.reallocations;
        (
            Some(u128::try_from(allocation_stats.bytes_allocated)? / operations),
            Some(u128::try_from(allocations)? * 100 / operations),
        )
    };
    #[cfg(not(feature = "bench-alloc"))]
    let (bytes_per_operation, allocations_hundredths_per_operation) = (None, None);

    Ok(BenchmarkResult {
        elapsed,
        latencies,
        bytes_per_operation,
        allocations_hundredths_per_operation,
    })
}

async fn prepare_native_workers(
    config: &BenchmarkConfig,
    quic: Option<&QuicClient>,
    worker_operations: Vec<usize>,
    subscribe_permits: &[Arc<Semaphore>],
) -> Result<Vec<(PreparedOperation, usize)>> {
    let mut prepared = Vec::with_capacity(config.concurrency);
    for (worker, operations) in worker_operations.into_iter().enumerate() {
        let session = NativeSession::connect(config.transport, quic).await?;
        let mut operation =
            PreparedOperation::new(session, config, subscribe_permits.get(worker).cloned()).await?;
        if operation.needs_warmup() {
            operation.run().await?;
        }
        prepared.push((operation, operations));
    }
    Ok(prepared)
}

async fn wait_for_worker_signals(
    receiver: &mut mpsc::Receiver<()>,
    workers: &mut JoinSet<Result<Vec<u64>>>,
    count: usize,
    phase: &'static str,
) -> Result<()> {
    for _ in 0..count {
        tokio::select! {
            signal = receiver.recv() => {
                if signal.is_none() {
                    return unexpected_worker_exit(workers.join_next().await, phase);
                }
            }
            result = workers.join_next() => return unexpected_worker_exit(result, phase),
        }
    }
    Ok(())
}

fn unexpected_worker_exit(
    result: Option<std::result::Result<Result<Vec<u64>>, tokio::task::JoinError>>,
    phase: &'static str,
) -> Result<()> {
    match result {
        Some(Ok(Err(error))) => Err(error).with_context(|| format!("worker failed {phase}")),
        Some(Err(error)) => Err(error).with_context(|| format!("worker panicked {phase}")),
        Some(Ok(Ok(_))) => bail!("worker completed unexpectedly {phase}"),
        None => bail!("all workers exited {phase}"),
    }
}

#[derive(Debug)]
struct WorkerPlan {
    operation: PreparedOperation,
    operations: usize,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    ready: mpsc::Sender<()>,
    done: mpsc::Sender<()>,
}

async fn run_worker(mut plan: WorkerPlan) -> Result<Vec<u64>> {
    let mut latencies = Vec::with_capacity(plan.operations);
    plan.ready
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.start.wait().await;
    for _ in 0..plan.operations {
        let started = Instant::now();
        plan.operation.run().await?;
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    plan.operation.close().await?;
    plan.done
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.finish.wait().await;
    Ok(latencies)
}

trait BenchStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> BenchStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
type BoxStream = Box<dyn BenchStream>;
type BufferedStream = BufReader<BoxStream>;
struct NativeSession {
    stream: BufferedStream,
}
impl std::fmt::Debug for NativeSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeSession")
            .finish_non_exhaustive()
    }
}

impl NativeSession {
    async fn connect(transport: Transport, quic: Option<&QuicClient>) -> Result<Self> {
        let mut session = match transport {
            Transport::Tcp => Self {
                stream: BufReader::new(Box::new(connect_tcp(TCP_ADDRESS).await?)),
            },
            Transport::Unix => Self {
                stream: BufReader::new(Box::new(connect_unix().await?)),
            },
            Transport::WebSocket => connect_websocket().await?,
            Transport::Quic => Self {
                stream: BufReader::new(Box::new(
                    quic.context("QUIC client missing")?.open_stream().await?,
                )),
            },
        };
        session.negotiate_and_bind().await?;
        Ok(session)
    }

    async fn negotiate_and_bind(&mut self) -> Result<()> {
        self.stream.write_all(&hello_frame()?).await?;
        let mut hello = [0_u8; 4];
        self.stream.read_exact(&mut hello).await?;
        if hello != [ResponseCode::Hello as u8, 0, 1, 1] {
            bail!("invalid HELLO response {hello:?}");
        }
        let _server_build = read_bytes(&mut self.stream).await?;

        self.stream.write_all(&bind_frame()?).await?;
        let code = self.stream.read_u8().await?;
        let status = self.stream.read_u8().await?;
        if code != ResponseCode::Bind as u8 || status != 0 {
            bail!("invalid BIND response code={code} status={status}");
        }
        let routes = self.stream.read_u32().await?;
        if routes != 3 {
            bail!("invalid BIND route count {routes}");
        }
        for _ in 0..routes {
            let route = read_bytes(&mut self.stream).await?;
            let mut profile = [0_u8; 4];
            self.stream.read_exact(&mut profile).await?;
            if route == b"pub" && profile[1] != AcceptanceGuarantee::Local as u8 {
                bail!("invalid pub route profile {profile:?}");
            }
        }
        Ok(())
    }

    async fn operation_success(&mut self, code: ResponseCode, correlation_id: u32) -> Result<()> {
        let actual_code = self.stream.read_u8().await?;
        let actual_correlation = self.stream.read_u32().await?;
        let status = self.stream.read_u8().await?;
        if actual_code != code as u8 || actual_correlation != correlation_id || status != 0 {
            bail!(
                "invalid operation response code={actual_code} correlation={actual_correlation} status={status}"
            );
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.stream
            .write_all(&[RequestCode::Disconnect as u8])
            .await?;
        let response = self.stream.read_u8().await?;
        if response != ResponseCode::Disconnect as u8 {
            bail!("invalid DISCONNECT response {response}");
        }
        Ok(())
    }
}

#[derive(Debug)]
enum PreparedOperation {
    Produce {
        session: NativeSession,
        request: Vec<u8>,
        response: ResponseCode,
    },
    Fetch {
        session: NativeSession,
        request: Vec<u8>,
        response: ResponseCode,
        payload: usize,
        batch: usize,
        with_headers: bool,
    },
    Subscribe {
        session: NativeSession,
        subscription_id: u8,
        payload: usize,
        with_headers: bool,
        permit: Arc<Semaphore>,
    },
    Settlement {
        session: NativeSession,
        request: Vec<u8>,
        id_sequence_offsets: Vec<usize>,
        response: ResponseCode,
        batch: usize,
    },
    Transaction {
        session: NativeSession,
        begin: Vec<u8>,
        produce: Vec<u8>,
        commit: Vec<u8>,
    },
}

impl PreparedOperation {
    async fn new(
        mut session: NativeSession,
        config: &BenchmarkConfig,
        subscribe_permit: Option<Arc<Semaphore>>,
    ) -> Result<Self> {
        match config.operation {
            BenchmarkOperation::Produce => Ok(Self::Produce {
                session,
                request: produce_frame(config.payload, false)?,
                response: ResponseCode::Produce,
            }),
            BenchmarkOperation::HProduce => Ok(Self::Produce {
                session,
                request: produce_frame(config.payload, true)?,
                response: ResponseCode::HProduce,
            }),
            BenchmarkOperation::Fetch | BenchmarkOperation::HFetch => {
                let with_headers = config.operation == BenchmarkOperation::HFetch;
                Ok(Self::Fetch {
                    session,
                    request: fetch_frame(config.batch, true, with_headers)?,
                    response: if with_headers {
                        ResponseCode::HFetch
                    } else {
                        ResponseCode::Fetch
                    },
                    payload: config.payload,
                    batch: config.batch,
                    with_headers,
                })
            }
            BenchmarkOperation::Subscribe | BenchmarkOperation::HSubscribe => {
                let with_headers = config.operation == BenchmarkOperation::HSubscribe;
                session
                    .stream
                    .write_all(&subscribe_frame(true, with_headers)?)
                    .await?;
                let response = session.stream.read_u8().await?;
                let correlation = session.stream.read_u32().await?;
                let status = session.stream.read_u8().await?;
                let subscription_id = session.stream.read_u8().await?;
                let expected = if with_headers {
                    ResponseCode::HSubscribe
                } else {
                    ResponseCode::Subscribe
                };
                if response != expected as u8 || correlation != 1 || status != 0 {
                    bail!("invalid SUBSCRIBE response");
                }
                Ok(Self::Subscribe {
                    session,
                    subscription_id,
                    payload: config.payload,
                    with_headers,
                    permit: subscribe_permit.context("subscription permit missing")?,
                })
            }
            BenchmarkOperation::Ack | BenchmarkOperation::Nack => {
                session
                    .stream
                    .write_all(&fetch_frame(config.batch, false, false)?)
                    .await?;
                let (subscription_id, message_ids) = read_fetch_response(
                    &mut session.stream,
                    ResponseCode::Fetch,
                    config.payload,
                    config.batch,
                    false,
                    false,
                )
                .await?;
                let response = if config.operation == BenchmarkOperation::Ack {
                    ResponseCode::Ack
                } else {
                    ResponseCode::Nack
                };
                let (request, id_sequence_offsets) = settlement_frame(
                    config.operation == BenchmarkOperation::Nack,
                    subscription_id,
                    &message_ids,
                )?;
                Ok(Self::Settlement {
                    session,
                    request,
                    id_sequence_offsets,
                    response,
                    batch: config.batch,
                })
            }
            BenchmarkOperation::Transaction => Ok(Self::Transaction {
                session,
                begin: begin_transaction_frame()?,
                produce: transaction_produce_frame(config.payload)?,
                commit: commit_transaction_frame(),
            }),
        }
    }

    const fn needs_warmup(&self) -> bool {
        matches!(
            self,
            Self::Produce { .. } | Self::Fetch { .. } | Self::Transaction { .. }
        )
    }

    async fn run(&mut self) -> Result<()> {
        match self {
            Self::Produce {
                session,
                request,
                response,
            } => {
                session.stream.write_all(request).await?;
                session.operation_success(*response, 1).await
            }
            Self::Fetch {
                session,
                request,
                response,
                payload,
                batch,
                with_headers,
            } => {
                session.stream.write_all(request).await?;
                read_fetch_response(
                    &mut session.stream,
                    *response,
                    *payload,
                    *batch,
                    *with_headers,
                    true,
                )
                .await
                .map(|_| ())
            }
            Self::Subscribe {
                session,
                subscription_id,
                payload,
                with_headers,
                permit,
            } => {
                read_subscription_message(
                    &mut session.stream,
                    *subscription_id,
                    *payload,
                    *with_headers,
                )
                .await?;
                permit.add_permits(1);
                Ok(())
            }
            Self::Settlement {
                session,
                request,
                id_sequence_offsets,
                response,
                batch,
            } => {
                session.stream.write_all(request).await?;
                read_settlement_response(&mut session.stream, *response, *batch).await?;
                advance_settlement_frame(request, id_sequence_offsets)?;
                Ok(())
            }
            Self::Transaction {
                session,
                begin,
                produce,
                commit,
            } => {
                session.stream.write_all(begin).await?;
                session
                    .operation_success(ResponseCode::BeginTransaction, 1)
                    .await?;
                session.stream.write_all(produce).await?;
                session
                    .operation_success(ResponseCode::TransactionProduce, 2)
                    .await?;
                session.stream.write_all(commit).await?;
                session
                    .operation_success(ResponseCode::CommitTransaction, 3)
                    .await
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Self::Produce { session, .. }
            | Self::Fetch { session, .. }
            | Self::Subscribe { session, .. }
            | Self::Settlement { session, .. }
            | Self::Transaction { session, .. } => session.disconnect().await,
        }
    }
}

async fn read_fetch_response(
    stream: &mut BufferedStream,
    response: ResponseCode,
    payload_size: usize,
    batch: usize,
    with_headers: bool,
    auto_settle: bool,
) -> Result<(u8, Vec<Bytes>)> {
    let code = stream.read_u8().await?;
    let correlation = stream.read_u32().await?;
    let status = stream.read_u8().await?;
    let subscription_id = stream.read_u8().await?;
    let count = usize::try_from(stream.read_u32().await?)?;
    if code != response as u8 || correlation != 1 || status != 0 || count != batch {
        bail!(
            "invalid FETCH response code={code} correlation={correlation} status={status} count={count} expected_code={} expected_count={batch}",
            response as u8,
        );
    }
    let mut message_ids = Vec::with_capacity(if auto_settle { 0 } else { count });
    for _ in 0..count {
        if with_headers {
            read_headers(stream).await?;
        }
        if !auto_settle {
            message_ids.push(Bytes::from(read_bytes(stream).await?));
        }
        read_bytes_length(stream, payload_size).await?;
    }
    Ok((subscription_id, message_ids))
}

async fn read_subscription_message(
    stream: &mut BufferedStream,
    subscription_id: u8,
    payload_size: usize,
    with_headers: bool,
) -> Result<()> {
    let code = stream.read_u8().await?;
    let actual_subscription = stream.read_u8().await?;
    let expected = if with_headers {
        ResponseCode::HMessage
    } else {
        ResponseCode::Message
    };
    if code != expected as u8 || actual_subscription != subscription_id {
        bail!("invalid subscription message");
    }
    if with_headers {
        read_headers(stream).await?;
    }
    read_bytes_length(stream, payload_size).await?;
    Ok(())
}

async fn read_settlement_response(
    stream: &mut BufferedStream,
    response: ResponseCode,
    batch: usize,
) -> Result<()> {
    let code = stream.read_u8().await?;
    let correlation = stream.read_u32().await?;
    let status = stream.read_u8().await?;
    let count = usize::try_from(stream.read_u32().await?)?;
    if code != response as u8 || correlation != 2 || status != 0 || count != batch {
        bail!(
            "invalid settlement response code={code} correlation={correlation} status={status} count={count} expected_code={} expected_count={batch}",
            response as u8,
        );
    }
    for _ in 0..count {
        skip_bytes(stream).await?;
        if stream.read_u8().await? != 0 {
            bail!("settlement result failed");
        }
    }
    Ok(())
}

async fn read_headers(stream: &mut BufferedStream) -> Result<()> {
    let strings = usize::from(stream.read_u16().await?);
    if strings != 2 {
        bail!("invalid header string count {strings}");
    }
    read_expected_bytes(stream, b"content-type").await?;
    read_expected_bytes(stream, b"application/octet-stream").await?;
    Ok(())
}

async fn read_bytes_length(stream: &mut BufferedStream, expected: usize) -> Result<()> {
    let length = usize::try_from(stream.read_u32().await?)?;
    if length != expected {
        bail!("byte field length: got {length}, want {expected}");
    }
    skip_exact(stream, length).await
}

async fn read_expected_bytes(stream: &mut BufferedStream, expected: &[u8]) -> Result<()> {
    let length = usize::try_from(stream.read_u32().await?)?;
    if length != expected.len() {
        bail!("byte field length: got {length}, want {}", expected.len());
    }
    let mut offset = 0;
    while offset < expected.len() {
        let available = stream.fill_buf().await?;
        if available.is_empty() {
            bail!("unexpected EOF while reading byte field");
        }
        let read = available.len().min(expected.len() - offset);
        if available[..read] != expected[offset..offset + read] {
            bail!("invalid byte field");
        }
        stream.consume(read);
        offset += read;
    }
    Ok(())
}

async fn skip_bytes(stream: &mut BufferedStream) -> Result<()> {
    let length = usize::try_from(stream.read_u32().await?)?;
    skip_exact(stream, length).await
}

async fn skip_exact(stream: &mut BufferedStream, mut remaining: usize) -> Result<()> {
    while remaining > 0 {
        let available = stream.fill_buf().await?;
        if available.is_empty() {
            bail!("unexpected EOF while skipping byte field");
        }
        let read = available.len().min(remaining);
        stream.consume(read);
        remaining -= read;
    }
    Ok(())
}

fn hello_frame() -> Result<Vec<u8>> {
    let mut frame = vec![RequestCode::Hello as u8, 1, 1, 1];
    append_bytes(&mut frame, b"fujin-rust-bench")?;
    append_bytes(&mut frame, b"dev")?;
    Ok(frame)
}

fn bind_frame() -> Result<Vec<u8>> {
    let mut frame = vec![RequestCode::Bind as u8];
    append_bytes(&mut frame, b"connector")?;
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame.extend_from_slice(&0_u16.to_be_bytes());
    Ok(frame)
}

fn produce_frame(payload_size: usize, with_headers: bool) -> Result<Vec<u8>> {
    let mut frame = vec![if with_headers {
        RequestCode::HProduce as u8
    } else {
        RequestCode::Produce as u8
    }];
    frame.extend_from_slice(&1_u32.to_be_bytes());
    append_bytes(&mut frame, b"pub")?;
    if with_headers {
        append_headers(&mut frame)?;
    }
    append_bytes(&mut frame, &vec![0; payload_size])?;
    Ok(frame)
}

fn fetch_frame(batch: usize, auto_settle: bool, with_headers: bool) -> Result<Vec<u8>> {
    let mut frame = vec![if with_headers {
        RequestCode::HFetch as u8
    } else {
        RequestCode::Fetch as u8
    }];
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.push(u8::from(auto_settle));
    append_bytes(&mut frame, b"sub")?;
    frame.extend_from_slice(&u32::try_from(batch)?.to_be_bytes());
    Ok(frame)
}

fn subscribe_frame(auto_settle: bool, with_headers: bool) -> Result<Vec<u8>> {
    let mut frame = vec![if with_headers {
        RequestCode::HSubscribe as u8
    } else {
        RequestCode::Subscribe as u8
    }];
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.push(u8::from(auto_settle));
    append_bytes(&mut frame, b"sub")?;
    Ok(frame)
}

fn begin_transaction_frame() -> Result<Vec<u8>> {
    let mut frame = vec![RequestCode::BeginTransaction as u8];
    frame.extend_from_slice(&1_u32.to_be_bytes());
    append_bytes(&mut frame, b"tx")?;
    Ok(frame)
}

fn transaction_produce_frame(payload_size: usize) -> Result<Vec<u8>> {
    let mut frame = vec![RequestCode::TransactionProduce as u8];
    frame.extend_from_slice(&2_u32.to_be_bytes());
    append_bytes(&mut frame, &vec![0; payload_size])?;
    Ok(frame)
}

fn commit_transaction_frame() -> Vec<u8> {
    let mut frame = vec![RequestCode::CommitTransaction as u8];
    frame.extend_from_slice(&3_u32.to_be_bytes());
    frame
}

fn settlement_frame(
    nack: bool,
    subscription_id: u8,
    message_ids: &[Bytes],
) -> Result<(Vec<u8>, Vec<usize>)> {
    let mut frame = vec![if nack {
        RequestCode::Nack as u8
    } else {
        RequestCode::Ack as u8
    }];
    frame.extend_from_slice(&2_u32.to_be_bytes());
    frame.push(subscription_id);
    frame.extend_from_slice(&u32::try_from(message_ids.len())?.to_be_bytes());
    let mut offsets = Vec::with_capacity(message_ids.len());
    for message_id in message_ids {
        frame.extend_from_slice(&u32::try_from(message_id.len())?.to_be_bytes());
        let start = frame.len();
        frame.extend_from_slice(message_id);
        if message_id.len() < 9 {
            bail!("message ID is too short");
        }
        offsets.push(start + 1);
    }
    Ok((frame, offsets))
}

fn advance_settlement_frame(frame: &mut [u8], offsets: &[usize]) -> Result<()> {
    let increment = u64::try_from(offsets.len())?;
    for offset in offsets {
        let end = offset
            .checked_add(8)
            .context("message ID offset overflow")?;
        let sequence = u64::from_be_bytes(frame[*offset..end].try_into()?);
        let next = sequence
            .checked_add(increment)
            .context("message ID sequence overflow")?;
        frame[*offset..end].copy_from_slice(&next.to_be_bytes());
    }
    Ok(())
}

fn append_headers(frame: &mut Vec<u8>) -> Result<()> {
    frame.extend_from_slice(&2_u16.to_be_bytes());
    append_bytes(frame, b"content-type")?;
    append_bytes(frame, b"application/octet-stream")
}

fn append_bytes(frame: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    frame.extend_from_slice(&u32::try_from(value.len())?.to_be_bytes());
    frame.extend_from_slice(value);
    Ok(())
}

async fn read_bytes<R: AsyncRead + Unpin + ?Sized>(stream: &mut R) -> Result<Vec<u8>> {
    let length = usize::try_from(stream.read_u32().await?)?;
    let mut value = vec![0; length];
    stream.read_exact(&mut value).await?;
    Ok(value)
}

fn operation_count(worker: usize, concurrency: usize, operations: usize) -> usize {
    operations / concurrency + usize::from(worker < operations % concurrency)
}

async fn connect_tcp(address: &str) -> Result<tokio::net::TcpStream> {
    for _ in 0..100 {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                return Ok(stream);
            }
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    }
    bail!("TCP server did not become ready at {address}")
}

#[cfg(unix)]
async fn connect_unix() -> Result<tokio::net::UnixStream> {
    for _ in 0..100 {
        match tokio::net::UnixStream::connect(UNIX_PATH).await {
            Ok(stream) => return Ok(stream),
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    }
    bail!("Unix server did not become ready at {UNIX_PATH}")
}

#[cfg(not(unix))]
async fn connect_unix() -> Result<tokio::io::DuplexStream> {
    bail!("Unix benchmark is unavailable on this platform")
}

async fn connect_websocket() -> Result<NativeSession> {
    let stream = connect_tcp(WEBSOCKET_ADDRESS).await?;
    let (websocket, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
        .await
        .context("connect WebSocket")?;
    Ok(NativeSession {
        stream: BufReader::new(Box::new(NativeWebSocketStream::new(websocket))),
    })
}

#[derive(Debug)]
struct QuicClient {
    _endpoint: Endpoint,
    connection: Connection,
}

impl QuicClient {
    async fn open_stream(&self) -> Result<ClientQuicStream> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok(ClientQuicStream { recv, send })
    }
}

async fn connect_quic(
    certificate: rustls::pki_types::CertificateDer<'static>,
) -> Result<QuicClient> {
    let mut roots = RootCertStore::empty();
    roots.add(certificate)?;
    let mut endpoint = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    endpoint.set_default_client_config(quinn::ClientConfig::with_root_certificates(Arc::new(
        roots,
    ))?);
    let address: SocketAddr = QUIC_ADDRESS.parse()?;
    for _ in 0..100 {
        if let Ok(connecting) = endpoint.connect(address, "localhost")
            && let Ok(connection) = connecting.await
        {
            return Ok(QuicClient {
                _endpoint: endpoint,
                connection,
            });
        }
        sleep(Duration::from_millis(10)).await;
    }
    bail!("QUIC server did not become ready at {address}")
}

struct ClientQuicStream {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
}

impl AsyncRead for ClientQuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(context, buffer)
    }
}

impl AsyncWrite for ClientQuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.send), context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.send), context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.send), context)
    }
}

struct ServerHandle {
    shutdown: CancellationToken,
    task: JoinHandle<Result<()>>,
    quic_certificate: Option<rustls::pki_types::CertificateDer<'static>>,
    temporary_files: Vec<PathBuf>,
}

impl ServerHandle {
    async fn start(transport: Transport, catalog: Arc<fujin_core::Catalog>) -> Result<Self> {
        #[cfg(unix)]
        if transport == Transport::Unix {
            match tokio::fs::remove_file(UNIX_PATH).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("remove stale benchmark socket"),
            }
        }
        let mut config = fujin_runtime::fujin_server_config::ServerConfig {
            build: "rust-bench".into(),
            ..Default::default()
        };
        let mut quic_certificate = None;
        let mut temporary_files = Vec::new();
        match transport {
            Transport::Tcp => {
                config.tcp = Some(fujin_runtime::fujin_server_config::TcpListenerConfig {
                    listen: TCP_ADDRESS.into(),
                    tls: None,
                });
            }
            Transport::Unix => {
                config.unix = Some(fujin_runtime::fujin_server_config::UnixListenerConfig {
                    path: UNIX_PATH.into(),
                });
            }
            Transport::WebSocket => {
                config.websocket = Some(
                    fujin_runtime::fujin_server_config::WebSocketListenerConfig {
                        listen: WEBSOCKET_ADDRESS.into(),
                        path: "/".into(),
                        allowed_origins: Vec::new(),
                        max_message_bytes: 4 * 1024 * 1024,
                        tls: None,
                    },
                );
            }
            Transport::Quic => {
                let CertifiedKey { cert, signing_key } =
                    generate_simple_self_signed(vec!["localhost".into()])?;
                let prefix =
                    std::env::temp_dir().join(format!("fujin-rust-bench-{}", std::process::id()));
                let certificate_path = prefix.with_extension("cert.pem");
                let private_key_path = prefix.with_extension("key.pem");
                tokio::fs::write(&certificate_path, cert.pem()).await?;
                tokio::fs::write(&private_key_path, signing_key.serialize_pem()).await?;
                quic_certificate = Some(cert.der().clone());
                temporary_files.push(certificate_path.clone());
                temporary_files.push(private_key_path.clone());
                config.quic = Some(fujin_runtime::fujin_server_config::QuicListenerConfig {
                    listen: QUIC_ADDRESS.into(),
                    tls: fujin_runtime::fujin_server_config::TlsConfig {
                        certificate: certificate_path.display().to_string(),
                        private_key: private_key_path.display().to_string(),
                        client_certificates: None,
                        require_client_certificate: false,
                    },
                    max_incoming_streams: 1024,
                    max_idle_timeout: None,
                    keepalive_period: None,
                });
            }
        }
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            fujin_server::serve(config, catalog, Arc::new(NoBindMiddleware), server_shutdown).await
        });
        Ok(Self {
            shutdown,
            task,
            quic_certificate,
            temporary_files,
        })
    }

    fn quic_certificate(&self) -> Result<rustls::pki_types::CertificateDer<'static>> {
        self.quic_certificate
            .clone()
            .context("QUIC certificate missing")
    }

    async fn stop(self) -> Result<()> {
        self.shutdown.cancel();
        self.task.await.context("native matrix server panicked")??;
        for path in self.temporary_files {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("remove temporary file {}", path.display()));
                }
            }
        }
        Ok(())
    }
}
