use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fujin_core::NoBindMiddleware;
use fujin_proto::fujin::v1 as pb;
use fujin_server::{
    GrpcService,
    bench_support::{
        BenchmarkOperation, SubscribeGate, session_bench_catalog, validate_benchmark_shape,
    },
};
use tokio::{
    net::TcpListener,
    sync::{Barrier, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Channel, Endpoint, Server};

#[cfg(feature = "bench-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
#[cfg(feature = "bench-alloc")]
use std::alloc::System;

#[cfg(feature = "bench-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const GRPC_SESSIONS_PER_CHANNEL: usize = 64;

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    operation: BenchmarkOperation,
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
            "rust/grpc/{operation} payload={payload} batch={batch} concurrency={concurrency} operations={operations} ns/op={ns_per_operation} MB/s={megabytes_per_second:.2} p99-ns={p99} B/op={bytes} allocs/op={allocations}",
            operation = config.operation.label(),
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
        .context("Rust gRPC matrix benchmark deadline exceeded")??;
    result.report(&config);
    Ok(())
}

async fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkResult> {
    let worker_operations: Vec<_> = (0..config.concurrency)
        .map(|worker| operation_count(worker, config.concurrency, config.operations))
        .collect();
    let subscribe_gate = Arc::new(SubscribeGate::default());
    let catalog = session_bench_catalog(
        config.payload,
        if config.operation.is_subscription() {
            worker_operations.clone()
        } else {
            Vec::new()
        },
        Arc::clone(&subscribe_gate),
    )
    .await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (server, shutdown) = spawn_server(listener, Arc::clone(&catalog));
    let channels = open_channels(address, config.concurrency).await?;
    let prepared = prepare_grpc_workers(&channels, config, worker_operations).await?;
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
        latencies.extend(result.context("gRPC matrix worker panicked")??);
    }
    let _ = shutdown.send(());
    server.await.context("gRPC matrix server panicked")??;
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

async fn open_channels(address: std::net::SocketAddr, concurrency: usize) -> Result<Vec<Channel>> {
    let count = concurrency.div_ceil(GRPC_SESSIONS_PER_CHANNEL);
    let mut channels = Vec::with_capacity(count);
    for _ in 0..count {
        channels.push(
            Endpoint::from_shared(format!("http://{address}"))?
                .connect()
                .await
                .context("connect pooled gRPC matrix channel")?,
        );
    }
    Ok(channels)
}

async fn prepare_grpc_workers(
    channels: &[Channel],
    config: &BenchmarkConfig,
    worker_operations: Vec<usize>,
) -> Result<Vec<(PreparedOperation, usize)>> {
    let mut prepared = Vec::with_capacity(config.concurrency);
    for (worker, operations) in worker_operations.into_iter().enumerate() {
        let channel = channels[worker % channels.len()].clone();
        let (sender, responses) = open_stream(channel).await?;
        let mut operation = PreparedOperation::new(sender, responses, config).await?;
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
        plan.operation
            .run()
            .await
            .context("run gRPC benchmark operation")?;
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    plan.operation
        .close()
        .await
        .context("close gRPC benchmark stream")?;
    plan.done
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.finish.wait().await;
    Ok(latencies)
}

type GrpcSender = mpsc::Sender<pb::FujinRequest>;
type GrpcResponses = tonic::codec::Streaming<pb::FujinResponse>;

async fn open_stream(channel: Channel) -> Result<(GrpcSender, GrpcResponses)> {
    let mut client = pb::fujin_service_client::FujinServiceClient::new(channel);
    let (sender, receiver) = mpsc::channel(64);
    let response = client.stream(ReceiverStream::new(receiver)).await?;
    let mut responses = response.into_inner();
    send(
        &sender,
        pb::fujin_request::Request::Bind(pb::BindRequest {
            connector: "connector".into(),
            meta: std::collections::HashMap::default(),
            config_overrides: std::collections::HashMap::default(),
        }),
    )
    .await?;
    match receive(&mut responses).await?.response {
        Some(pb::fujin_response::Response::Bind(response))
            if response.error.is_none() && response.routes.len() == 3 => {}
        response => bail!("invalid gRPC BIND response {response:?}"),
    }
    Ok((sender, responses))
}

#[derive(Debug)]
enum PreparedOperation {
    Produce {
        sender: GrpcSender,
        responses: GrpcResponses,
        request: pb::fujin_request::Request,
        with_headers: bool,
    },
    Fetch {
        sender: GrpcSender,
        responses: GrpcResponses,
        request: pb::fujin_request::Request,
        payload: usize,
        batch: usize,
        with_headers: bool,
    },
    Subscribe {
        _sender: GrpcSender,
        responses: GrpcResponses,
        subscription_id: u32,
        payload: usize,
        with_headers: bool,
    },
    Settlement {
        sender: GrpcSender,
        responses: GrpcResponses,
        request: pb::fujin_request::Request,
        batch: usize,
        nack: bool,
    },
    Transaction {
        sender: GrpcSender,
        responses: GrpcResponses,
        payload: Vec<u8>,
    },
}

impl PreparedOperation {
    async fn new(
        sender: GrpcSender,
        responses: GrpcResponses,
        config: &BenchmarkConfig,
    ) -> Result<Self> {
        let payload = vec![0; config.payload];
        match config.operation {
            BenchmarkOperation::Produce => Ok(prepare_produce(sender, responses, payload, false)),
            BenchmarkOperation::HProduce => Ok(prepare_produce(sender, responses, payload, true)),
            BenchmarkOperation::Fetch | BenchmarkOperation::HFetch => prepare_fetch(
                sender,
                responses,
                config.payload,
                config.batch,
                config.operation == BenchmarkOperation::HFetch,
            ),
            BenchmarkOperation::Subscribe | BenchmarkOperation::HSubscribe => {
                prepare_subscribe(
                    sender,
                    responses,
                    config.payload,
                    config.operation == BenchmarkOperation::HSubscribe,
                )
                .await
            }
            BenchmarkOperation::Ack | BenchmarkOperation::Nack => {
                prepare_settlement(
                    sender,
                    responses,
                    config.batch,
                    config.operation == BenchmarkOperation::Nack,
                )
                .await
            }
            BenchmarkOperation::Transaction => Ok(Self::Transaction {
                sender,
                responses,
                payload,
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
                sender,
                responses,
                request,
                with_headers,
            } => run_produce(sender, responses, request, *with_headers).await,
            Self::Fetch {
                sender,
                responses,
                request,
                payload,
                batch,
                with_headers,
            } => run_fetch(sender, responses, request, *payload, *batch, *with_headers).await,
            Self::Subscribe {
                responses,
                subscription_id,
                payload,
                with_headers,
                ..
            } => run_subscribe(responses, *subscription_id, *payload, *with_headers).await,
            Self::Settlement {
                sender,
                responses,
                request,
                batch,
                nack,
            } => run_settlement(sender, responses, request, *batch, *nack).await,
            Self::Transaction {
                sender,
                responses,
                payload,
            } => run_transaction(sender, responses, payload).await,
        }
    }

    async fn close(self) -> Result<()> {
        let (sender, mut responses) = match self {
            Self::Produce {
                sender, responses, ..
            }
            | Self::Fetch {
                sender, responses, ..
            }
            | Self::Settlement {
                sender, responses, ..
            }
            | Self::Transaction {
                sender, responses, ..
            }
            | Self::Subscribe {
                _sender: sender,
                responses,
                ..
            } => (sender, responses),
        };
        drop(sender);
        while responses.message().await?.is_some() {}
        Ok(())
    }
}

fn prepare_produce(
    sender: GrpcSender,
    responses: GrpcResponses,
    payload: Vec<u8>,
    with_headers: bool,
) -> PreparedOperation {
    let request = if with_headers {
        pb::fujin_request::Request::Hproduce(pb::HProduceRequest {
            correlation_id: 1,
            route: "pub".into(),
            headers: benchmark_headers(),
            message: payload,
        })
    } else {
        pb::fujin_request::Request::Produce(pb::ProduceRequest {
            correlation_id: 1,
            route: "pub".into(),
            message: payload,
        })
    };
    PreparedOperation::Produce {
        sender,
        responses,
        request,
        with_headers,
    }
}

fn prepare_fetch(
    sender: GrpcSender,
    responses: GrpcResponses,
    payload: usize,
    batch: usize,
    with_headers: bool,
) -> Result<PreparedOperation> {
    let batch_size = u32::try_from(batch)?;
    let request = if with_headers {
        pb::fujin_request::Request::Hfetch(pb::HFetchRequest {
            correlation_id: 1,
            route: "sub".into(),
            auto_commit: true,
            batch_size,
        })
    } else {
        pb::fujin_request::Request::Fetch(pb::FetchRequest {
            correlation_id: 1,
            route: "sub".into(),
            auto_commit: true,
            batch_size,
        })
    };
    Ok(PreparedOperation::Fetch {
        sender,
        responses,
        request,
        payload,
        batch,
        with_headers,
    })
}

async fn prepare_subscribe(
    sender: GrpcSender,
    mut responses: GrpcResponses,
    payload: usize,
    with_headers: bool,
) -> Result<PreparedOperation> {
    let request = if with_headers {
        pb::fujin_request::Request::Hsubscribe(pb::HSubscribeRequest {
            correlation_id: 1,
            route: "sub".into(),
            auto_commit: true,
        })
    } else {
        pb::fujin_request::Request::Subscribe(pb::SubscribeRequest {
            correlation_id: 1,
            route: "sub".into(),
            auto_commit: true,
        })
    };
    send(&sender, request).await?;
    let subscription_id = match receive(&mut responses).await?.response {
        Some(pb::fujin_response::Response::Subscribe(value))
            if !with_headers && value.error.is_none() =>
        {
            value.subscription_id
        }
        Some(pb::fujin_response::Response::Hsubscribe(value))
            if with_headers && value.error.is_none() =>
        {
            value.subscription_id
        }
        value => bail!("invalid gRPC SUBSCRIBE response {value:?}"),
    };
    Ok(PreparedOperation::Subscribe {
        _sender: sender,
        responses,
        subscription_id,
        payload,
        with_headers,
    })
}

async fn prepare_settlement(
    sender: GrpcSender,
    mut responses: GrpcResponses,
    batch: usize,
    nack: bool,
) -> Result<PreparedOperation> {
    send(
        &sender,
        pb::fujin_request::Request::Fetch(pb::FetchRequest {
            correlation_id: 1,
            route: "sub".into(),
            auto_commit: false,
            batch_size: u32::try_from(batch)?,
        }),
    )
    .await?;
    let fetch = match receive(&mut responses).await?.response {
        Some(pb::fujin_response::Response::Fetch(value))
            if value.error.is_none() && value.messages.len() == batch =>
        {
            value
        }
        value => bail!("invalid gRPC settlement FETCH response {value:?}"),
    };
    let message_ids = fetch
        .messages
        .into_iter()
        .map(|message| message.message_id)
        .collect();
    let request = if nack {
        pb::fujin_request::Request::Nack(pb::NackRequest {
            correlation_id: 2,
            message_ids,
            subscription_id: fetch.subscription_id,
        })
    } else {
        pb::fujin_request::Request::Ack(pb::AckRequest {
            correlation_id: 2,
            message_ids,
            subscription_id: fetch.subscription_id,
        })
    };
    Ok(PreparedOperation::Settlement {
        sender,
        responses,
        request,
        batch,
        nack,
    })
}

async fn run_produce(
    sender: &GrpcSender,
    responses: &mut GrpcResponses,
    request: &pb::fujin_request::Request,
    with_headers: bool,
) -> Result<()> {
    send(sender, request.clone()).await?;
    match receive(responses).await?.response {
        Some(pb::fujin_response::Response::Produce(value))
            if !with_headers && value.error.is_none() =>
        {
            Ok(())
        }
        Some(pb::fujin_response::Response::Hproduce(value))
            if with_headers && value.error.is_none() =>
        {
            Ok(())
        }
        value => bail!("invalid gRPC PRODUCE response {value:?}"),
    }
}

async fn run_fetch(
    sender: &GrpcSender,
    responses: &mut GrpcResponses,
    request: &pb::fujin_request::Request,
    payload: usize,
    batch: usize,
    with_headers: bool,
) -> Result<()> {
    send(sender, request.clone()).await?;
    match receive(responses).await?.response {
        Some(pb::fujin_response::Response::Fetch(value))
            if !with_headers
                && value.error.is_none()
                && value.messages.len() == batch
                && value
                    .messages
                    .iter()
                    .all(|message| message.payload.len() == payload) =>
        {
            Ok(())
        }
        Some(pb::fujin_response::Response::Hfetch(value))
            if with_headers
                && value.error.is_none()
                && value.messages.len() == batch
                && value.messages.iter().all(|message| {
                    message.payload.len() == payload && message.headers.len() == 1
                }) =>
        {
            Ok(())
        }
        value => bail!("invalid gRPC FETCH response {value:?}"),
    }
}

async fn run_subscribe(
    responses: &mut GrpcResponses,
    subscription_id: u32,
    payload: usize,
    with_headers: bool,
) -> Result<()> {
    match receive(responses).await?.response {
        Some(pb::fujin_response::Response::Message(value))
            if !with_headers
                && value.subscription_id == subscription_id
                && value.payload.len() == payload =>
        {
            Ok(())
        }
        Some(pb::fujin_response::Response::Hmessage(value))
            if with_headers
                && value.subscription_id == subscription_id
                && value.payload.len() == payload
                && value.headers.len() == 1 =>
        {
            Ok(())
        }
        value => bail!("invalid gRPC subscription message {value:?}"),
    }
}

async fn run_settlement(
    sender: &GrpcSender,
    responses: &mut GrpcResponses,
    request: &mut pb::fujin_request::Request,
    batch: usize,
    nack: bool,
) -> Result<()> {
    send(sender, request.clone()).await?;
    match receive(responses).await?.response {
        Some(pb::fujin_response::Response::Ack(value))
            if !nack
                && value.error.is_none()
                && value.results.len() == batch
                && value.results.iter().all(|result| result.error.is_none()) => {}
        Some(pb::fujin_response::Response::Nack(value))
            if nack
                && value.error.is_none()
                && value.results.len() == batch
                && value.results.iter().all(|result| result.error.is_none()) => {}
        value => bail!("invalid gRPC settlement response {value:?}"),
    }
    advance_settlement_request(request, batch)
}

async fn run_transaction(
    sender: &GrpcSender,
    responses: &mut GrpcResponses,
    payload: &[u8],
) -> Result<()> {
    send(
        sender,
        pb::fujin_request::Request::BeginTx(pb::BeginTxRequest {
            correlation_id: 1,
            route: "tx".into(),
        }),
    )
    .await?;
    expect_transaction_response(receive(responses).await?, TransactionResponse::Begin)?;
    send(
        sender,
        pb::fujin_request::Request::TxProduce(pb::TxProduceRequest {
            correlation_id: 2,
            message: payload.to_vec(),
        }),
    )
    .await?;
    expect_transaction_response(receive(responses).await?, TransactionResponse::Produce)?;
    send(
        sender,
        pb::fujin_request::Request::CommitTx(pb::CommitTxRequest { correlation_id: 3 }),
    )
    .await?;
    expect_transaction_response(receive(responses).await?, TransactionResponse::Commit)
}

#[derive(Clone, Copy)]
enum TransactionResponse {
    Begin,
    Produce,
    Commit,
}

fn expect_transaction_response(
    response: pb::FujinResponse,
    expected: TransactionResponse,
) -> Result<()> {
    match (expected, response.response) {
        (TransactionResponse::Begin, Some(pb::fujin_response::Response::BeginTx(value)))
            if value.error.is_none() =>
        {
            Ok(())
        }
        (TransactionResponse::Produce, Some(pb::fujin_response::Response::TxProduce(value)))
            if value.error.is_none() =>
        {
            Ok(())
        }
        (TransactionResponse::Commit, Some(pb::fujin_response::Response::CommitTx(value)))
            if value.error.is_none() =>
        {
            Ok(())
        }
        (_, value) => bail!("invalid gRPC transaction response {value:?}"),
    }
}

fn advance_settlement_request(
    request: &mut pb::fujin_request::Request,
    batch: usize,
) -> Result<()> {
    let message_ids = match request {
        pb::fujin_request::Request::Ack(value) => &mut value.message_ids,
        pb::fujin_request::Request::Nack(value) => &mut value.message_ids,
        _ => bail!("settlement request has wrong type"),
    };
    let increment = u64::try_from(batch)?;
    for message_id in message_ids {
        if message_id.len() < 9 {
            bail!("message ID is too short");
        }
        let sequence = u64::from_be_bytes(message_id[1..9].try_into()?);
        message_id[1..9].copy_from_slice(
            &sequence
                .checked_add(increment)
                .context("message ID sequence overflow")?
                .to_be_bytes(),
        );
    }
    Ok(())
}

fn benchmark_headers() -> Vec<pb::Kv> {
    vec![pb::Kv {
        key: b"content-type".to_vec(),
        value: b"application/octet-stream".to_vec(),
    }]
}

async fn send(sender: &GrpcSender, request: pb::fujin_request::Request) -> Result<()> {
    sender
        .send(pb::FujinRequest {
            request: Some(request),
        })
        .await
        .context("gRPC request stream closed")
}

async fn receive(responses: &mut GrpcResponses) -> Result<pb::FujinResponse> {
    responses
        .message()
        .await?
        .context("gRPC response stream closed")
}

fn operation_count(worker: usize, concurrency: usize, operations: usize) -> usize {
    operations / concurrency + usize::from(worker < operations % concurrency)
}

fn spawn_server(
    listener: TcpListener,
    catalog: Arc<fujin_core::Catalog>,
) -> (JoinHandle<Result<()>>, oneshot::Sender<()>) {
    let (shutdown, receiver) = oneshot::channel();
    let service = GrpcService::new(catalog, Arc::new(NoBindMiddleware));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::fujin_service_server::FujinServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = receiver.await;
            })
            .await
            .context("serve gRPC matrix benchmark")
    });
    (server, shutdown)
}
