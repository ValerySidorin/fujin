use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fujin_bench::nop_catalog;
use fujin_connector::Catalog;
use fujin_grpc_proto::fujin::v1 as pb;
use fujin_middleware::NoBindMiddleware;
use fujin_runtime::GrpcService;
use tokio::{
    net::TcpListener,
    sync::{Barrier, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;

#[cfg(feature = "bench-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
#[cfg(feature = "bench-alloc")]
use std::alloc::System;

#[cfg(feature = "bench-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// Matches the server response relay capacity so both streaming encoders can form full batches.
const MAX_PIPELINE_IN_FLIGHT: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Sync,
    Pipeline,
}

impl Mode {
    fn from_env() -> Result<Self> {
        match env::var("FUJIN_BENCH_MODE") {
            Ok(value) if value == "sync" => Ok(Self::Sync),
            Ok(value) if value == "pipeline" => Ok(Self::Pipeline),
            Ok(value) => bail!("invalid FUJIN_BENCH_MODE={value:?}; use sync or pipeline"),
            Err(env::VarError::NotPresent) => Ok(Self::Sync),
            Err(error) => Err(error.into()),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Pipeline => "pipeline",
        }
    }
}

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    payload: usize,
    payload_label: String,
    concurrency: usize,
    operations: usize,
    deadline: Duration,
    mode: Mode,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        let payload_label = env::var("FUJIN_BENCH_PAYLOAD").unwrap_or_else(|_| "128B".into());
        let mode = Mode::from_env()?;
        let concurrency = parse_positive("FUJIN_BENCH_CONCURRENCY", 1)?;
        if mode == Mode::Pipeline && concurrency != 1 {
            bail!("pipeline mode requires FUJIN_BENCH_CONCURRENCY=1");
        }
        Ok(Self {
            payload: parse_size(&payload_label)?,
            payload_label,
            concurrency,
            operations: parse_positive("FUJIN_BENCH_OPERATIONS", 10_000)?,
            deadline: parse_duration("FUJIN_BENCH_DEADLINE", Duration::from_secs(30))?,
            mode,
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
        let p99 = self.latencies[(99 * self.latencies.len()).div_ceil(100) - 1];
        let operations = u128::try_from(config.operations).expect("operation count fits u128");
        let ns_per_operation = self.elapsed.as_nanos() / operations;
        let megabytes_per_second =
            f64::from(u32::try_from(config.payload).expect("payload size fits benchmark metric"))
                * f64::from(
                    u32::try_from(config.operations)
                        .expect("operation count fits benchmark metric"),
                )
                / self.elapsed.as_secs_f64()
                / 1_000_000.0;
        println!(
            "rust/grpc/produce mode={} payload={} batch=1 concurrency={} operations={} ns/op={ns_per_operation} MB/s={megabytes_per_second:.2} p99-ns={p99} B/op={} allocs/op={}",
            config.mode.label(),
            config.payload_label,
            config.concurrency,
            config.operations,
            format_optional(self.bytes_per_operation),
            format_hundredths(self.allocations_hundredths_per_operation),
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
            Ok(Duration::from_secs(
                value.parse().context("invalid FUJIN_BENCH_DEADLINE")?,
            ))
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
        .parse::<usize>()
        .with_context(|| format!("invalid payload size {value:?}"))?
        .checked_mul(multiplier)
        .context("payload size overflow")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let mut result = timeout(config.deadline, run_benchmark(&config))
        .await
        .context("gRPC benchmark deadline exceeded")??;
    result.report(&config);
    Ok(())
}

async fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkResult> {
    let catalog = nop_catalog().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (server, shutdown) = spawn_server(listener, Arc::clone(&catalog));
    let payload = Arc::new(vec![0; config.payload]);
    let start = Arc::new(Barrier::new(config.concurrency + 1));
    let finish = Arc::new(Barrier::new(config.concurrency + 1));
    let (ready_sender, mut ready_receiver) = mpsc::channel(config.concurrency);
    let (done_sender, mut done_receiver) = mpsc::channel(config.concurrency);
    let mut workers = JoinSet::new();
    for worker in 0..config.concurrency {
        workers.spawn(run_worker(WorkerPlan {
            address,
            operations: operation_count(worker, config.concurrency, config.operations),
            payload: Arc::clone(&payload),
            mode: config.mode,
            start: Arc::clone(&start),
            finish: Arc::clone(&finish),
            ready: ready_sender.clone(),
            done: done_sender.clone(),
        }));
    }
    drop(ready_sender);
    drop(done_sender);
    for _ in 0..config.concurrency {
        ready_receiver
            .recv()
            .await
            .context("worker exited before benchmark start")?;
    }

    #[cfg(feature = "bench-alloc")]
    let allocation_region = Region::new(GLOBAL);
    let started = Instant::now();
    start.wait().await;
    for _ in 0..config.concurrency {
        tokio::select! {
            Some(()) = done_receiver.recv() => {}
            Some(result) = workers.join_next() => {
                result.context("gRPC benchmark worker panicked")??;
                bail!("gRPC benchmark worker exited before completion");
            }
            else => bail!("all gRPC benchmark workers exited before completion"),
        }
    }
    let elapsed = started.elapsed();
    #[cfg(feature = "bench-alloc")]
    let allocation_stats = allocation_region.change();
    finish.wait().await;

    let mut latencies = Vec::with_capacity(config.operations);
    while let Some(result) = workers.join_next().await {
        latencies.extend(result.context("gRPC benchmark worker panicked")??);
    }
    let _ = shutdown.send(());
    server.await.context("gRPC benchmark server panicked")??;
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

#[derive(Debug)]
struct WorkerPlan {
    address: std::net::SocketAddr,
    operations: usize,
    payload: Arc<Vec<u8>>,
    mode: Mode,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    ready: mpsc::Sender<()>,
    done: mpsc::Sender<()>,
}

async fn run_worker(plan: WorkerPlan) -> Result<Vec<u64>> {
    let (sender, mut responses) = open_stream(plan.address).await?;
    bind(&sender, &mut responses).await?;
    produce_round_trip(&sender, &mut responses, produce_request(&plan.payload, 0)).await?;
    let mut latencies = Vec::with_capacity(plan.operations);
    plan.ready
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.start.wait().await;
    match plan.mode {
        Mode::Sync => {
            for operation in 0..plan.operations {
                let started = Instant::now();
                produce_round_trip(
                    &sender,
                    &mut responses,
                    produce_request(&plan.payload, u32::try_from(operation + 1)?),
                )
                .await?;
                latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
        }
        Mode::Pipeline => {
            pipeline(
                &sender,
                &mut responses,
                &plan.payload,
                plan.operations,
                &mut latencies,
            )
            .await?;
        }
    }
    plan.done
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.finish.wait().await;
    drop(sender);
    Ok(latencies)
}

async fn open_stream(
    address: std::net::SocketAddr,
) -> Result<(
    mpsc::Sender<pb::FujinRequest>,
    tonic::codec::Streaming<pb::FujinResponse>,
)> {
    let mut client =
        pb::fujin_service_client::FujinServiceClient::connect(format!("http://{address}"))
            .await
            .context("connect gRPC benchmark client")?;
    let (sender, receiver) = mpsc::channel(MAX_PIPELINE_IN_FLIGHT);
    let response = client.stream(ReceiverStream::new(receiver)).await?;
    Ok((sender, response.into_inner()))
}

async fn bind(
    sender: &mpsc::Sender<pb::FujinRequest>,
    responses: &mut tonic::codec::Streaming<pb::FujinResponse>,
) -> Result<()> {
    sender
        .send(pb::FujinRequest {
            request: Some(pb::fujin_request::Request::Bind(pb::BindRequest {
                connector: "connector".into(),
                meta: std::collections::HashMap::default(),
                config_overrides: std::collections::HashMap::default(),
            })),
        })
        .await
        .context("gRPC benchmark request stream closed")?;
    let response = responses
        .message()
        .await?
        .context("gRPC stream closed before BIND")?;
    match response.response {
        Some(pb::fujin_response::Response::Bind(response)) if response.error.is_none() => Ok(()),
        _ => bail!("invalid gRPC BIND response"),
    }
}

async fn produce_round_trip(
    sender: &mpsc::Sender<pb::FujinRequest>,
    responses: &mut tonic::codec::Streaming<pb::FujinResponse>,
    request: pb::FujinRequest,
) -> Result<()> {
    sender
        .send(request)
        .await
        .context("gRPC benchmark request stream closed")?;
    validate_produce(
        responses
            .message()
            .await?
            .context("gRPC stream closed before PRODUCE response")?,
    )
}

async fn pipeline(
    sender: &mpsc::Sender<pb::FujinRequest>,
    responses: &mut tonic::codec::Streaming<pb::FujinResponse>,
    payload: &[u8],
    operations: usize,
    latencies: &mut Vec<u64>,
) -> Result<()> {
    let started = Instant::now();
    let mut sent = 0;
    let mut received = 0;
    while received < operations {
        tokio::select! {
            permit = sender.reserve(), if sent < operations && sent - received < MAX_PIPELINE_IN_FLIGHT => {
                permit.context("gRPC benchmark request stream closed")?
                    .send(produce_request(payload, u32::try_from(sent + 1)?));
                sent += 1;
            }
            response = responses.message(), if received < sent => {
                validate_produce(response?.context("gRPC stream closed before PRODUCE response")?)?;
                received += 1;
                latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
        }
    }
    Ok(())
}

fn validate_produce(response: pb::FujinResponse) -> Result<()> {
    match response.response {
        Some(pb::fujin_response::Response::Produce(response)) if response.error.is_none() => Ok(()),
        _ => bail!("invalid gRPC PRODUCE response"),
    }
}

fn produce_request(payload: &[u8], correlation_id: u32) -> pb::FujinRequest {
    pb::FujinRequest {
        request: Some(pb::fujin_request::Request::Produce(pb::ProduceRequest {
            correlation_id,
            route: "pub".into(),
            message: payload.to_owned(),
        })),
    }
}

fn operation_count(worker: usize, concurrency: usize, operations: usize) -> usize {
    operations / concurrency + usize::from(worker < operations % concurrency)
}

fn spawn_server(
    listener: TcpListener,
    catalog: Arc<Catalog>,
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
            .context("serve gRPC benchmark")
    });
    (server, shutdown)
}
