use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fujin_core::{AcceptanceGuarantee, Catalog, NoBindMiddleware};
use fujin_server::bench_support::nop_catalog;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{Barrier, mpsc},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

#[cfg(feature = "bench-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
#[cfg(feature = "bench-alloc")]
use std::alloc::System;

#[cfg(feature = "bench-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const HELLO_RESPONSE: u8 = 19;
const BIND_RESPONSE: u8 = 16;
const PRODUCE_RESPONSE: u8 = 3;
const DISCONNECT_RESPONSE: u8 = 15;

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
        let p99_index = (99 * self.latencies.len()).div_ceil(100) - 1;
        let p99 = self.latencies[p99_index];
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
        let bytes = format_optional(self.bytes_per_operation);
        let allocations = format_hundredths(self.allocations_hundredths_per_operation);
        println!(
            "rust/native-tcp/produce mode={} payload={} batch=1 concurrency={} operations={} ns/op={ns_per_operation} MB/s={megabytes_per_second:.2} p99-ns={p99} B/op={bytes} allocs/op={allocations}",
            config.mode.label(),
            config.payload_label,
            config.concurrency,
            config.operations
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
            let seconds = value
                .parse::<u64>()
                .context("invalid FUJIN_BENCH_DEADLINE")?;
            Ok(Duration::from_secs(seconds))
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
    let number = number
        .parse::<usize>()
        .with_context(|| format!("invalid payload size {value:?}"))?;
    number
        .checked_mul(multiplier)
        .context("payload size overflow")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let mut result = timeout(config.deadline, run_benchmark(&config))
        .await
        .context("Rust benchmark deadline exceeded")??;
    result.report(&config);
    Ok(())
}

async fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkResult> {
    let catalog = nop_catalog().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = spawn_server(listener, Arc::clone(&catalog), config.concurrency);
    let request = Arc::new(produce_frame(config.payload)?);
    let start = Arc::new(Barrier::new(config.concurrency + 1));
    let finish = Arc::new(Barrier::new(config.concurrency + 1));
    let (ready_sender, mut ready_receiver) = mpsc::channel(config.concurrency);
    let (done_sender, mut done_receiver) = mpsc::channel(config.concurrency);
    let mut workers = JoinSet::new();

    for worker in 0..config.concurrency {
        let count = operation_count(worker, config.concurrency, config.operations);
        workers.spawn(run_worker(WorkerPlan {
            address,
            operations: count,
            request: Arc::clone(&request),
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
        done_receiver
            .recv()
            .await
            .context("worker exited during benchmark")?;
    }
    let elapsed = started.elapsed();
    #[cfg(feature = "bench-alloc")]
    let allocation_stats = allocation_region.change();
    finish.wait().await;

    let mut latencies = Vec::with_capacity(config.operations);
    while let Some(result) = workers.join_next().await {
        latencies.extend(result.context("Rust benchmark worker panicked")??);
    }
    server.await.context("Rust benchmark server panicked")??;
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
    request: Arc<Vec<u8>>,
    mode: Mode,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    ready: mpsc::Sender<()>,
    done: mpsc::Sender<()>,
}

async fn run_worker(plan: WorkerPlan) -> Result<Vec<u64>> {
    let mut stream = TcpStream::connect(plan.address).await?;
    stream.set_nodelay(true)?;
    negotiate_and_bind(&mut stream).await?;
    round_trip_produce(&mut stream, &plan.request).await?;
    let mut latencies = Vec::with_capacity(plan.operations);
    plan.ready
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.start.wait().await;
    match plan.mode {
        Mode::Sync => {
            for _ in 0..plan.operations {
                let started = Instant::now();
                round_trip_produce(&mut stream, &plan.request).await?;
                latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
            disconnect(&mut stream).await?;
        }
        Mode::Pipeline => pipeline(stream, &plan.request, plan.operations, &mut latencies).await?,
    }
    plan.done
        .send(())
        .await
        .context("benchmark coordinator closed")?;
    plan.finish.wait().await;
    Ok(latencies)
}

async fn negotiate_and_bind(stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&hello_frame()?).await?;
    let mut hello = [0_u8; 4];
    stream.read_exact(&mut hello).await?;
    if hello != [HELLO_RESPONSE, 0, 1, 1] {
        bail!("invalid HELLO response {hello:?}");
    }
    let _server_build = read_bytes(stream).await?;

    stream.write_all(&bind_frame()?).await?;
    let mut bind = [0_u8; 6];
    stream.read_exact(&mut bind).await?;
    if bind[0] != BIND_RESPONSE || bind[1] != 0 || u32::from_be_bytes(bind[2..6].try_into()?) != 1 {
        bail!("invalid BIND response {bind:?}");
    }
    if read_bytes(stream).await? != b"pub" {
        bail!("invalid BIND route");
    }
    let mut profile = [0_u8; 4];
    stream.read_exact(&mut profile).await?;
    if profile != [0x07, AcceptanceGuarantee::Local as u8, 0, 0] {
        bail!("invalid BIND profile {profile:?}");
    }
    Ok(())
}

async fn round_trip_produce(stream: &mut TcpStream, request: &[u8]) -> Result<()> {
    stream.write_all(request).await?;
    let mut response = [0_u8; 6];
    stream.read_exact(&mut response).await?;
    if response != [PRODUCE_RESPONSE, 0, 0, 0, 1, 0] {
        bail!("invalid PRODUCE response {response:?}");
    }
    Ok(())
}

async fn pipeline(
    stream: TcpStream,
    request: &[u8],
    operations: usize,
    latencies: &mut Vec<u64>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(512 * 1024, reader);
    let request = request.to_owned();
    let write = tokio::spawn(async move {
        let mut writer = BufWriter::with_capacity(512 * 1024, writer);
        for _ in 0..operations {
            writer.write_all(&request).await?;
        }
        writer.flush().await?;
        Result::<(), std::io::Error>::Ok(())
    });
    let started = Instant::now();
    let mut response = [0_u8; 6];
    for _ in 0..operations {
        reader.read_exact(&mut response).await?;
        if response != [PRODUCE_RESPONSE, 0, 0, 0, 1, 0] {
            bail!("invalid PRODUCE response {response:?}");
        }
        latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    write.await.context("native pipeline writer panicked")??;
    Ok(())
}

async fn disconnect(stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&[14]).await?;
    let response = stream.read_u8().await?;
    if response != DISCONNECT_RESPONSE {
        bail!("invalid DISCONNECT response {response}");
    }
    Ok(())
}

async fn read_bytes(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let length = usize::try_from(stream.read_u32().await?)?;
    let mut value = vec![0; length];
    stream.read_exact(&mut value).await?;
    Ok(value)
}

fn operation_count(worker: usize, concurrency: usize, operations: usize) -> usize {
    operations / concurrency + usize::from(worker < operations % concurrency)
}

fn hello_frame() -> Result<Vec<u8>> {
    let mut frame = vec![0, 1, 1, 1];
    append_bytes(&mut frame, b"fujin-rust-bench")?;
    append_bytes(&mut frame, b"dev")?;
    Ok(frame)
}

fn bind_frame() -> Result<Vec<u8>> {
    let mut frame = vec![1];
    append_bytes(&mut frame, b"connector")?;
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame.extend_from_slice(&0_u16.to_be_bytes());
    Ok(frame)
}

fn produce_frame(payload_size: usize) -> Result<Vec<u8>> {
    let mut frame = vec![2];
    frame.extend_from_slice(&1_u32.to_be_bytes());
    append_bytes(&mut frame, b"pub")?;
    append_bytes(&mut frame, &vec![0; payload_size])?;
    Ok(frame)
}

fn append_bytes(frame: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    frame.extend_from_slice(&u32::try_from(value.len())?.to_be_bytes());
    frame.extend_from_slice(value);
    Ok(())
}

fn spawn_server(
    listener: TcpListener,
    catalog: Arc<Catalog>,
    sessions: usize,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        for _ in 0..sessions {
            let (stream, _) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let catalog = Arc::clone(&catalog);
            tasks.spawn(async move {
                fujin_native::run(stream, catalog, Arc::new(NoBindMiddleware), "rust-bench")
                    .await
                    .map_err(anyhow::Error::from)
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.context("native benchmark session panicked")??;
        }
        Ok(())
    })
}
