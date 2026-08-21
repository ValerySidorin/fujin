#!/usr/bin/env bash
# Generate a self-contained Rust Fujin no-broker performance report.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output=${OUTPUT:-"$root/fujin-rs/bench_report.md"}
payloads=${FUJIN_BENCH_PAYLOAD:-1B,128B,1MiB}
concurrency=${FUJIN_BENCH_CONCURRENCY:-1,16,128}
deadline=${FUJIN_BENCH_DEADLINE:-300s}
small_operations=${FUJIN_BENCH_SMALL_OPERATIONS:-10000}
large_operations=${FUJIN_BENCH_LARGE_OPERATIONS:-1000}
peak_operations=${FUJIN_BENCH_PEAK_ITERATIONS:-1000000}

if ! command -v cargo >/dev/null 2>&1; then
	printf '%s\n' 'cargo is required' >&2
	exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
	printf '%s\n' 'python3 is required' >&2
	exit 1
fi

mkdir -p "$(dirname -- "$output")"
raw=$(mktemp "${output}.raw.XXXXXX")
alloc_raw=$(mktemp "${output}.alloc.raw.XXXXXX")
tmp=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$raw" "$alloc_raw" "$tmp"' EXIT

case "$deadline" in
	*[!0-9s]* | '')
		printf '%s\n' 'FUJIN_BENCH_DEADLINE must be a whole number of seconds, such as 300s' >&2
		exit 1
		;;
esac

if git -C "$root" diff --quiet && git -C "$root" diff --cached --quiet; then
	git_state=clean
else
	git_state=dirty
fi
source=$(git -C "$root" describe --always --dirty)

IFS=',' read -r -a payload_values <<<"$payloads"
IFS=',' read -r -a concurrency_values <<<"$concurrency"

for payload in "${payload_values[@]}"; do
	payload=${payload//[[:space:]]/}
	case "$payload" in
		1MiB) operations=$large_operations ;;
		*) operations=$small_operations ;;
	esac
	for sessions in "${concurrency_values[@]}"; do
		sessions=${sessions//[[:space:]]/}
		for binary in session-bench grpc-session-bench; do
			if ! (
				cd "$root/fujin-rs"
				FUJIN_BENCH_MODE=sync \
				FUJIN_BENCH_PAYLOAD="$payload" \
				FUJIN_BENCH_CONCURRENCY="$sessions" \
				FUJIN_BENCH_OPERATIONS="$operations" \
				FUJIN_BENCH_DEADLINE="$deadline" \
				cargo run --release -q -p fujin-server --bin "$binary" --features bench
			) >>"$raw" 2>&1; then
				printf 'benchmark failed for %s payload=%s concurrency=%s; preserved existing report at %s\n' "$binary" "$payload" "$sessions" "$output" >&2
				exit 1
			fi
			if ! (
				cd "$root/fujin-rs"
				FUJIN_BENCH_MODE=sync \
				FUJIN_BENCH_PAYLOAD="$payload" \
				FUJIN_BENCH_CONCURRENCY="$sessions" \
				FUJIN_BENCH_OPERATIONS="$operations" \
				FUJIN_BENCH_DEADLINE="$deadline" \
				CARGO_TARGET_DIR=target/bench-alloc \
				cargo run --release -q -p fujin-server --bin "$binary" --features bench-alloc
			) >>"$alloc_raw" 2>&1; then
				printf 'allocation benchmark failed for %s payload=%s concurrency=%s; preserved existing report at %s\n' "$binary" "$payload" "$sessions" "$output" >&2
				exit 1
			fi
		done
	done
done

for binary in session-bench grpc-session-bench; do
	if ! (
		cd "$root/fujin-rs"
		FUJIN_BENCH_MODE=pipeline \
		FUJIN_BENCH_PAYLOAD=1B \
		FUJIN_BENCH_CONCURRENCY=1 \
		FUJIN_BENCH_OPERATIONS="$peak_operations" \
		FUJIN_BENCH_DEADLINE="$deadline" \
		cargo run --release -q -p fujin-server --bin "$binary" --features bench
	) >>"$raw" 2>&1; then
		printf 'pipeline benchmark failed for %s; preserved existing report at %s\n' "$binary" "$output" >&2
		exit 1
	fi
	if ! (
		cd "$root/fujin-rs"
		FUJIN_BENCH_MODE=pipeline \
		FUJIN_BENCH_PAYLOAD=1B \
		FUJIN_BENCH_CONCURRENCY=1 \
		FUJIN_BENCH_OPERATIONS="$peak_operations" \
		FUJIN_BENCH_DEADLINE="$deadline" \
		CARGO_TARGET_DIR=target/bench-alloc \
		cargo run --release -q -p fujin-server --bin "$binary" --features bench-alloc
	) >>"$alloc_raw" 2>&1; then
		printf 'pipeline allocation benchmark failed for %s; preserved existing report at %s\n' "$binary" "$output" >&2
		exit 1
	fi
done

python3 - "$raw" "$alloc_raw" "$tmp" "$payloads" "$concurrency" "$small_operations" "$large_operations" "$peak_operations" "$source" "$git_state" <<'PY'
import datetime
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

raw_path, alloc_path, output_path, payload_filter, concurrency_filter, small_operations, large_operations, peak_operations, source, git_state = sys.argv[1:]
line = re.compile(
    r"^rust/(?P<transport>native-tcp|grpc)/produce mode=(?P<mode>sync|pipeline) "
    r"payload=(?P<payload>\S+) batch=1 concurrency=(?P<concurrency>\d+) "
    r"operations=(?P<operations>\d+) ns/op=(?P<ns>\d+) MB/s=(?P<mbps>[0-9.]+) "
    r"p99-ns=(?P<p99>\d+) B/op=(?P<bytes>\S+) allocs/op=(?P<allocs>\S+)$"
)

def parse(path):
    rows = {}
    for text in Path(path).read_text().splitlines():
        match = line.fullmatch(text)
        if not match:
            continue
        row = match.groupdict()
        key = row["transport"], row["mode"], row["payload"], int(row["concurrency"])
        if key in rows:
            raise SystemExit(f"duplicate benchmark result: {key}")
        rows[key] = row
    return rows

raw = parse(raw_path)
alloc = parse(alloc_path)
payloads = [item.strip() for item in payload_filter.split(",") if item.strip()]
concurrency = [int(item.strip()) for item in concurrency_filter.split(",") if item.strip()]
expected_sync = {
    (transport, "sync", payload, sessions)
    for transport in ("native-tcp", "grpc")
    for payload in payloads
    for sessions in concurrency
}
expected_pipeline = {
    (transport, "pipeline", "1B", 1)
    for transport in ("native-tcp", "grpc")
}
expected = expected_sync | expected_pipeline
if set(raw) != expected:
    raise SystemExit(f"incomplete timing results: missing={expected - set(raw)} unexpected={set(raw) - expected}")
if set(alloc) != expected:
    raise SystemExit(f"incomplete allocation results: missing={expected - set(alloc)} unexpected={set(alloc) - expected}")
for key in expected:
    if raw[key]["bytes"] != "n/a" or raw[key]["allocs"] != "n/a":
        raise SystemExit(f"timing run unexpectedly used allocation instrumentation: {key}")
    if alloc[key]["bytes"] == "n/a" or alloc[key]["allocs"] == "n/a":
        raise SystemExit(f"allocation metrics missing: {key}")

def rendered_transport(name):
    return "native TCP" if name == "native-tcp" else "gRPC"

def table_row(key):
    timing = raw[key]
    allocation = alloc[key]
    ns = int(timing["ns"])
    messages = 1_000_000_000 / ns
    return (
        f"| {rendered_transport(timing['transport'])} | {timing['payload']} | {timing['concurrency']} | "
        f"{messages:.0f} | {messages / 1_000_000:.3f} | {timing['mbps']} MB/s | "
        f"{int(timing['p99']) / 1_000:.2f} µs | {allocation['allocs']} | {allocation['bytes']} |"
    )

def pipeline_row(key):
    timing = raw[key]
    allocation = alloc[key]
    ns = int(timing["ns"])
    messages = 1_000_000_000 / ns
    mode = "One full-duplex pipelined session" if timing["transport"] == "native-tcp" else "One bounded full-duplex session"
    return (
        f"| {rendered_transport(timing['transport'])} | 1 B | {mode} | {peak_operations} | "
        f"{messages:.0f} | {messages / 1_000_000:.3f} | {timing['mbps']} MB/s | "
        f"{allocation['allocs']} | {allocation['bytes']} |"
    )

rust_version = subprocess.check_output(["rustc", "--version"], text=True).strip()
os_name = f"{platform.system()} {platform.release()} {platform.machine()}"
generated = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
lines = [
    "# Fujin Rust Nop Connector Performance Report",
    "",
    f"**Generated:** {generated}",
    f"**Source:** `{source}` ({git_state})",
    f"**Environment:** `{rust_version}` on `{os_name}`",
    "",
    "## Scope",
    "",
    "The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Rust Fujin's Session Core and real localhost wire adapters using the built-in **`nop` connector**. The connector accepts every message locally and performs no broker I/O. Results therefore isolate protocol, Session Core, scheduling, encoding, callback, and transport overhead.",
    "",
    "- **Measured transports:** native TCP and gRPC. The production runtime also supports QUIC, Unix sockets, and WebSocket; those adapters are outside this focused no-broker benchmark.",
    f"- **Synchronous matrix payloads:** {payload_filter}",
    f"- **Synchronous concurrent sessions:** {concurrency_filter}",
    "- **Synchronous batch:** 1 message per operation",
    f"- **Synchronous operations per cell:** {small_operations} for 1B/128B; {large_operations} for 1MiB",
    f"- **Pipeline peak:** 1 B payload, one session, {peak_operations} messages for native TCP and gRPC",
    "- **Allocation metrics:** a separate `stats_alloc` instrumented process; latency and throughput come only from normal allocator runs.",
    "",
    "> These are single-host no-broker snapshots. They do not characterize connector durability, broker acknowledgement latency, unmeasured transports, or cross-machine performance.",
    "",
    "## Synchronous request/response results",
    "",
    "| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
]
for transport in ("native-tcp", "grpc"):
    for payload in payloads:
        for sessions in concurrency:
            lines.append(table_row((transport, "sync", payload, sessions)))
lines += [
    "",
    "### Reading the two result modes",
    "",
    "The synchronous matrix reports request/response behavior at the stated concurrent-session count. It is the p99 and capacity view. The pipeline table uses one 1 B session with concurrent response draining and is the sustainable throughput view, **not a latency comparison**.",
    "",
    "## 1 B pipelined throughput",
    "",
    f"Both rows use one client session, exactly {peak_operations} PRODUCE messages, concurrent response draining, and the nop connector. Native TCP uses 512 KiB buffered writes and reads while validating every pre-encoded request and six-byte response. gRPC keeps at most 4096 operations in flight, matching the server response relay capacity so Tonic can coalesce ready messages up to its 32 KiB encoder yield threshold.",
    "",
    "| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |",
    "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    pipeline_row(("native-tcp", "pipeline", "1B", 1)),
    pipeline_row(("grpc", "pipeline", "1B", 1)),
    "",
    "## Reproduce",
    "",
    "```bash",
    "./fujin-rs/scripts/generate_bench_report.sh",
    "```",
    "",
    "Run a smaller local validation report:",
    "",
    "```bash",
    "FUJIN_BENCH_SMALL_OPERATIONS=1000 FUJIN_BENCH_LARGE_OPERATIONS=100 FUJIN_BENCH_PEAK_ITERATIONS=10000 ./fujin-rs/scripts/generate_bench_report.sh",
    "```",
    "",
    "The generator performs normal-allocation timing runs and isolated allocation runs, validates every required result, and atomically replaces the report only after the complete matrix succeeds.",
]
Path(output_path).write_text("\n".join(lines) + "\n")
PY

mv "$tmp" "$output"
trap - EXIT
rm -f "$raw" "$alloc_raw"
printf 'Wrote %s\n' "$output"
