#!/usr/bin/env bash
# Generate a self-contained Fujin Nop-connector benchmark report in Markdown.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${OUTPUT:-"$root/test/bench_report.md"}
benchtime=${BENCHTIME:-3s}
payloads=${FUJIN_BENCH_PAYLOAD:-1B,128B,1MiB}
concurrency=${FUJIN_BENCH_CONCURRENCY:-1,16,128}
deadline=${FUJIN_BENCH_DEADLINE:-5m}
bench_pattern='^Benchmark_Session_Produce_(Native|GRPC)$'
peak_iterations=${FUJIN_BENCH_PEAK_ITERATIONS:-1000000}

mkdir -p "$(dirname -- "$output")"
raw=$(mktemp "${output}.raw.XXXXXX")
tmp=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$raw" "$tmp"' EXIT

if ! go version >/dev/null 2>&1; then
	printf '%s\n' 'go is required' >&2
	exit 1
fi

if git -C "$root" diff --quiet && git -C "$root" diff --cached --quiet; then
	git_state=clean
else
	git_state=dirty
fi

if ! (
	cd "$root"
	FUJIN_BENCH_PAYLOAD="$payloads" \
	FUJIN_BENCH_CONCURRENCY="$concurrency" \
	FUJIN_BENCH_DEADLINE="$deadline" \
	FUJIN_BENCH_QUIET=1 \
	go test -tags=fujin,grpc -run '^$' -bench "$bench_pattern" \
		-benchtime="$benchtime" -count=1 -benchmem ./test
) >"$raw" 2>&1; then
	printf 'benchmark failed; preserved existing report at %s\n' "$output" >&2
	exit 1
fi

if ! (
	cd "$root"
	FUJIN_BENCH_QUIET=1 \
	go test -tags=fujin,grpc -run '^$' -bench '^Benchmark_Produce_1BPayload_Nop_TCP$' \
		-benchtime="${peak_iterations}x" -count=1 -benchmem ./test
) >>"$raw" 2>&1; then
	printf 'TCP peak benchmark failed; preserved existing report at %s\n' "$output" >&2
	exit 1
fi

if ! grep -q '^Benchmark_Produce_1BPayload_Nop_TCP-' "$raw"; then
	printf 'TCP peak benchmark produced no result; preserved existing report at %s\n' "$output" >&2
	exit 1
fi

for transport in tcp quic unix; do
	if ! grep -q "^Benchmark_Session_Produce_Native/connector=nop/transport=$transport/" "$raw"; then
		printf 'benchmark produced no Nop %s result; preserved existing report at %s\n' "$transport" "$output" >&2
		exit 1
	fi
done
if ! grep -q '^Benchmark_Session_Produce_GRPC/connector=nop/' "$raw"; then
	printf 'benchmark produced no Nop gRPC result; preserved existing report at %s\n' "$output" >&2
	exit 1
fi

{
	printf '%s\n' '# Fujin Nop Connector Performance Report' ''
	printf '**Generated:** %s  \n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf '**Source:** `%s` (%s)  \n' "$(git -C "$root" describe --always --dirty)" "$git_state"
	printf '**Environment:** `%s` on `%s`\n\n' "$(go version)" "$(uname -srm)"
	printf '%s\n' '## Scope' ''
	printf '%s\n' "The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Fujin's Session Core and wire adapters using the built-in **\`nop\` connector**. The connector accepts every message immediately and performs no broker I/O; these figures isolate Fujin’s protocol, session, scheduling, and callback overhead on localhost." ''
	printf '%s\n' '- **Transports:** native TCP, QUIC, Unix socket, and gRPC' "- **Synchronous matrix payloads:** $payloads" "- **Synchronous concurrent sessions:** $concurrency" '- **Synchronous batch:** 1 message per operation' "- **Synchronous sample duration:** $benchtime per subtest" "- **TCP pipeline peak:** 1 B payload, one session, $peak_iterations messages" ''
	printf '%s\n' '> These are single-host performance snapshots, not a cross-machine comparison or a broker durability benchmark. Run broker-backed tests separately when evaluating connector throughput and acknowledgement latency.' ''
	printf '%s\n' '## Synchronous request/response results' ''
	printf '%s\n' '| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |' '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
} >"$tmp"

awk '
/^Benchmark_Session_Produce_(Native|GRPC)\/connector=nop\// {
	name = $1
	ns = mbps = p99 = bytes = allocs = ""
	for (i = 2; i <= NF; i++) {
		if ($(i + 1) == "ns/op") ns = $i
		if ($(i + 1) == "MB/s") mbps = $i
		if ($(i + 1) == "p99-ns") p99 = $i
		if ($(i + 1) == "B/op") bytes = $i
		if ($(i + 1) == "allocs/op") allocs = $i
	}
	n = split(name, parts, "/")
	transport = "gRPC"
	payload = concurrency = ""
	for (i = 1; i <= n; i++) {
		if (parts[i] ~ /^transport=/) { sub(/^transport=/, "", parts[i]); transport = parts[i] }
		if (parts[i] ~ /^payload=/) { sub(/^payload=/, "", parts[i]); payload = parts[i] }
		if (parts[i] ~ /^concurrency=/) { sub(/^concurrency=/, "", parts[i]); sub(/-[0-9]+$/, "", parts[i]); concurrency = parts[i] }
	}
	if (!(name in seen)) order[++count] = name
	seen[name] = 1
	row[name] = sprintf("| %s | %s | %s | %.0f | %.3f | %s MB/s | %.2f µs | %s | %s |", transport, payload, concurrency, 1000000000 / ns, 1000 / ns, mbps, p99 / 1000, allocs, bytes)
}
END {
	for (i = 1; i <= count; i++) print row[order[i]]
}
' "$raw" >>"$tmp"

awk '
/^Benchmark_Produce_1BPayload_Nop_TCP-/ {
	ns = mbps = bytes = allocs = ""
	for (i = 2; i <= NF; i++) {
		if ($(i + 1) == "ns/op") ns = $i
		if ($(i + 1) == "MB/s") mbps = $i
		if ($(i + 1) == "B/op") bytes = $i
		if ($(i + 1) == "allocs/op") allocs = $i
	}
	if (ns > 0) printf "\n## TCP pipelined peak throughput\n\n| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |\n| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n| TCP | 1 B | One pipelined session | %s | %.0f | %.3f | %s MB/s | %s | %s |\n", iterations, 1000000000 / ns, 1000 / ns, mbps, allocs, bytes
}
' iterations="$peak_iterations" "$raw" >>"$tmp"

{
	printf '%s\n' '' '## Reproduce' '' '```bash' 'make bench-report' '```' '' 'Run a longer, focused sample:' '' '```bash' 'BENCHTIME=10s FUJIN_BENCH_PAYLOAD=1MiB FUJIN_BENCH_CONCURRENCY=128 make bench-report' '```' '' 'The generator is [`test/generate_bench_report.sh`](generate_bench_report.sh). It fails without a result for each native transport and gRPC, and writes the report atomically only after all benchmark subtests succeed.'
} >>"$tmp"

mv "$tmp" "$output"
trap - EXIT
rm -f "$raw"
printf 'Wrote %s\n' "$output"
