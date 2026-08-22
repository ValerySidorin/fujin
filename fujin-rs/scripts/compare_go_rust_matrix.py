#!/usr/bin/env python3
"""Run resumable, validated Go/Rust Session Core benchmark comparisons."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import math
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable

OPERATIONS = (
    "produce",
    "hproduce",
    "fetch",
    "hfetch",
    "subscribe",
    "hsubscribe",
    "ack",
    "nack",
    "transaction",
)
NATIVE_TRANSPORTS = ("tcp", "quic", "unix", "websocket")
TRANSPORTS = (*NATIVE_TRANSPORTS, "grpc")
PAYLOADS = {"1B": 1, "128B": 128, "1KiB": 1024, "32KiB": 32 * 1024, "1MiB": 1024 * 1024}
CONCURRENCY = (1, 16, 128)
BATCHES = (1, 32, 256)
BATCHED_OPERATIONS = frozenset(("fetch", "hfetch", "ack", "nack"))
MAX_BATCH_PAYLOAD_BYTES = 4 * 1024 * 1024
OPERATION_NAMES = {
    "produce": "Produce",
    "hproduce": "HProduce",
    "fetch": "Fetch",
    "hfetch": "HFetch",
    "subscribe": "Subscribe",
    "hsubscribe": "HSubscribe",
    "ack": "Ack",
    "nack": "Nack",
    "transaction": "Transaction",
}
GO_RESULT = re.compile(
    r"^(?P<name>Benchmark_Session_\S+)\s+\d+\s+"
    r"(?P<ns>[0-9.]+)\s+ns/op\s+"
    r"(?P<mb>[0-9.]+)\s+MB/s\s+"
    r"(?P<p99>[0-9.]+)\s+p99-ns\s+"
    r"(?P<bytes>[0-9.]+)\s+B/op\s+"
    r"(?P<allocs>[0-9.]+)\s+allocs/op$",
    re.MULTILINE,
)
RUST_RESULT = re.compile(
    r"^rust/(?:native|grpc)/\S+.*\s"
    r"ns/op=(?P<ns>[0-9]+)\s+"
    r"MB/s=(?P<mb>[0-9.]+)\s+"
    r"p99-ns=(?P<p99>[0-9]+)\s+"
    r"B/op=(?P<bytes>n/a|[0-9]+)\s+"
    r"allocs/op=(?P<allocs>n/a|[0-9.]+)$",
    re.MULTILINE,
)


@dataclasses.dataclass(frozen=True, order=True)
class Cell:
    operation: str
    transport: str
    payload: str
    batch: int
    concurrency: int

    @property
    def key(self) -> str:
        return "/".join(
            (self.operation, self.transport, self.payload, str(self.batch), str(self.concurrency))
        )

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def parse_csv(value: str, allowed: Iterable[str], name: str) -> tuple[str, ...]:
    allowed_set = set(allowed)
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    unknown = [value for value in values if value not in allowed_set]
    if not values or unknown:
        raise argparse.ArgumentTypeError(f"invalid {name}: {unknown or value!r}")
    return values


def parse_int_csv(value: str, allowed: Iterable[int], name: str) -> tuple[int, ...]:
    try:
        values = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid {name}: {value!r}") from error
    allowed_set = set(allowed)
    unknown = [item for item in values if item not in allowed_set]
    if not values or unknown:
        raise argparse.ArgumentTypeError(f"invalid {name}: {unknown or value!r}")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operations", default=",".join(OPERATIONS))
    parser.add_argument("--transports", default=",".join(TRANSPORTS))
    parser.add_argument("--payloads", default=",".join(PAYLOADS))
    parser.add_argument("--concurrency", default=",".join(map(str, CONCURRENCY)))
    parser.add_argument("--batches", default=",".join(map(str, BATCHES)))
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--small-operations", type=int, default=10_000)
    parser.add_argument("--large-operations", type=int, default=1_000)
    parser.add_argument("--allocation-operations", type=int, default=1_000)
    parser.add_argument("--deadline", default="60s")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--list-cells", action="store_true")
    args = parser.parse_args()
    args.operations = parse_csv(args.operations, OPERATIONS, "operations")
    args.transports = parse_csv(args.transports, TRANSPORTS, "transports")
    args.payloads = parse_csv(args.payloads, PAYLOADS, "payloads")
    args.concurrency = parse_int_csv(args.concurrency, CONCURRENCY, "concurrency")
    args.batches = parse_int_csv(args.batches, BATCHES, "batches")
    for name in ("samples", "small_operations", "large_operations"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.allocation_operations < 0:
        parser.error("--allocation-operations cannot be negative")
    minimum_operations = max(args.concurrency)
    if args.small_operations < minimum_operations or args.large_operations < minimum_operations:
        parser.error("timing operation counts must be at least the maximum concurrency")
    if 0 < args.allocation_operations < minimum_operations:
        parser.error("allocation operation count must be zero or at least maximum concurrency")
    if not re.fullmatch(r"[1-9][0-9]*s", args.deadline):
        parser.error("--deadline must be whole seconds, such as 60s")
    return args


def cells_for(args: argparse.Namespace) -> list[Cell]:
    cells: list[Cell] = []
    for operation in args.operations:
        for transport in args.transports:
            for payload in args.payloads:
                batches = args.batches if operation in BATCHED_OPERATIONS else (1,)
                for batch in batches:
                    if PAYLOADS[payload] * batch > MAX_BATCH_PAYLOAD_BYTES:
                        continue
                    for concurrency in args.concurrency:
                        cells.append(Cell(operation, transport, payload, batch, concurrency))
    return cells


def run_checked(
    command: list[str], cwd: Path, env: dict[str, str], timeout: int
) -> str:
    process = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"command failed ({process.returncode}): {' '.join(command)}\n{process.stdout}"
        )
    return process.stdout


def metrics_from_values(values: dict[str, str]) -> dict[str, float]:
    result = {
        "ns_per_operation": float(values["ns"]),
        "megabytes_per_second": float(values["mb"]),
        "p99_ns": float(values["p99"]),
    }
    for source, target in (
        ("bytes", "bytes_per_operation"),
        ("allocs", "allocations_per_operation"),
    ):
        if values[source] != "n/a":
            result[target] = float(values[source])
    return result


def parse_metrics(pattern: re.Pattern[str], output: str, runtime: str) -> dict[str, float]:
    matches = list(pattern.finditer(output))
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one {runtime} benchmark result, got {len(matches)}\n{output}"
        )
    return metrics_from_values(matches[0].groupdict())


def operations_for(cell: Cell, args: argparse.Namespace) -> int:
    return args.large_operations if cell.payload == "1MiB" else args.small_operations


def benchmark_environment(cell: Cell, operations: int, deadline: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "FUJIN_BENCH_OPERATION": cell.operation,
            "FUJIN_BENCH_TRANSPORT": cell.transport,
            "FUJIN_BENCH_PAYLOAD": cell.payload,
            "FUJIN_BENCH_BATCH": str(cell.batch),
            "FUJIN_BENCH_CONCURRENCY": str(cell.concurrency),
            "FUJIN_BENCH_OPERATIONS": str(operations),
            "FUJIN_BENCH_DEADLINE": deadline,
            "FUJIN_BENCH_QUIET": "1",
        }
    )
    return environment


def go_group_key(cell: Cell) -> tuple[str, str, str]:
    interface = "grpc" if cell.transport == "grpc" else "native"
    return (cell.operation, interface, cell.payload)


def grouped_cells(cells: list[Cell]) -> list[list[Cell]]:
    groups: dict[tuple[str, str, str], list[Cell]] = {}
    for cell in cells:
        groups.setdefault(go_group_key(cell), []).append(cell)
    return list(groups.values())


def go_benchmark_pattern(cell: Cell) -> str:
    suffix = "GRPC" if cell.transport == "grpc" else "Native"
    return f"^Benchmark_Session_{OPERATION_NAMES[cell.operation]}_{suffix}$"

def go_group_environment(cells: list[Cell], operations: int, deadline: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "FUJIN_BENCH_PAYLOAD": ",".join(dict.fromkeys(cell.payload for cell in cells)),
            "FUJIN_BENCH_BATCH": ",".join(
                map(str, dict.fromkeys(cell.batch for cell in cells))
            ),
            "FUJIN_BENCH_CONCURRENCY": ",".join(
                map(str, dict.fromkeys(cell.concurrency for cell in cells))
            ),
            "FUJIN_BENCH_OPERATIONS": str(operations),
            "FUJIN_BENCH_DEADLINE": deadline,
            "FUJIN_BENCH_QUIET": "1",
        }
    )
    return environment


def parse_go_results(output: str, operation: str) -> dict[Cell, dict[str, float]]:
    expected_native = f"Benchmark_Session_{OPERATION_NAMES[operation]}_Native"
    expected_grpc = f"Benchmark_Session_{OPERATION_NAMES[operation]}_GRPC"
    expected_connector = "nop" if operation == "produce" else "session_bench"
    results: dict[Cell, dict[str, float]] = {}
    for match in GO_RESULT.finditer(output):
        segments = match.group("name").split("/")
        benchmark = segments[0]
        if benchmark not in (expected_native, expected_grpc):
            continue
        attributes: dict[str, str] = {}
        for segment in segments[1:]:
            segment = re.sub(r"-[0-9]+$", "", segment)
            if "=" in segment:
                key, value = segment.split("=", 1)
                attributes[key] = value
        if attributes.get("connector") != expected_connector:
            continue
        transport = "grpc" if benchmark == expected_grpc else attributes.get("transport")
        try:
            cell = Cell(
                operation,
                str(transport),
                attributes["payload"],
                int(attributes["batch"]),
                int(attributes["concurrency"]),
            )
        except (KeyError, ValueError) as error:
            raise RuntimeError(f"malformed Go benchmark name {match.group('name')!r}") from error
        if cell in results:
            raise RuntimeError(f"duplicate Go benchmark result for {cell.key}")
        results[cell] = metrics_from_values(match.groupdict())
    return results


def run_go_group(
    root: Path, cells: list[Cell], operations: int, deadline: str
) -> dict[Cell, dict[str, float]]:
    if not cells or any(go_group_key(cell) != go_group_key(cells[0]) for cell in cells):
        raise RuntimeError("Go benchmark group contains mismatched cells")
    first = cells[0]
    output = run_checked(
        [
            "go",
            "test",
            "-tags=fujin,grpc",
            "-run",
            "^$",
            f"-bench={go_benchmark_pattern(first)}",
            f"-benchtime={operations}x",
            "-count=1",
            "-benchmem",
            "./test",
        ],
        root,
        go_group_environment(cells, operations, deadline),
        int(deadline[:-1]) + 120,
    )
    parsed = parse_go_results(output, first.operation)
    missing = [cell.key for cell in cells if cell not in parsed]
    if missing:
        raise RuntimeError(f"Go benchmark group omitted expected cells: {missing}\n{output}")
    return {cell: parsed[cell] for cell in cells}


def rust_binary(root: Path, cell: Cell, allocation: bool) -> Path:
    target = root / "fujin-rs" / ("target/full-matrix-alloc" if allocation else "target")
    name = "grpc-session-matrix-bench" if cell.transport == "grpc" else "session-matrix-bench"
    return target / "release" / name


def run_rust(
    root: Path, cell: Cell, operations: int, deadline: str, allocation: bool
) -> dict[str, float]:
    output = run_checked(
        [str(rust_binary(root, cell, allocation))],
        root / "fujin-rs",
        benchmark_environment(cell, operations, deadline),
        int(deadline[:-1]) + 120,
    )
    return parse_metrics(RUST_RESULT, output, "Rust")


def build_rust(root: Path, allocations: bool) -> None:
    command = [
        "cargo",
        "build",
        "--release",
        "-p",
        "fujin-server",
        "--bin",
        "session-matrix-bench",
        "--bin",
        "grpc-session-matrix-bench",
        "--features",
        "bench",
    ]
    run_checked(command, root / "fujin-rs", os.environ.copy(), 1800)
    if allocations:
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(root / "fujin-rs/target/full-matrix-alloc")
        command[-1] = "bench-alloc"
        run_checked(command, root / "fujin-rs", environment, 1800)


def command_output(command: list[str], cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def environment_fingerprint(root: Path) -> dict[str, str]:
    status = command_output(["git", "status", "--porcelain"], root)
    return {
        "source": command_output(["git", "rev-parse", "--short", "HEAD"], root)
        + ("-dirty" if status else ""),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "go": command_output(["go", "version"], root),
        "rustc": command_output(["rustc", "--version"], root),
        "cargo": command_output(["cargo", "--version"], root),
    }


def benchmark_configuration(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "operations": list(args.operations),
        "transports": list(args.transports),
        "payloads": list(args.payloads),
        "concurrency": list(args.concurrency),
        "batches": list(args.batches),
        "samples": args.samples,
        "small_operations": args.small_operations,
        "large_operations": args.large_operations,
        "allocation_operations": args.allocation_operations,
        "go_allocation_source": "timing_pass",
        "deadline": args.deadline,
        "interleaved": True,
        "max_batch_payload_bytes": MAX_BATCH_PAYLOAD_BYTES,
    }


def load_results(
    path: Path, expected_cells: list[Cell], expected_configuration: dict[str, Any]
) -> dict[str, Any]:
    if not path.exists():
        return {
            cell.key: {
                "cell": cell.as_dict(),
                "go": [],
                "rust": [],
                "go_alloc": [],
                "rust_alloc": [],
            }
            for cell in expected_cells
        }
    document = json.loads(path.read_text())
    if document.get("configuration") != expected_configuration:
        raise RuntimeError("existing output configuration does not match requested benchmark")
    results = {entry["key"]: entry for entry in document.get("results", [])}
    expected = {cell.key for cell in expected_cells}
    if set(results) != expected:
        raise RuntimeError("existing output matrix does not match requested cells")
    return results


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def median(sample: list[dict[str, float]], metric: str) -> float | None:
    values = [item[metric] for item in sample if metric in item]
    return statistics.median(values) if values else None


def ratio(rust: float | None, go: float | None) -> float | None:
    if rust is None or go is None or go <= 0:
        return None
    return rust / go


def mann_whitney_p(left: list[float], right: list[float]) -> float | None:
    if len(left) < 5 or len(right) < 5:
        return None
    tagged = [(value, 0) for value in left] + [(value, 1) for value in right]
    tagged.sort(key=lambda item: item[0])
    rank_sum = [0.0, 0.0]
    tie_term = 0
    index = 0
    while index < len(tagged):
        end = index + 1
        while end < len(tagged) and tagged[end][0] == tagged[index][0]:
            end += 1
        rank = (index + 1 + end) / 2
        for _, group in tagged[index:end]:
            rank_sum[group] += rank
        tied = end - index
        tie_term += tied**3 - tied
        index = end
    left_count = len(left)
    right_count = len(right)
    u_left = rank_sum[0] - left_count * (left_count + 1) / 2
    u_right = rank_sum[1] - right_count * (right_count + 1) / 2
    observed = min(u_left, u_right)
    total = left_count + right_count
    variance = left_count * right_count / 12 * (
        total + 1 - tie_term / (total * (total - 1))
    )
    if variance <= 0:
        return 1.0
    mean = left_count * right_count / 2
    z = (abs(observed - mean) - 0.5) / math.sqrt(variance)
    return math.erfc(max(0.0, z) / math.sqrt(2))


def geometric_mean(values: Iterable[float]) -> float | None:
    positive = [value for value in values if value > 0 and math.isfinite(value)]
    if not positive:
        return None
    return math.exp(sum(math.log(value) for value in positive) / len(positive))


def summarize(results: dict[str, Any]) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    for entry in results.values():
        go_ns = median(entry["go"], "ns_per_operation")
        rust_ns = median(entry["rust"], "ns_per_operation")
        go_p99 = median(entry["go"], "p99_ns")
        rust_p99 = median(entry["rust"], "p99_ns")
        go_bytes = median(entry["go_alloc"] or entry["go"], "bytes_per_operation")
        rust_bytes = median(entry["rust_alloc"], "bytes_per_operation")
        go_allocs = median(entry["go_alloc"] or entry["go"], "allocations_per_operation")
        rust_allocs = median(entry["rust_alloc"], "allocations_per_operation")
        go_ns_samples = [sample["ns_per_operation"] for sample in entry["go"]]
        rust_ns_samples = [sample["ns_per_operation"] for sample in entry["rust"]]
        cell = {
            **entry["cell"],
            "key": entry["key"],
            "samples": min(len(entry["go"]), len(entry["rust"])),
            "go_ns_per_operation": go_ns,
            "rust_ns_per_operation": rust_ns,
            "ns_ratio": ratio(rust_ns, go_ns),
            "ns_p_value": mann_whitney_p(go_ns_samples, rust_ns_samples),
            "go_p99_ns": go_p99,
            "rust_p99_ns": rust_p99,
            "p99_ratio": ratio(rust_p99, go_p99),
            "go_bytes_per_operation": go_bytes,
            "rust_bytes_per_operation": rust_bytes,
            "bytes_ratio": ratio(rust_bytes, go_bytes),
            "go_allocations_per_operation": go_allocs,
            "rust_allocations_per_operation": rust_allocs,
            "allocations_ratio": ratio(rust_allocs, go_allocs),
        }
        cells.append(cell)
    cells.sort(key=lambda item: item["key"])

    def group_summary(group: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "cells": len(group),
            "geomean_ns_ratio": geometric_mean(
                item["ns_ratio"] for item in group if item["ns_ratio"] is not None
            ),
            "geomean_p99_ratio": geometric_mean(
                item["p99_ratio"] for item in group if item["p99_ratio"] is not None
            ),
            "geomean_bytes_ratio": geometric_mean(
                item["bytes_ratio"] for item in group if item["bytes_ratio"] is not None
            ),
            "geomean_allocations_ratio": geometric_mean(
                item["allocations_ratio"]
                for item in group
                if item["allocations_ratio"] is not None
            ),
            "rust_faster_cells": sum(
                item["ns_ratio"] is not None and item["ns_ratio"] < 1 for item in group
            ),
            "rust_slower_10_percent_cells": sum(
                item["ns_ratio"] is not None and item["ns_ratio"] > 1.1 for item in group
            ),
            "significant_regressions": sum(
                item["ns_ratio"] is not None
                and item["ns_ratio"] > 1.1
                and item["ns_p_value"] is not None
                and item["ns_p_value"] < 0.05
                for item in group
            ),
        }

    return {
        "overall": group_summary(cells),
        "by_operation": {
            operation: group_summary([cell for cell in cells if cell["operation"] == operation])
            for operation in OPERATIONS
            if any(cell["operation"] == operation for cell in cells)
        },
        "by_transport": {
            transport: group_summary([cell for cell in cells if cell["transport"] == transport])
            for transport in TRANSPORTS
            if any(cell["transport"] == transport for cell in cells)
        },
        "worst_latency_cells": sorted(
            (cell for cell in cells if cell["ns_ratio"] is not None),
            key=lambda item: item["ns_ratio"],
            reverse=True,
        )[:20],
        "cells": cells,
    }


def format_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}x"


def markdown_report(document: dict[str, Any]) -> str:
    summary = document["summary"]
    lines = [
        "# Fujin Go/Rust Full Session Matrix Comparison",
        "",
        f"**Generated:** {document['generated_at']}",
        f"**Source:** `{document['environment']['source']}`",
        f"**Cells:** {summary['overall']['cells']}",
        "",
        "## Aggregate ratios",
        "",
        "Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.",
        "",
        "| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    groups = [("overall", summary["overall"])]
    groups.extend((f"operation/{name}", value) for name, value in summary["by_operation"].items())
    groups.extend((f"transport/{name}", value) for name, value in summary["by_transport"].items())
    for name, value in groups:
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                name,
                value["cells"],
                format_ratio(value["geomean_ns_ratio"]),
                format_ratio(value["geomean_p99_ratio"]),
                format_ratio(value["geomean_bytes_ratio"]),
                format_ratio(value["geomean_allocations_ratio"]),
                value["rust_faster_cells"],
                value["rust_slower_10_percent_cells"],
                value["significant_regressions"],
            )
        )
    lines.extend(
        (
            "",
            "## Worst median latency ratios",
            "",
            "| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |",
            "|---|---:|---:|---:|---:|---:|",
        )
    )
    for cell in summary["worst_latency_cells"]:
        p_value = cell["ns_p_value"]
        lines.append(
            "| `{}` | {} | {:.0f} | {:.0f} | {} | {} |".format(
                cell["key"],
                cell["samples"],
                cell["go_ns_per_operation"],
                cell["rust_ns_per_operation"],
                format_ratio(cell["ns_ratio"]),
                "n/a" if p_value is None else f"{p_value:.4f}",
            )
        )
    lines.extend(
        (
            "",
            "The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.",
            "",
        )
    )
    return "\n".join(lines)


def serialize(
    args: argparse.Namespace,
    cells: list[Cell],
    results: dict[str, Any],
    environment: dict[str, str],
) -> dict[str, Any]:
    entries = []
    for cell in cells:
        entry = results[cell.key]
        entries.append({"key": cell.key, **entry})
    document = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "environment": environment,
        "configuration": benchmark_configuration(args),
        "results": entries,
    }
    document["summary"] = summarize({entry["key"]: entry for entry in entries})
    return document


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    cells = cells_for(args)
    if args.list_cells:
        for cell in cells:
            print(cell.key)
        print(f"cells={len(cells)}")
        return 0
    if not args.no_build:
        build_rust(root, args.allocation_operations > 0)
    environment = environment_fingerprint(root)
    results = load_results(args.output_json, cells, benchmark_configuration(args))

    def persist() -> None:
        document = serialize(args, cells, results, environment)
        atomic_write(args.output_json, json.dumps(document, indent=2, sort_keys=True) + "\n")
        if args.output_markdown:
            atomic_write(args.output_markdown, markdown_report(document))

    for group_index, group in enumerate(grouped_cells(cells)):
        operations = operations_for(group[0], args)
        for sample in range(args.samples):
            timing_missing = {
                "go": [cell for cell in group if len(results[cell.key]["go"]) <= sample],
                "rust": [cell for cell in group if len(results[cell.key]["rust"]) <= sample],
            }
            timing_order = (
                ("go", "rust") if (group_index + sample) % 2 == 0 else ("rust", "go")
            )
            for runtime in timing_order:
                missing = timing_missing[runtime]
                if not missing:
                    continue
                if runtime == "go":
                    measured = run_go_group(root, group, operations, args.deadline)
                    for cell in missing:
                        results[cell.key]["go"].append(measured[cell])
                else:
                    for cell in missing:
                        results[cell.key]["rust"].append(
                            run_rust(root, cell, operations, args.deadline, False)
                        )
                persist()
            if timing_missing["go"] or timing_missing["rust"]:
                print(
                    f"timing {group[0].operation}/{go_group_key(group[0])[1]}/"
                    f"{go_group_key(group[0])[2]} "
                    f"cells={len(group)} sample={sample + 1}/{args.samples}",
                    flush=True,
                )

            if args.allocation_operations == 0:
                continue
            for cell in group:
                entry = results[cell.key]
                if len(entry["go_alloc"]) <= sample:
                    entry["go_alloc"].append(entry["go"][sample])
            rust_allocation_missing = [
                cell for cell in group if len(results[cell.key]["rust_alloc"]) <= sample
            ]
            for cell in rust_allocation_missing:
                results[cell.key]["rust_alloc"].append(
                    run_rust(
                        root,
                        cell,
                        args.allocation_operations,
                        args.deadline,
                        True,
                    )
                )
                persist()
            if rust_allocation_missing:
                print(
                    f"alloc  {group[0].operation}/{go_group_key(group[0])[1]}/"
                    f"{go_group_key(group[0])[2]} "
                    f"cells={len(group)} sample={sample + 1}/{args.samples}",
                    flush=True,
                )
    persist()
    print(f"wrote {args.output_json}")
    if args.output_markdown:
        print(f"wrote {args.output_markdown}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
