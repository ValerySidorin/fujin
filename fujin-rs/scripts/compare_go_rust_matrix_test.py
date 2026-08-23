#!/usr/bin/env python3

import argparse
import importlib.util
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

SCRIPT = Path(__file__).with_name("compare_go_rust_matrix.py")
SPEC = importlib.util.spec_from_file_location("compare_go_rust_matrix", SCRIPT)
assert SPEC and SPEC.loader
matrix = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = matrix
SPEC.loader.exec_module(matrix)


class MatrixDriverTest(unittest.TestCase):
    def arguments(self) -> argparse.Namespace:
        return argparse.Namespace(
            operations=matrix.OPERATIONS,
            transports=matrix.TRANSPORTS,
            payloads=tuple(matrix.PAYLOADS),
            concurrency=matrix.CONCURRENCY,
            batches=matrix.BATCHES,
            samples=1,
            small_operations=10_000,
            large_operations=1_000,
            allocation_operations=1_000,
            deadline="60s",
        )

    def test_full_contract_contains_1095_bounded_cells(self) -> None:
        cells = matrix.cells_for(self.arguments())
        self.assertEqual(len(cells), 1095)
        self.assertTrue(
            all(
                matrix.PAYLOADS[cell.payload] * cell.batch
                <= matrix.MAX_BATCH_PAYLOAD_BYTES
                for cell in cells
            )
        )

    def test_go_grouping_isolates_quic_processes(self) -> None:
        tcp = matrix.Cell("fetch", "tcp", "1B", 256, 128)
        quic = matrix.Cell("fetch", "quic", "1B", 256, 128)
        grpc = matrix.Cell("fetch", "grpc", "1B", 256, 128)
        larger = matrix.Cell("fetch", "tcp", "128B", 256, 128)
        self.assertEqual(
            matrix.grouped_cells([tcp, quic, grpc, larger]),
            [[tcp], [quic], [grpc], [larger]],
        )
        large_batch_one = matrix.Cell("fetch", "tcp", "32KiB", 1, 16)
        large_batch_many = matrix.Cell("fetch", "tcp", "32KiB", 32, 16)
        large_other_transport = matrix.Cell("fetch", "quic", "32KiB", 1, 16)
        self.assertNotEqual(
            matrix.go_group_key(large_batch_one),
            matrix.go_group_key(large_batch_many),
        )
        self.assertNotEqual(
            matrix.go_group_key(large_batch_one),
            matrix.go_group_key(large_other_transport),
        )
        self.assertEqual(
            matrix.go_benchmark_pattern(tcp), "^Benchmark_Session_Fetch_Native$"
        )
        self.assertEqual(
            matrix.go_benchmark_pattern(grpc), "^Benchmark_Session_Fetch_GRPC$"
        )

    def test_go_group_environment_filters_native_transports(self) -> None:
        cells = [
            matrix.Cell("subscribe", "tcp", "1MiB", 1, 128),
            matrix.Cell("subscribe", "websocket", "1MiB", 1, 128),
        ]
        environment = matrix.go_group_environment(cells, 1000, "300s")
        self.assertEqual(environment["FUJIN_BENCH_TRANSPORT"], "tcp,websocket")

    def test_go_parser_returns_every_transport_cell(self) -> None:
        output = "\n".join(
            (
                "Benchmark_Session_Fetch_Native/connector=session_bench/transport=tcp/"
                "payload=1B/batch=256/concurrency=128-8 1000 5000 ns/op 51.20 MB/s "
                "100000 p99-ns 20000 B/op 500 allocs/op",
                "Benchmark_Session_Fetch_Native/connector=session_bench/transport=quic/"
                "payload=1B/batch=256/concurrency=128-8 1000 7000 ns/op 36.57 MB/s "
                "200000 p99-ns 21000 B/op 510 allocs/op",
            )
        )
        parsed = matrix.parse_go_results(output, "fetch")
        self.assertEqual(len(parsed), 2)
        self.assertEqual(
            parsed[matrix.Cell("fetch", "tcp", "1B", 256, 128)]["ns_per_operation"],
            5000,
        )

    def test_rust_runner_reports_the_failed_cell(self) -> None:
        cell = matrix.Cell("fetch", "unix", "32KiB", 32, 128)
        with mock.patch.object(matrix, "run_checked", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, cell.key):
                matrix.run_rust(Path("/tmp"), cell, 1000, "30s", False)

    def test_result_parser_requires_exactly_one_valid_row(self) -> None:
        row = (
            "rust/native/fetch transport=unix payload=1B batch=256 concurrency=128 "
            "operations=1000 ns/op=3757 MB/s=68.13 p99-ns=649750 "
            "B/op=24064 allocs/op=5.55\n"
        )
        metrics = matrix.parse_metrics(matrix.RUST_RESULT, row, "Rust")
        self.assertEqual(metrics["ns_per_operation"], 3757)
        self.assertEqual(metrics["allocations_per_operation"], 5.55)
        with self.assertRaises(RuntimeError):
            matrix.parse_metrics(matrix.RUST_RESULT, row + row, "Rust")

    def test_mann_whitney_flags_separated_samples(self) -> None:
        self.assertLess(
            matrix.mann_whitney_p(
                [100, 101, 102, 103, 104], [200, 201, 202, 203, 204]
            ),
            0.05,
        )
        self.assertEqual(matrix.mann_whitney_p([100] * 5, [100] * 5), 1.0)

    def test_resume_rejects_configuration_changes(self) -> None:
        arguments = self.arguments()
        cells = matrix.cells_for(arguments)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.json"
            path.write_text('{"configuration": {}, "results": []}')
            with self.assertRaises(RuntimeError):
                matrix.load_results(
                    path, cells, matrix.benchmark_configuration(arguments)
                )


if __name__ == "__main__":
    unittest.main()
