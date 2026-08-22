#!/usr/bin/env python3

import argparse
import importlib.util
import sys
import tempfile
import unittest
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

    def test_go_selector_targets_one_native_cell(self) -> None:
        cell = matrix.Cell("fetch", "unix", "1B", 256, 128)
        self.assertEqual(
            matrix.go_benchmark_pattern(cell),
            "^Benchmark_Session_Fetch_Native$/^connector=session_bench$/"
            "^transport=unix$/^payload=1B$/^batch=256$/^concurrency=128$",
        )

        produce = matrix.Cell("produce", "grpc", "128B", 1, 16)
        self.assertEqual(
            matrix.go_benchmark_pattern(produce),
            "^Benchmark_Session_Produce_GRPC$/^connector=nop$/"
            "^payload=128B$/^batch=1$/^concurrency=16$",
        )

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
