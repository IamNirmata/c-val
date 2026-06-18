"""Tests for DL rank JSON ingestion into metric DBs."""

from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.storage.dltest_ingest import find_dl_run_dirs, ingest_dltest_results, load_rank_files


def _write_rank_json(path: Path, rank: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "runID": f"20260618_example_RANK{rank}",
        "test_plan": "80gb-example",
        "nn_tasks": [
            {
                "task_name": "linear_float16",
                "status": "completed",
                "norm_output": 0.5 + rank,
                "weight": 0.1 + rank,
                "bias": 0.01 + rank,
                "fp_cpu_time": 10.0 + rank,
                "fp_gpu_time": 1.0 + rank,
                "bp_cpu_time": 11.0 + rank,
                "bp_gpu_time": 1.1 + rank,
            }
        ],
        "f_tasks": [
            {
                "task_name": "functional_float16",
                "status": "completed",
                "norm_output": 0.6 + rank,
                "weight": 0.2 + rank,
                "fp_cpu_time": 20.0 + rank,
                "fp_gpu_time": 2.0 + rank,
                "bp_cpu_time": 21.0 + rank,
                "bp_gpu_time": 2.1 + rank,
            }
        ],
        "coll_tasks": [
            {
                "task_name": "allreduce_float16",
                "status": "completed",
                "norm_output": 0.7 + rank,
                "cpu_time": 30.0 + rank,
                "gpu_time": 3.0 + rank,
            }
        ],
        "overlap_tasks": [
            {
                "task_name": "overlap_float16",
                "status": "completed",
                "coll_name": "allreduce",
                "layer_name": "linear",
                "coll_mean": 4.0 + rank,
                "coll_stdev": 0.4 + rank,
                "layer_mean": 5.0 + rank,
                "layer_stdev": 0.5 + rank,
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class DltestIngestTests(unittest.TestCase):
    def test_recursive_scanner_handles_remapped_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "dltest-slc01-cl02-hgx-0002-1781649558"
            _write_rank_json(
                run_dir
                / "workdir"
                / "test_plans"
                / "80gb-example"
                / "runs"
                / "example_RANK0.json",
                0,
            )

            self.assertEqual(find_dl_run_dirs(root), [run_dir])
            rank_files = list(load_rank_files(root))

        self.assertEqual(len(rank_files), 1)
        self.assertEqual(rank_files[0].node, "slc01-cl02-hgx-0002")
        self.assertEqual(rank_files[0].cval_timestamp, 1781649558)
        self.assertEqual(rank_files[0].rank, 0)

    def test_recursive_scanner_handles_nested_continuous_validation_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "slc01-cl02-hgx-0005" / "dltest-slc01-cl02-hgx-0005-1781655211"
            _write_rank_json(
                run_dir
                / "workdir"
                / "test_plans"
                / "80gb-example"
                / "runs"
                / "example_RANK1.json",
                1,
            )

            self.assertEqual(find_dl_run_dirs(root), [run_dir])
            rank_files = list(load_rank_files(root))

        self.assertEqual(rank_files[0].node, "slc01-cl02-hgx-0005")
        self.assertEqual(rank_files[0].rank, 1)

    def test_ingest_writes_four_metric_dbs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output_dir = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-slc01-cl02-hgx-0002-1781649558"
            for rank in range(2):
                _write_rank_json(
                    run_dir
                    / "workdir"
                    / "test_plans"
                    / "80gb-example"
                    / "runs"
                    / f"example_RANK{rank}.json",
                    rank,
                )

            summary = ingest_dltest_results(root, output_dir)

            self.assertEqual(summary["rank_files"], 2)
            self.assertEqual(summary["runs"], 1)
            expected_tables = {
                "dltest_numerical_correctness.db": "numerical_correctness",
                "dltest_compute_performance.db": "compute_performance",
                "dltest_collective_performance.db": "collective_performance",
                "dltest_overlap_performance.db": "overlap_performance",
            }
            for db_name, table_name in expected_tables.items():
                with sqlite3.connect(output_dir / db_name) as connection:
                    count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.assertGreater(count, 0)

    def test_ingest_raises_when_no_rank_jsons_exist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                ingest_dltest_results(Path(tmpdir), Path(tmpdir) / "out")


if __name__ == "__main__":
    unittest.main()
