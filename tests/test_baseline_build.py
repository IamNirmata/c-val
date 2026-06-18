"""Tests for dynamic baseline building from result DBs and versioned storage."""

import sqlite3
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.baselines import build, stats
from cval.baselines.storage import (
    activate_baseline,
    default_classification_db_path,
    default_dynamic_baseline_db_path,
    get_active_baseline,
    list_dynamic_baselines,
    load_dynamic_baseline,
    store_classification_results,
    store_dynamic_baseline,
)
from cval.config import BaselineClassificationConfig, CvalConfig, load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS

NOW = int(time.time())


def _make_storage_db(path: Path, n_rows: int = 14) -> None:
    columns_ddl = ", ".join(f"{column} REAL" for column in STORAGE_METRIC_COLUMNS)
    insert_columns = ", ".join(STORAGE_METRIC_COLUMNS)
    placeholders = ", ".join("?" for _ in STORAGE_METRIC_COLUMNS)
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE storage_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                image_name TEXT NOT NULL DEFAULT '',
                {columns_ddl},
                PRIMARY KEY (node, timestamp)
            )
            """
        )
        for i in range(n_rows):
            # Cluster around a per-column base with small variation; row 0 is an
            # outlier the trimming step should drop.
            factor = 0.4 if i == 0 else 1.0 + (i % 5 - 2) * 0.01
            values = [1000.0 * (c + 1) * factor for c in range(len(STORAGE_METRIC_COLUMNS))]
            connection.execute(
                f"""
                INSERT INTO storage_performance
                (node, timestamp, image_name, {insert_columns})
                VALUES (?, ?, ?, {placeholders})
                """,
                ("node-a", NOW - i * 60, "img:1", *values),
            )
        connection.commit()


def _make_nccl_db(path: Path, n_rows: int = 12) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE nccl_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                image_name TEXT NOT NULL DEFAULT '',
                busbw REAL,
                latency REAL,
                PRIMARY KEY (node, timestamp)
            )
            """
        )
        for i in range(n_rows):
            busbw = 500.0 + (i % 5 - 2) * 1.0
            latency = 25.0 + (i % 5 - 2) * 0.1
            connection.execute(
                "INSERT INTO nccl_performance (node, timestamp, image_name, busbw, latency) "
                "VALUES (?, ?, ?, ?, ?)",
                ("node-a", NOW - i * 60, "img:1", busbw, latency),
            )
        connection.commit()


def _make_dl_standard_db(path: Path, table: str, rows: list[dict]) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE {table} (
                run_key TEXT NOT NULL,
                node TEXT,
                cval_timestamp INTEGER,
                sample_dir TEXT NOT NULL,
                test_plan TEXT NOT NULL,
                dltest_run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                task_group TEXT NOT NULL,
                task_name TEXT NOT NULL,
                status TEXT,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                source_file TEXT NOT NULL,
                PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
            )
            """
        )
        for row in rows:
            connection.execute(
                f"""
                INSERT INTO {table} (
                    run_key, node, cval_timestamp, sample_dir, test_plan,
                    dltest_run_id, rank, task_group, task_name, status,
                    metric_name, metric_value, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["run_key"],
                    "node-a",
                    NOW - row.get("age", 0) * 60,
                    "/tmp/sample",
                    row.get("test_plan", "80gb-example"),
                    row["run_key"],
                    row["rank"],
                    row["task_group"],
                    row["task_name"],
                    "completed",
                    row["metric_name"],
                    row["metric_value"],
                    "rank.json",
                ),
            )
        connection.commit()


class TestBuildStorageBaseline(unittest.TestCase):
    def test_builds_low_bad_metrics(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage.db"
            _make_storage_db(db_path)

            record = build.build_storage_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5
            )

            self.assertEqual(record["test_type"], "storage")
            self.assertTrue(record["metrics"])
            first = record["metrics"]["iodepth_read_1file_iops"]
            self.assertEqual(first["direction"], stats.DIRECTION_LOW_BAD)
            self.assertIsNone(first["upper_bound"])  # higher is better
            self.assertAlmostEqual(first["median"], 1000.0, delta=50.0)


class TestBuildNcclBaseline(unittest.TestCase):
    def test_busbw_and_latency_directions(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            _make_nccl_db(db_path)

            record = build.build_nccl_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5
            )

            self.assertIn("busbw", record["metrics"])
            self.assertIn("latency", record["metrics"])
            self.assertIsNone(record["metrics"]["busbw"]["upper_bound"])
            self.assertIsNone(record["metrics"]["latency"]["lower_bound"])


class TestBuildDlBaseline(unittest.TestCase):
    def test_numerical_keeps_rank_compute_pools_rank(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            numerical_db = Path(tmpdir) / "num.db"
            compute_db = Path(tmpdir) / "compute.db"

            numerical_rows = [
                {
                    "run_key": f"run{i}",
                    "rank": 0,
                    "task_group": "nn_tasks",
                    "task_name": "layerA",
                    "metric_name": "norm_output",
                    "metric_value": 0.5 + i * 1e-6,
                    "age": i,
                }
                for i in range(8)
            ]
            compute_rows = [
                {
                    "run_key": f"run{i // 2}",
                    "rank": i % 8,
                    "task_group": "nn_tasks",
                    "task_name": "layerA",
                    "metric_name": "fp_gpu_time",
                    "metric_value": 10.0 + (i % 3) * 0.2,
                    "age": i,
                }
                for i in range(10)
            ]
            _make_dl_standard_db(numerical_db, "numerical_correctness", numerical_rows)
            _make_dl_standard_db(compute_db, "compute_performance", compute_rows)

            record = build.build_dl_baseline(
                config=config,
                db_paths={
                    "numerical_correctness": str(numerical_db),
                    "compute_performance": str(compute_db),
                    "collective_performance": str(Path(tmpdir) / "missing1.db"),
                    "overlap_performance": str(Path(tmpdir) / "missing2.db"),
                },
                window_days=365,
                min_samples=5,
            )

            metrics = record["metrics"]
            self.assertIn("nn_tasks/layerA/rank0/norm_output", metrics)
            self.assertIn("nn_tasks/layerA/fp_gpu_time", metrics)
            # numerical is two-sided; compute time is high-bad (lower is better).
            self.assertEqual(
                metrics["nn_tasks/layerA/rank0/norm_output"]["direction"],
                stats.DIRECTION_TWO_SIDED,
            )
            self.assertEqual(
                metrics["nn_tasks/layerA/fp_gpu_time"]["direction"],
                stats.DIRECTION_HIGH_BAD,
            )

    def test_missing_dbs_yield_empty_metrics(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            record = build.build_dl_baseline(
                config=config,
                db_paths={
                    "numerical_correctness": str(Path(tmpdir) / "a.db"),
                    "compute_performance": str(Path(tmpdir) / "b.db"),
                    "collective_performance": str(Path(tmpdir) / "c.db"),
                    "overlap_performance": str(Path(tmpdir) / "d.db"),
                },
                window_days=365,
                min_samples=5,
            )
            self.assertEqual(record["metrics"], {})


class TestVersionedBaselineStorage(unittest.TestCase):
    def _record(self, baseline_id: str) -> dict:
        return {
            "schema_version": "cval.baseline.v2",
            "baseline_id": baseline_id,
            "test_type": "nccl",
            "stratum_key": "",
            "window_days": 30,
            "created_at": NOW,
            "timestamp": NOW,
            "n_samples": 12,
            "method": "robust_mad",
            "metrics": {"busbw": {"median": 500.0, "direction": "low_bad"}},
        }

    def test_store_candidate_then_activate(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)

            # Stored as candidate -> no active baseline yet.
            self.assertIsNone(get_active_baseline("nccl", db_path=db_path))

            listed = list_dynamic_baselines("nccl", db_path=db_path)
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0][2], "candidate")

            self.assertTrue(activate_baseline("nccl-all-1", "nccl", db_path=db_path))
            active = get_active_baseline("nccl", db_path=db_path)
            self.assertIsNotNone(active)
            self.assertEqual(active["baseline_id"], "nccl-all-1")

    def test_activate_supersedes_previous(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)
            store_dynamic_baseline(self._record("nccl-all-2"), db_path=db_path)

            activate_baseline("nccl-all-1", "nccl", db_path=db_path)
            activate_baseline("nccl-all-2", "nccl", db_path=db_path)

            active = get_active_baseline("nccl", db_path=db_path)
            self.assertEqual(active["baseline_id"], "nccl-all-2")

            statuses = {
                row[0]: row[2]
                for row in list_dynamic_baselines("nccl", db_path=db_path)
            }
            self.assertEqual(statuses["nccl-all-1"], "superseded")
            self.assertEqual(statuses["nccl-all-2"], "active")

    def test_load_dynamic_baseline_roundtrip(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)
            loaded = load_dynamic_baseline("nccl-all-1", "nccl", db_path=db_path)
            self.assertEqual(loaded["metrics"]["busbw"]["median"], 500.0)


class TestBaselineRootStorage(unittest.TestCase):
    def _config(self, root: str) -> CvalConfig:
        return CvalConfig(baseline=BaselineClassificationConfig(baseline_root_path=root))

    def test_default_baseline_db_paths(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            self.assertEqual(
                default_dynamic_baseline_db_path("storage", config=config),
                Path(tmpdir) / "test-storage-baselines.db",
            )
            self.assertEqual(
                default_dynamic_baseline_db_path("nccl", config=config),
                Path(tmpdir) / "test-nccl-baselines.db",
            )
            self.assertEqual(
                default_dynamic_baseline_db_path(
                    "dltest", "numerical_correctness", config=config
                ),
                Path(tmpdir) / "dltest_numerical_correctness-baselines.db",
            )
            self.assertEqual(
                default_classification_db_path(config),
                Path(tmpdir) / "classification-results.db",
            )

    def test_store_dl_baseline_splits_into_four_component_dbs(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            record = {
                "schema_version": "cval.baseline.v2",
                "baseline_id": "dl-all-1",
                "test_type": "dltest",
                "stratum_key": "test_plan=80gb-example",
                "window_days": 30,
                "created_at": NOW,
                "timestamp": NOW,
                "n_samples": 10,
                "method": "robust_mad",
                "metrics": {
                    "nn/layer/rank0/norm_output": {
                        "median": 0.5,
                        "direction": "two_sided",
                        "source_table": "numerical_correctness",
                    },
                    "nn/layer/fp_gpu_time": {
                        "median": 10.0,
                        "direction": "high_bad",
                        "source_table": "compute_performance",
                    },
                    "coll/layer/gpu_time": {
                        "median": 20.0,
                        "direction": "high_bad",
                        "source_table": "collective_performance",
                    },
                    "overlap/task/coll_mean": {
                        "median": 1.2,
                        "direction": "two_sided",
                        "source_table": "overlap_performance",
                    },
                },
            }

            store_dynamic_baseline(record, config=config)
            activate_baseline("dl-all-1", "dltest", config=config)
            merged = get_active_baseline("dltest", config=config)

            expected_files = {
                "dltest_numerical_correctness-baselines.db",
                "dltest_compute_performance-baselines.db",
                "dltest_collective_performance-baselines.db",
                "dltest_overlap_performance-baselines.db",
            }
            self.assertEqual({path.name for path in Path(tmpdir).glob("*.db")}, expected_files)
            self.assertEqual(len(merged["metrics"]), 4)
            self.assertEqual(sorted(merged["components"]), sorted([
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ]))

    def test_store_classification_results(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            verdicts = [
                {
                    "node": "node-a",
                    "test_type": "storage",
                    "baseline_id": "storage-1",
                    "status": "normal",
                    "n_compared": 12,
                    "n_degraded": 0,
                    "n_improved": 0,
                    "metrics": [],
                },
                {
                    "node": "node-b",
                    "test_type": "storage",
                    "baseline_id": "storage-1",
                    "status": "degraded",
                    "n_compared": 12,
                    "n_degraded": 2,
                    "n_improved": 0,
                    "metrics": [{"metric": "x"}],
                },
            ]

            count = store_classification_results(verdicts, classified_at=NOW, config=config)

            self.assertEqual(count, 2)
            with sqlite3.connect(default_classification_db_path(config)) as connection:
                rows = connection.execute(
                    "SELECT node, status, passed FROM classification_results ORDER BY node"
                ).fetchall()
            self.assertEqual(rows, [("node-a", "normal", 1), ("node-b", "degraded", 0)])


if __name__ == "__main__":
    unittest.main()
