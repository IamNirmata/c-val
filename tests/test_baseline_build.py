"""Tests for dynamic baseline building from result DBs and versioned storage."""

import json
import sqlite3
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cval.baselines import build, stats, storage
from cval.baselines.storage import (
    activate_baseline,
    default_classification_db_path,
    default_dynamic_baseline_db_path,
    get_active_baseline,
    global_classification_db_path,
    list_dynamic_baselines,
    load_dynamic_baseline,
    store_classification_results,
    store_dynamic_baseline,
    validate_default_baseline_db_paths,
)
from cval.config import BaselineClassificationConfig, CvalConfig, load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS

NOW = int(time.time())


def _make_storage_db(path: Path, n_rows: int = 14) -> None:
    columns_ddl = ", ".join(f"{column} REAL" for column in STORAGE_METRIC_COLUMNS)
    insert_columns = ", ".join(STORAGE_METRIC_COLUMNS)
    placeholders = ", ".join("?" for _ in STORAGE_METRIC_COLUMNS)
    with closing(sqlite3.connect(path)) as connection:
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


def _make_dl_standard_db(path: Path, table: str, rows: list[dict]) -> None:
    with closing(sqlite3.connect(path)) as connection:
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

    def test_insufficient_storage_samples_are_rejected_before_storage(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage.db"
            output = Path(tmpdir) / "baseline.db"
            _make_storage_db(db_path, n_rows=2)
            record = build.build_storage_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5
            )
            self.assertEqual(record["metrics"], {})
            with self.assertRaisesRegex(ValueError, "metrics must be non-empty"):
                store_dynamic_baseline(record, db_path=output, config=config)
            self.assertFalse(output.exists())


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
            with self.assertRaisesRegex(ValueError, "metrics must be non-empty"):
                store_dynamic_baseline(record, config=config)
            self.assertEqual(list(Path(tmpdir).glob("*-baselines.db")), [])


class TestVersionedBaselineStorage(unittest.TestCase):
    def _record(self, baseline_id: str) -> dict:
        metric = stats.summarize_metric(
            "busbw",
            [498.0, 499.0, 500.0, 501.0, 502.0],
            direction=stats.DIRECTION_LOW_BAD,
            tolerance_pct=5.0,
            bootstrap=False,
        ).to_dict()
        metric["source_table"] = "IB_HEALTH"
        return {
            "schema_version": "cval.baseline.v2",
            "baseline_id": baseline_id,
            "test_type": "storage",
            "stratum_key": "",
            "window_days": 30,
            "created_at": NOW,
            "timestamp": NOW,
            "n_samples": 12,
            "method": "robust_mad",
            "metrics": {"busbw": metric},
        }

    def test_store_candidate_then_activate(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)

            # Stored as candidate -> no active baseline yet.
            self.assertIsNone(get_active_baseline("storage", db_path=db_path))

            listed = list_dynamic_baselines("storage", db_path=db_path)
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0][2], "candidate")

            self.assertTrue(activate_baseline("nccl-all-1", "storage", db_path=db_path))
            active = get_active_baseline("storage", db_path=db_path)
            self.assertIsNotNone(active)
            self.assertEqual(active["baseline_id"], "nccl-all-1")

    def test_activate_supersedes_previous(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)
            store_dynamic_baseline(self._record("nccl-all-2"), db_path=db_path)

            activate_baseline("nccl-all-1", "storage", db_path=db_path)
            activate_baseline("nccl-all-2", "storage", db_path=db_path)

            active = get_active_baseline("storage", db_path=db_path)
            self.assertEqual(active["baseline_id"], "nccl-all-2")

            statuses = {
                row[0]: row[2]
                for row in list_dynamic_baselines("storage", db_path=db_path)
            }
            self.assertEqual(statuses["nccl-all-1"], "superseded")
            self.assertEqual(statuses["nccl-all-2"], "active")

    def test_rejected_candidate_preserves_existing_active_baseline(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            active = self._record("nccl-active")
            store_dynamic_baseline(active, db_path=db_path)
            activate_baseline("nccl-active", "storage", db_path=db_path)
            invalid = self._record("nccl-empty")
            invalid["metrics"] = {}
            invalid["n_samples"] = 0

            with self.assertRaisesRegex(ValueError, "metrics must be non-empty"):
                store_dynamic_baseline(invalid, db_path=db_path)

            self.assertEqual(
                get_active_baseline("storage", db_path=db_path)["baseline_id"],
                "nccl-active",
            )
            self.assertEqual(len(list_dynamic_baselines("storage", db_path=db_path)), 1)

    def test_store_rejects_nonempty_baseline_below_configured_minimum(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            record = self._record("nccl-too-small")
            record["n_samples"] = load_config().baseline.min_samples - 1

            with self.assertRaisesRegex(ValueError, "below the configured minimum"):
                store_dynamic_baseline(record, db_path=db_path)

            self.assertFalse(db_path.exists())

    def test_activation_rejects_empty_candidate_and_preserves_active(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-active"), db_path=db_path)
            store_dynamic_baseline(self._record("nccl-invalid"), db_path=db_path)
            activate_baseline("nccl-active", "storage", db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                invalid = self._record("nccl-invalid")
                invalid["metrics"] = {}
                connection.execute(
                    "UPDATE baselines SET metrics_json=? "
                    "WHERE baseline_id='nccl-invalid' AND test_type='storage'",
                    (json.dumps(invalid),),
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "metrics must be non-empty"):
                activate_baseline("nccl-invalid", "storage", db_path=db_path)

            self.assertEqual(
                get_active_baseline("storage", db_path=db_path)["baseline_id"],
                "nccl-active",
            )

    def test_load_dynamic_baseline_roundtrip(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)
            loaded = load_dynamic_baseline("nccl-all-1", "storage", db_path=db_path)
            self.assertEqual(loaded["metrics"]["busbw"]["median"], 500.0)

    def test_exact_retry_preserves_active_lifecycle_and_changed_id_conflicts(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation %?#.db"
            record = self._record("nccl-all-1")
            store_dynamic_baseline(record, db_path=db_path)
            self.assertTrue(activate_baseline("nccl-all-1", "storage", db_path=db_path))

            self.assertEqual(
                store_dynamic_baseline(record, db_path=db_path),
                "nccl-all-1",
            )
            statuses = list_dynamic_baselines("storage", db_path=db_path)
            self.assertEqual(statuses[0][2], "active")

            changed = dict(record)
            changed["n_samples"] = 13
            with self.assertRaisesRegex(ValueError, "different content"):
                store_dynamic_baseline(changed, db_path=db_path)
            self.assertEqual(
                list_dynamic_baselines("storage", db_path=db_path)[0][2],
                "active",
            )

    def test_many_concurrent_exact_writers_serialize_first_use_schema(self):
        with TemporaryDirectory() as tmpdir:
            writer_count = 12
            for round_index in range(8):
                db_path = Path(tmpdir) / f"fresh-concurrent-{round_index}.db"
                baseline_id = f"nccl-concurrent-{round_index}"
                record = self._record(baseline_id)
                barrier = threading.Barrier(writer_count)

                def write() -> str:
                    barrier.wait(timeout=10)
                    return store_dynamic_baseline(record, db_path=db_path)

                with ThreadPoolExecutor(max_workers=writer_count) as executor:
                    futures = [executor.submit(write) for _index in range(writer_count)]
                    results = [future.result(timeout=40) for future in futures]

                self.assertEqual(results, [baseline_id] * writer_count)
                with closing(sqlite3.connect(db_path)) as connection:
                    rows = connection.execute(
                        "SELECT baseline_id, status, metrics_json FROM baselines "
                        "WHERE baseline_id=? AND test_type='storage'",
                        (baseline_id,),
                    ).fetchall()
                    columns = [
                        row[1]
                        for row in connection.execute("PRAGMA table_info(baselines)")
                    ]

                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][1], "candidate")
                self.assertEqual(len(columns), len(set(columns)))
                self.assertTrue(
                    {name for name, _definition in storage._DYNAMIC_COLUMNS}.issubset(columns)
                )

    def test_unscoped_override_list_filters_disabled_or_unknown_targets(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mixed.db"
            store_dynamic_baseline(self._record("nccl-all-1"), db_path=db_path)
            unknown = dict(self._record("unknown-all-1"))
            unknown["test_type"] = "unknown"
            store_dynamic_baseline(unknown, db_path=db_path)

            rows = list_dynamic_baselines(db_path=db_path, config=load_config())

        self.assertEqual({row[1] for row in rows}, {"storage"})

    def test_generic_sqlite_api_rejects_nccl_case_variants_and_explicit_paths(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "legacy-nccl-baselines.db"
            record = self._record("legacy-nccl")
            record["test_type"] = "NCCL"
            with self.assertRaisesRegex(ValueError, "only by cval.nccl_eval"):
                store_dynamic_baseline(record, db_path=db_path)
            self.assertFalse(db_path.exists())
            for operation in (
                lambda: load_dynamic_baseline("legacy", "NcCl", db_path=db_path),
                lambda: get_active_baseline("NCCL", db_path=db_path),
                lambda: list_dynamic_baselines("nccl", db_path=db_path),
            ):
                with self.subTest(operation=operation), self.assertRaisesRegex(
                    ValueError, "only by cval.nccl_eval"
                ):
                    operation()


class TestBaselineRootStorage(unittest.TestCase):
    def _config(self, root: str) -> CvalConfig:
        return replace(
            load_config(),
            baseline=BaselineClassificationConfig(baseline_root_path=root),
        )

    def test_default_baseline_db_paths(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            self.assertEqual(
                default_dynamic_baseline_db_path("storage", config=config),
                Path(tmpdir) / "test-storage-baselines.db",
            )
            with self.assertRaisesRegex(ValueError, "only by cval.nccl_eval"):
                default_dynamic_baseline_db_path("nccl", config=config)
            with self.assertRaisesRegex(ValueError, "only by cval.nccl_eval"):
                default_classification_db_path("nccl", config=config)
            self.assertEqual(
                default_dynamic_baseline_db_path(
                    "dltest", "numerical_correctness", config=config
                ),
                Path(tmpdir) / "dltest_numerical_correctness-baselines.db",
            )
            self.assertEqual(
                default_classification_db_path("storage", config),
                Path(tmpdir) / "storage-classifications.db",
            )

    def test_generic_plugin_names_are_prefixed_and_default_paths_are_unique(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            self.assertEqual(
                default_dynamic_baseline_db_path("synthetic", config=config),
                Path(tmpdir) / "plugin-synthetic-baselines.db",
            )
            self.assertNotEqual(
                default_dynamic_baseline_db_path("test-storage", config=config),
                default_dynamic_baseline_db_path("storage", config=config),
            )
            validate_default_baseline_db_paths(load_config())

    def test_default_path_collision_validation_fails_closed(self):
        with patch.dict(
            "cval.baselines.storage.BASELINE_DB_FILENAMES",
            {"storage": "same.db"},
            clear=True,
        ), patch.dict(
            "cval.baselines.storage.DL_BASELINE_DB_FILENAMES",
            {
                "numerical_correctness": "same.db",
                "compute_performance": "dl-compute.db",
                "collective_performance": "dl-collective.db",
                "overlap_performance": "dl-overlap.db",
            },
            clear=True,
        ), self.assertRaisesRegex(ValueError, "collide"):
            validate_default_baseline_db_paths(load_config())

    def test_store_dl_baseline_splits_into_four_component_dbs(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            def metric(
                name: str,
                direction: str,
                source_table: str,
                center: float,
            ) -> dict:
                value = stats.summarize_metric(
                    name,
                    [center] * 8,
                    direction=direction,
                    tolerance_pct=10.0,
                    bootstrap=False,
                ).to_dict()
                value["source_table"] = source_table
                return value

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
                    "nn/layer/rank0/norm_output": metric(
                        "nn/layer/rank0/norm_output",
                        "two_sided",
                        "numerical_correctness",
                        0.5,
                    ),
                    "nn/layer/fp_gpu_time": metric(
                        "nn/layer/fp_gpu_time",
                        "high_bad",
                        "compute_performance",
                        10.0,
                    ),
                    "coll/layer/gpu_time": metric(
                        "coll/layer/gpu_time",
                        "high_bad",
                        "collective_performance",
                        20.0,
                    ),
                    "overlap/task/coll_mean": metric(
                        "overlap/task/coll_mean",
                        "two_sided",
                        "overlap_performance",
                        1.2,
                    ),
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

    def test_dl_store_omits_components_without_metrics(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            metric = stats.summarize_metric(
                "nn/layer/fp_gpu_time",
                [10.0] * 8,
                direction="high_bad",
                tolerance_pct=10.0,
                bootstrap=False,
            ).to_dict()
            metric["source_table"] = "compute_performance"
            record = {
                "schema_version": "cval.baseline.v2",
                "baseline_id": "dl-compute-only",
                "test_type": "dltest",
                "stratum_key": "",
                "window_days": 30,
                "created_at": NOW,
                "timestamp": NOW,
                "n_samples": 12,
                "method": "robust_mad",
                "metrics": {"nn/layer/fp_gpu_time": metric},
            }

            store_dynamic_baseline(record, config=config)
            activate_baseline("dl-compute-only", "dltest", config=config)

            self.assertEqual(
                {path.name for path in Path(tmpdir).glob("*.db")},
                {"dltest_compute_performance-baselines.db"},
            )
            self.assertEqual(
                get_active_baseline("dltest", config=config)["components"],
                ["compute_performance"],
            )

    def test_store_classification_results(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            normal_metric = {
                "metric": "x",
                "component": "",
                "value": 100.0,
                "median": 100.0,
                "status": "normal",
                "pct_diff": 0.0,
                "abs_pct_diff": 0.0,
                "counts_for_degraded_status": False,
                "direction": "low_bad",
                "lower_bound": 90.0,
                "upper_bound": None,
            }
            degraded_metric = {
                **normal_metric,
                "value": 50.0,
                "status": "degraded",
                "pct_diff": -50.0,
                "abs_pct_diff": 50.0,
            }
            verdicts = [
                {
                    "node": "node-a",
                    "test_type": "storage",
                    "baseline_test_type": "storage",
                    "dl_component": "",
                    "baseline_id": "storage-1",
                    "status": "normal",
                    "n_metrics": 1,
                    "n_compared": 1,
                    "n_degraded": 0,
                    "n_band_degraded": 0,
                    "n_improved": 0,
                    "degraded_metric_fraction": 0.0,
                    "degraded_metric_percent": 0.0,
                    "worst_pct_diff": 0.0,
                    "metrics": [normal_metric],
                },
                {
                    "node": "node-b",
                    "test_type": "storage",
                    "baseline_test_type": "storage",
                    "dl_component": "",
                    "baseline_id": "storage-1",
                    "status": "degraded",
                    "n_metrics": 1,
                    "n_compared": 1,
                    "n_degraded": 1,
                    "n_band_degraded": 1,
                    "n_improved": 0,
                    "degraded_metric_fraction": 1.0,
                    "degraded_metric_percent": 100.0,
                    "worst_pct_diff": 50.0,
                    "metrics": [degraded_metric],
                },
            ]

            count = store_classification_results(verdicts, classified_at=NOW, config=config)

            self.assertEqual(count, 2)
            with closing(
                sqlite3.connect(default_classification_db_path("storage", config))
            ) as connection:
                rows = connection.execute(
                    "SELECT node, status, passed FROM classification_results ORDER BY node"
                ).fetchall()
            self.assertEqual(rows, [("node-a", "normal", 1), ("node-b", "degraded", 0)])

    def test_store_classifications_uses_operational_target_database(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            metric = {
                "metric": "busbw", "component": "", "value": 100.0,
                "median": 100.0, "status": "normal", "pct_diff": 0.0,
                "abs_pct_diff": 0.0, "counts_for_degraded_status": False,
                "direction": "low_bad", "lower_bound": 95.0, "upper_bound": None,
            }
            verdict = {
                "node": "node-a", "baseline_test_type": "storage", "dl_component": "",
                "baseline_id": "storage-1", "status": "normal", "n_metrics": 1,
                "n_compared": 1, "n_degraded": 0, "n_band_degraded": 0,
                "n_improved": 0, "degraded_metric_fraction": 0.0,
                "degraded_metric_percent": 0.0, "worst_pct_diff": 0.0,
                "metrics": [metric],
            }
            store_classification_results(
                [verdict | {"test_type": "storage"}],
                classified_at=NOW,
                config=config,
            )

            self.assertTrue(default_classification_db_path("storage", config).exists())
            with self.assertRaisesRegex(ValueError, "only by cval.nccl_eval"):
                default_classification_db_path("nccl", config)
            self.assertFalse((Path(tmpdir) / "nccl-classifications.db").exists())

    def test_retained_global_and_multi_target_explicit_writes_are_rejected(self):
        with TemporaryDirectory() as tmpdir:
            config = self._config(tmpdir)
            metric = {
                "metric": "x", "component": "", "value": 100.0,
                "median": 100.0, "status": "normal", "pct_diff": 0.0,
                "abs_pct_diff": 0.0, "counts_for_degraded_status": False,
                "direction": "low_bad", "lower_bound": 90.0, "upper_bound": None,
            }
            base = {
                "node": "node-a", "baseline_id": "one", "status": "normal",
                "n_metrics": 1, "n_compared": 1, "n_degraded": 0,
                "n_band_degraded": 0, "n_improved": 0,
                "degraded_metric_fraction": 0.0, "degraded_metric_percent": 0.0,
                "worst_pct_diff": 0.0, "metrics": [metric], "dl_component": "",
            }
            storage_verdict = base | {
                "test_type": "storage", "baseline_test_type": "storage"
            }
            with self.assertRaisesRegex(ValueError, "global.*read-only"):
                store_classification_results(
                    [storage_verdict],
                    db_path=global_classification_db_path(config),
                    config=config,
                )
            self.assertFalse(global_classification_db_path(config).exists())

            nccl_metric = dict(metric, component="IB_HEALTH", metric="busbw")
            nccl_verdict = base | {
                "node": "node-b", "test_type": "nccl",
                "baseline_test_type": "nccl", "metrics": [nccl_metric],
            }
            explicit = Path(tmpdir) / "mixed.db"
            with self.assertRaisesRegex(ValueError, "only one target group"):
                store_classification_results(
                    [storage_verdict, nccl_verdict],
                    db_path=explicit,
                    config=config,
                )
            self.assertFalse(explicit.exists())


if __name__ == "__main__":
    unittest.main()
