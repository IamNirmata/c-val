"""Tests for node classification against baselines."""

import sqlite3
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.baselines import build
from cval.baselines.classify import classify_node, classify_nodes
from cval.baselines.storage import activate_baseline, get_active_baseline, store_dynamic_baseline
from cval.config import load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS

NOW = int(time.time())


def _make_storage_two_nodes(path: Path) -> None:
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

        def insert(node: str, factor: float, n_rows: int) -> None:
            for i in range(n_rows):
                jitter = 1.0 + (i % 5 - 2) * 0.01
                values = [
                    1000.0 * (c + 1) * factor * jitter
                    for c in range(len(STORAGE_METRIC_COLUMNS))
                ]
                connection.execute(
                    f"""
                    INSERT INTO storage_performance
                    (node, timestamp, image_name, {insert_columns})
                    VALUES (?, ?, ?, {placeholders})
                    """,
                    (node, NOW - i * 60, "img:1", *values),
                )

        insert("node-good", 1.0, 12)  # healthy fleet behavior
        insert("node-bad", 0.5, 6)    # 50% slower storage -> degraded
        connection.commit()


def _make_nccl_two_nodes(path: Path) -> None:
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

        def insert(node: str, busbw: float, latency: float, n_rows: int) -> None:
            for i in range(n_rows):
                connection.execute(
                    "INSERT INTO nccl_performance (node, timestamp, image_name, busbw, latency) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (node, NOW - i * 60, "img:1", busbw + (i % 5 - 2) * 0.5, latency),
                )

        insert("node-good", 500.0, 25.0, 12)
        insert("node-bad", 400.0, 35.0, 6)  # lower busbw + higher latency
        connection.commit()


class TestClassifyStorage(unittest.TestCase):
    def test_bad_node_degraded_good_node_normal(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage.db"
            _make_storage_two_nodes(db_path)

            baseline = build.build_storage_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5, node="node-good"
            )

            bad = classify_node(
                "storage", "node-bad", baseline, config=config, db_path=db_path, window_days=365
            )
            good = classify_node(
                "storage", "node-good", baseline, config=config, db_path=db_path, window_days=365
            )

            self.assertEqual(bad["status"], "degraded")
            self.assertGreater(bad["n_degraded"], 0)
            self.assertEqual(good["status"], "normal")

    def test_classify_nodes_discovers_all(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage.db"
            _make_storage_two_nodes(db_path)
            baseline = build.build_storage_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5, node="node-good"
            )

            verdicts = classify_nodes(
                "storage", baseline, config=config, db_path=db_path, window_days=365
            )
            by_node = {v["node"]: v["status"] for v in verdicts}
            self.assertEqual(set(by_node), {"node-good", "node-bad"})
            self.assertEqual(by_node["node-bad"], "degraded")


class TestClassifyNccl(unittest.TestCase):
    def test_low_busbw_and_high_latency_degraded(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            _make_nccl_two_nodes(db_path)

            baseline = build.build_nccl_baseline(
                config=config, db_path=db_path, window_days=365, min_samples=5, node="node-good"
            )

            bad = classify_node(
                "nccl", "node-bad", baseline, config=config, db_path=db_path, window_days=365
            )
            self.assertEqual(bad["status"], "degraded")
            degraded_metrics = {m["metric"] for m in bad["metrics"] if m["status"] == "degraded"}
            self.assertIn("busbw", degraded_metrics)
            self.assertIn("latency", degraded_metrics)


class TestClassifyViaActiveBaseline(unittest.TestCase):
    def test_classify_uses_stored_active_baseline(self):
        config = load_config()
        with TemporaryDirectory() as tmpdir:
            storage_db = Path(tmpdir) / "storage.db"
            validation_db = Path(tmpdir) / "validation.db"
            _make_storage_two_nodes(storage_db)

            baseline = build.build_storage_baseline(
                config=config,
                db_path=storage_db,
                window_days=365,
                min_samples=5,
                node="node-good",
                baseline_id="storage-good-1",
            )
            store_dynamic_baseline(baseline, db_path=validation_db)
            activate_baseline("storage-good-1", "storage", db_path=validation_db)

            active = get_active_baseline("storage", db_path=validation_db)
            self.assertIsNotNone(active)

            bad = classify_node(
                "storage", "node-bad", active, config=config, db_path=storage_db, window_days=365
            )
            self.assertEqual(bad["status"], "degraded")
            self.assertEqual(bad["baseline_id"], "storage-good-1")


if __name__ == "__main__":
    unittest.main()
