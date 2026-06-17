"""Store and retrieve baselines in SQLite databases."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from cval.baselines.models import BaselineMetrics
from cval.config import load_config
from cval.storage.ingest import _connect_writable


def store_baseline(
    baseline: BaselineMetrics,
    db_path: str | Path | None = None,
    test_type: str | None = None,
) -> None:
    """Store a baseline in the validation DB.

    Creates/updates baselines table with baseline_id, test_type, metrics_json.
    """
    db_path = db_path or load_config().storage.validation_db_path
    test_type = test_type or baseline.test_type

    metrics_dict = {
        "baseline_id": baseline.baseline_id,
        "test_plan": baseline.test_plan,
        "timestamp": baseline.timestamp,
        "node": baseline.node,
    }

    if test_type == "storage":
        for key in (
            "iodepth_read_1file_iops",
            "iodepth_read_1file_bw",
            "iodepth_write_1file_iops",
            "iodepth_write_1file_bw",
            "numjobs_read_nfiles_iops",
            "numjobs_read_nfiles_bw",
            "numjobs_write_nfiles_iops",
            "numjobs_write_nfiles_bw",
            "randread_iops",
            "randread_bw",
            "randwrite_iops",
            "randwrite_bw",
        ):
            metrics_dict[key] = getattr(baseline, key, 0.0)

    elif test_type == "nccl":
        metrics_dict["busbw"] = baseline.busbw
        metrics_dict["latency"] = baseline.latency

    elif test_type == "dltest":
        metrics_dict["task_counts"] = baseline.task_counts
        metrics_dict["status_counts"] = baseline.status_counts
        metrics_dict["numerical_metrics"] = baseline.numerical_metrics
        metrics_dict["collective_metrics"] = baseline.collective_metrics

    with closing(_connect_writable(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS baselines (
              baseline_id TEXT NOT NULL,
              test_type TEXT NOT NULL,
              metrics_json TEXT,
              timestamp INTEGER,
              PRIMARY KEY (baseline_id, test_type)
            )
            """
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO baselines (baseline_id, test_type, metrics_json, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (baseline.baseline_id, test_type, json.dumps(metrics_dict), baseline.timestamp),
        )
        connection.commit()


def load_baseline_from_db(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
) -> BaselineMetrics | None:
    """Load a baseline from the validation DB."""
    db_path = db_path or load_config().storage.validation_db_path

    try:
        with closing(sqlite3.connect(db_path)) as connection:
            row = connection.execute(
                "SELECT metrics_json FROM baselines WHERE baseline_id = ? AND test_type = ?",
                (baseline_id, test_type),
            ).fetchone()

            if not row:
                return None

            metrics_dict = json.loads(row[0])
            baseline = BaselineMetrics(
                test_type=test_type,
                baseline_id=baseline_id,
                test_plan=metrics_dict.get("test_plan", ""),
                timestamp=metrics_dict.get("timestamp", 0),
                node=metrics_dict.get("node", ""),
            )

            if test_type == "storage":
                for key in (
                    "iodepth_read_1file_iops",
                    "iodepth_read_1file_bw",
                    "iodepth_write_1file_iops",
                    "iodepth_write_1file_bw",
                    "numjobs_read_nfiles_iops",
                    "numjobs_read_nfiles_bw",
                    "numjobs_write_nfiles_iops",
                    "numjobs_write_nfiles_bw",
                    "randread_iops",
                    "randread_bw",
                    "randwrite_iops",
                    "randwrite_bw",
                ):
                    if key in metrics_dict:
                        setattr(baseline, key, float(metrics_dict[key]))

            elif test_type == "nccl":
                if "busbw" in metrics_dict:
                    baseline.busbw = float(metrics_dict["busbw"])
                if "latency" in metrics_dict:
                    baseline.latency = float(metrics_dict["latency"])

            elif test_type == "dltest":
                baseline.task_counts = metrics_dict.get("task_counts", {})
                baseline.status_counts = metrics_dict.get("status_counts", {})
                baseline.numerical_metrics = metrics_dict.get("numerical_metrics", {})
                baseline.collective_metrics = metrics_dict.get("collective_metrics", {})

            return baseline
    except Exception:
        return None


def list_baselines(
    test_type: str | None = None,
    db_path: str | Path | None = None,
) -> list[tuple[str, str, int]]:
    """List all baselines in the validation DB.

    Returns list of (baseline_id, test_type, timestamp) tuples.
    """
    db_path = db_path or load_config().storage.validation_db_path

    try:
        with closing(sqlite3.connect(db_path)) as connection:
            if test_type:
                rows = connection.execute(
                    "SELECT baseline_id, test_type, timestamp FROM baselines WHERE test_type = ? ORDER BY test_type, baseline_id",
                    (test_type,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT baseline_id, test_type, timestamp FROM baselines ORDER BY test_type, baseline_id"
                ).fetchall()
            return rows
    except Exception:
        return []
