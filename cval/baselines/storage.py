"""Store and retrieve baselines in SQLite databases."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from cval.baselines.models import BaselineMetrics
from cval.config import load_config
from cval.storage.ingest import _connect_writable, _ensure_column


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


# --- Dynamic (versioned, statistically built) baselines ---------------------
#
# Dynamic baselines reuse the ``baselines`` table but add provenance columns so
# every record is immutable and auditable. ``status`` moves candidate -> active
# -> superseded; new results are classified against the single ``active`` row
# for their ``(test_type, stratum_key)``.

_DYNAMIC_COLUMNS = (
    ("status", "TEXT NOT NULL DEFAULT 'candidate'"),
    ("stratum_key", "TEXT NOT NULL DEFAULT ''"),
    ("n_samples", "INTEGER NOT NULL DEFAULT 0"),
    ("window_days", "INTEGER NOT NULL DEFAULT 0"),
    ("method", "TEXT NOT NULL DEFAULT ''"),
    ("schema_version", "TEXT NOT NULL DEFAULT ''"),
    ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ("supersedes", "TEXT NOT NULL DEFAULT ''"),
)


def _ensure_baselines_schema(connection: sqlite3.Connection) -> None:
    """Create the baselines table and add dynamic columns to older DBs."""

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
    for column_name, column_definition in _DYNAMIC_COLUMNS:
        _ensure_column(connection, "baselines", column_name, column_definition)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_baselines_type_status "
        "ON baselines(test_type, status, stratum_key)"
    )


def store_dynamic_baseline(
    record: dict,
    db_path: str | Path | None = None,
    status: str = "candidate",
) -> str:
    """Persist a built baseline record and return its baseline_id.

    The full record (including per-metric robust stats) is stored as JSON; key
    provenance fields are also written to first-class columns for querying.
    New baselines default to ``candidate`` so a degraded fleet cannot silently
    re-baseline itself; call :func:`activate_baseline` to promote.
    """

    db_path = db_path or load_config().storage.validation_db_path
    baseline_id = record["baseline_id"]
    test_type = record["test_type"]

    with closing(_connect_writable(db_path)) as connection:
        _ensure_baselines_schema(connection)
        connection.execute(
            """
            INSERT OR REPLACE INTO baselines (
              baseline_id, test_type, metrics_json, timestamp,
              status, stratum_key, n_samples, window_days, method,
              schema_version, created_at, supersedes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                baseline_id,
                test_type,
                json.dumps(record),
                int(record.get("timestamp", record.get("created_at", 0))),
                status,
                record.get("stratum_key", ""),
                int(record.get("n_samples", 0)),
                int(record.get("window_days", 0)),
                record.get("method", ""),
                record.get("schema_version", ""),
                int(record.get("created_at", 0)),
                record.get("supersedes", ""),
            ),
        )
        connection.commit()
    return baseline_id


def activate_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
) -> bool:
    """Promote one baseline to ``active`` and supersede the previous active one.

    Only the prior active baseline for the *same* ``(test_type, stratum_key)``
    is superseded, so different strata keep independent active baselines.
    Returns False when the baseline_id does not exist.
    """

    db_path = db_path or load_config().storage.validation_db_path
    with closing(_connect_writable(db_path)) as connection:
        _ensure_baselines_schema(connection)
        row = connection.execute(
            "SELECT stratum_key FROM baselines WHERE baseline_id = ? AND test_type = ?",
            (baseline_id, test_type),
        ).fetchone()
        if row is None:
            return False
        stratum_key = row[0]
        connection.execute(
            """
            UPDATE baselines SET status = 'superseded'
            WHERE test_type = ? AND stratum_key = ? AND status = 'active'
              AND baseline_id != ?
            """,
            (test_type, stratum_key, baseline_id),
        )
        connection.execute(
            "UPDATE baselines SET status = 'active' WHERE baseline_id = ? AND test_type = ?",
            (baseline_id, test_type),
        )
        connection.commit()
    return True


def load_dynamic_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
) -> dict | None:
    """Load a full baseline record (with per-metric stats) by id."""

    db_path = db_path or load_config().storage.validation_db_path
    try:
        with closing(sqlite3.connect(db_path)) as connection:
            row = connection.execute(
                "SELECT metrics_json FROM baselines WHERE baseline_id = ? AND test_type = ?",
                (baseline_id, test_type),
            ).fetchone()
            if not row or not row[0]:
                return None
            return json.loads(row[0])
    except Exception:
        return None


def get_active_baseline(
    test_type: str,
    stratum_key: str | None = None,
    db_path: str | Path | None = None,
) -> dict | None:
    """Return the active baseline record for a test type / stratum, if any."""

    db_path = db_path or load_config().storage.validation_db_path
    try:
        with closing(sqlite3.connect(db_path)) as connection:
            query = (
                "SELECT metrics_json FROM baselines "
                "WHERE test_type = ? AND status = 'active'"
            )
            params: list = [test_type]
            if stratum_key is not None:
                query += " AND stratum_key = ?"
                params.append(stratum_key)
            query += " ORDER BY created_at DESC LIMIT 1"
            row = connection.execute(query, params).fetchone()
            if not row or not row[0]:
                return None
            return json.loads(row[0])
    except Exception:
        return None


def list_dynamic_baselines(
    test_type: str | None = None,
    db_path: str | Path | None = None,
) -> list[tuple]:
    """List dynamic baselines as (id, test_type, status, stratum, n, created_at)."""

    db_path = db_path or load_config().storage.validation_db_path
    try:
        with closing(sqlite3.connect(db_path)) as connection:
            base_query = (
                "SELECT baseline_id, test_type, status, stratum_key, n_samples, created_at "
                "FROM baselines"
            )
            if test_type:
                rows = connection.execute(
                    base_query + " WHERE test_type = ? ORDER BY created_at DESC",
                    (test_type,),
                ).fetchall()
            else:
                rows = connection.execute(
                    base_query + " ORDER BY test_type, created_at DESC"
                ).fetchall()
            return rows
    except Exception:
        return []
