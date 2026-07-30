"""Store and retrieve baselines in SQLite databases."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from cval.config import load_config
from cval.models import dl_component_for_test_type, normalize_baseline_test_type
from cval.storage.ingest import _connect_writable, _ensure_column


BASELINE_DB_FILENAMES = {
    "storage": "test-storage-baselines.db",
    "nccl": "test-nccl-baselines.db",
}

DL_BASELINE_DB_FILENAMES = {
    "numerical_correctness": "dltest_numerical_correctness-baselines.db",
    "compute_performance": "dltest_compute_performance-baselines.db",
    "collective_performance": "dltest_collective_performance-baselines.db",
    "overlap_performance": "dltest_overlap_performance-baselines.db",
}

CLASSIFICATION_DB_FILENAME = "classification-results.db"


def baseline_root_path(config=None) -> Path:
    """Return the operator-owned baseline root directory."""

    active_config = config or load_config()
    return Path(active_config.baseline.baseline_root_path).expanduser()


def default_dynamic_baseline_db_path(
    test_type: str,
    source_table: str | None = None,
    config=None,
) -> Path:
    """Return the default baseline DB for a test/component."""

    root = baseline_root_path(config)
    baseline_test_type = normalize_baseline_test_type(test_type)
    component = source_table or dl_component_for_test_type(test_type)
    if baseline_test_type == "dltest":
        if not component:
            return root / "dltest-baselines.db"
        filename = DL_BASELINE_DB_FILENAMES.get(component)
        if filename is None:
            raise ValueError(f"unknown DL baseline component: {component!r}")
        return root / filename
    filename = BASELINE_DB_FILENAMES.get(baseline_test_type)
    if filename is None:
        raise ValueError(f"unknown test_type: {test_type!r}")
    return root / filename


def default_dynamic_baseline_db_paths(test_type: str, config=None) -> list[Path]:
    """Return all default baseline DB paths for a test type."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    component = dl_component_for_test_type(test_type)
    if baseline_test_type == "dltest":
        if component:
            return [default_dynamic_baseline_db_path(test_type, config=config)]
        return [
            default_dynamic_baseline_db_path(baseline_test_type, table, config=config)
            for table in DL_BASELINE_DB_FILENAMES
        ]
    return [default_dynamic_baseline_db_path(baseline_test_type, config=config)]


def default_classification_db_path(config=None) -> Path:
    """Return the default classification-result DB path."""

    return baseline_root_path(config) / CLASSIFICATION_DB_FILENAME


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


def _store_dynamic_baseline_in_db(record: dict, db_path: str | Path, status: str) -> str:
    """Store one dynamic baseline record in one SQLite DB."""

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


def _split_dl_record_for_config(record: dict, config=None) -> list[tuple[Path, dict]]:
    """Split a DL record using paths from the supplied config."""

    targets: list[tuple[Path, dict]] = []
    metrics = record.get("metrics", {})
    for source_table in DL_BASELINE_DB_FILENAMES:
        component_metrics = {
            metric_name: metric_stat
            for metric_name, metric_stat in metrics.items()
            if metric_stat.get("source_table") == source_table
        }
        component_record = dict(record)
        component_record["metrics"] = component_metrics
        component_record["component"] = source_table
        targets.append(
            (default_dynamic_baseline_db_path("dltest", source_table, config=config), component_record)
        )
    return targets


def _merge_records(records: list[dict]) -> dict | None:
    """Merge component baseline records into one logical record."""

    if not records:
        return None
    merged = dict(records[0])
    merged_metrics = {}
    baseline_ids = []
    components = []
    for record in records:
        merged_metrics.update(record.get("metrics", {}))
        baseline_id = record.get("baseline_id")
        if baseline_id and baseline_id not in baseline_ids:
            baseline_ids.append(baseline_id)
        component = record.get("component")
        if component and component not in components:
            components.append(component)
    merged["metrics"] = merged_metrics
    merged["components"] = components
    if len(baseline_ids) > 1:
        merged["baseline_id"] = ",".join(baseline_ids)
    return merged


def store_dynamic_baseline(
    record: dict,
    db_path: str | Path | None = None,
    status: str = "candidate",
    config=None,
) -> str:
    """Persist a built baseline record and return its baseline_id.

    The full record (including per-metric robust stats) is stored as JSON; key
    provenance fields are also written to first-class columns for querying.
    New baselines default to ``candidate`` so a degraded fleet cannot silently
    re-baseline itself; call :func:`activate_baseline` to promote.
    """

    if db_path is not None:
        return _store_dynamic_baseline_in_db(record, db_path, status)

    if record["test_type"] == "dltest":
        for target_path, component_record in _split_dl_record_for_config(record, config=config):
            _store_dynamic_baseline_in_db(component_record, target_path, status)
        return record["baseline_id"]

    target_path = default_dynamic_baseline_db_path(record["test_type"], config=config)
    return _store_dynamic_baseline_in_db(record, target_path, status)


def _activate_baseline_in_db(
    baseline_id: str,
    test_type: str,
    db_path: str | Path,
) -> bool:
    """Promote one baseline in one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    with closing(_connect_writable(db_path)) as connection:
        _ensure_baselines_schema(connection)
        row = connection.execute(
            "SELECT stratum_key FROM baselines WHERE baseline_id = ? AND test_type = ?",
            (baseline_id, baseline_test_type),
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
                        (baseline_test_type, stratum_key, baseline_id),
        )
        connection.execute(
            "UPDATE baselines SET status = 'active' WHERE baseline_id = ? AND test_type = ?",
                        (baseline_id, baseline_test_type),
        )
        connection.commit()
    return True


def activate_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
    config=None,
) -> bool:
    """Promote one baseline to ``active`` and supersede the previous active one.

    Only the prior active baseline for the *same* ``(test_type, stratum_key)``
    is superseded, so different strata keep independent active baselines.
    Returns False when the baseline_id does not exist.
    """

    if db_path is not None:
        return _activate_baseline_in_db(baseline_id, test_type, db_path)

    activated_any = False
    for path in default_dynamic_baseline_db_paths(test_type, config=config):
        activated_any = _activate_baseline_in_db(baseline_id, test_type, path) or activated_any
    return activated_any


def _load_dynamic_baseline_from_db(
    baseline_id: str,
    test_type: str,
    db_path: str | Path,
) -> dict | None:
    """Load a full baseline record from one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    try:
        with closing(sqlite3.connect(db_path)) as connection:
            row = connection.execute(
                "SELECT metrics_json FROM baselines WHERE baseline_id = ? AND test_type = ?",
                (baseline_id, baseline_test_type),
            ).fetchone()
            if not row or not row[0]:
                return None
            return json.loads(row[0])
    except Exception:
        return None


def load_dynamic_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
    config=None,
) -> dict | None:
    """Load a full baseline record (with per-metric stats) by id."""

    if db_path is not None:
        return _load_dynamic_baseline_from_db(baseline_id, test_type, db_path)

    records = [
        record
        for path in default_dynamic_baseline_db_paths(test_type, config=config)
        if (record := _load_dynamic_baseline_from_db(baseline_id, test_type, path))
    ]
    return _merge_records(records)


def _get_active_baseline_from_db(
    test_type: str,
    stratum_key: str | None,
    db_path: str | Path,
) -> dict | None:
    """Return the active baseline record from one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    try:
        with closing(sqlite3.connect(db_path)) as connection:
            query = (
                "SELECT metrics_json FROM baselines "
                "WHERE test_type = ? AND status = 'active'"
            )
            params: list = [baseline_test_type]
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


def get_active_baseline(
    test_type: str,
    stratum_key: str | None = None,
    db_path: str | Path | None = None,
    config=None,
) -> dict | None:
    """Return the active baseline record for a test type / stratum, if any."""

    if db_path is not None:
        return _get_active_baseline_from_db(test_type, stratum_key, db_path)

    records = [
        record
        for path in default_dynamic_baseline_db_paths(test_type, config=config)
        if (record := _get_active_baseline_from_db(test_type, stratum_key, path))
    ]
    return _merge_records(records)


def _list_dynamic_baselines_from_db(
    test_type: str | None,
    db_path: str | Path,
) -> list[tuple]:
    """List dynamic baselines from one DB."""

    try:
        with closing(sqlite3.connect(db_path)) as connection:
            base_query = (
                "SELECT baseline_id, test_type, status, stratum_key, n_samples, created_at "
                "FROM baselines"
            )
            if test_type:
                return connection.execute(
                    base_query + " WHERE test_type = ? ORDER BY created_at DESC",
                    (normalize_baseline_test_type(test_type),),
                ).fetchall()
            return connection.execute(
                base_query + " ORDER BY test_type, created_at DESC"
            ).fetchall()
    except Exception:
        return []


def list_dynamic_baselines(
    test_type: str | None = None,
    db_path: str | Path | None = None,
    config=None,
) -> list[tuple]:
    """List dynamic baselines as (id, test_type, status, stratum, n, created_at)."""

    if db_path is not None:
        return _list_dynamic_baselines_from_db(test_type, db_path)

    paths = default_dynamic_baseline_db_paths(test_type, config=config) if test_type else [
        *default_dynamic_baseline_db_paths("storage", config=config),
        *default_dynamic_baseline_db_paths("nccl", config=config),
        *default_dynamic_baseline_db_paths("dltest", config=config),
    ]
    rows: list[tuple] = []
    for path in paths:
        rows.extend(_list_dynamic_baselines_from_db(test_type, path))
    return rows


def store_classification_results(
    verdicts: list[dict],
    db_path: str | Path | None = None,
    classified_at: int | None = None,
    config=None,
) -> int:
    """Persist node classification verdicts and return rows written.

    ``passed`` is true for normal/improved and false for degraded. Raw
    validation pass/fail rows stay untouched in validation.db.
    """

    db_path = db_path or default_classification_db_path(config)
    classified_at = int(time.time()) if classified_at is None else int(classified_at)
    with closing(_connect_writable(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS classification_results (
              classified_at INTEGER NOT NULL,
              node TEXT NOT NULL,
              test_type TEXT NOT NULL,
              baseline_id TEXT NOT NULL,
              status TEXT NOT NULL,
              passed INTEGER NOT NULL,
              n_compared INTEGER NOT NULL,
              n_degraded INTEGER NOT NULL,
              n_improved INTEGER NOT NULL,
              n_band_degraded INTEGER NOT NULL DEFAULT 0,
              degraded_metric_fraction REAL NOT NULL DEFAULT 0.0,
              worst_pct_diff REAL NOT NULL DEFAULT 0.0,
              metrics_json TEXT NOT NULL,
              PRIMARY KEY (classified_at, node, test_type, baseline_id)
            )
            """
        )
        _ensure_column(
            connection,
            "classification_results",
            "n_band_degraded",
            "INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            connection,
            "classification_results",
            "degraded_metric_fraction",
            "REAL NOT NULL DEFAULT 0.0",
        )
        _ensure_column(
            connection,
            "classification_results",
            "worst_pct_diff",
            "REAL NOT NULL DEFAULT 0.0",
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_classification_node_test_time "
            "ON classification_results(node, test_type, classified_at)"
        )
        connection.executemany(
            """
            INSERT OR REPLACE INTO classification_results (
              classified_at, node, test_type, baseline_id, status, passed,
                            n_compared, n_degraded, n_improved, n_band_degraded,
                            degraded_metric_fraction, worst_pct_diff, metrics_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    classified_at,
                    verdict["node"],
                    verdict["test_type"],
                    verdict.get("baseline_id") or "",
                    verdict["status"],
                    0 if verdict["status"] == "degraded" else 1,
                    int(verdict.get("n_compared", 0)),
                    int(verdict.get("n_degraded", 0)),
                    int(verdict.get("n_improved", 0)),
                    int(verdict.get("n_band_degraded", verdict.get("n_degraded", 0))),
                    float(verdict.get("degraded_metric_fraction", 0.0)),
                    float(verdict.get("worst_pct_diff", 0.0)),
                    json.dumps(verdict.get("metrics", [])),
                )
                for verdict in verdicts
            ],
        )
        connection.commit()
    return len(verdicts)
