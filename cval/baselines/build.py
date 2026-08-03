"""Build dynamic baselines from result DBs using robust statistics.

A baseline is computed per ``(test_type, stratum)`` over a rolling time window:

1. Pull each metric's recent values out of the result DB (SQLite has no
   median, so values are aggregated in Python).
2. Drop non-positive performance readings (failed/missing storage or DL runs).
3. ``cval.baselines.stats.summarize_metric`` trims extreme outliers, then
   records the median, MAD/IQR, percentiles, and a directional acceptance band.

Stratification keys differ by test type because the schemas differ:
    - storage rows carry ``image_name`` (no ``test_plan``);
    - DL tall tables carry ``test_plan`` (no ``image_name``).
GPU SKU / topology are not present in any table and are left for future work.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

from cval.baselines import stats
from cval.config import CvalConfig, load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS
from cval.storage.dltest_ingest import validate_dl_metric_generation
from cval.storage.sqlite_uri import connect_sqlite_file

BASELINE_SCHEMA_VERSION = "cval.baseline.v2"

# short_name, table, direction, tolerance config attribute, keep rank in key
_DL_DB_SPECS = (
    ("numerical_correctness", stats.DIRECTION_TWO_SIDED, "dl_numerical_tolerance_pct", True),
    ("compute_performance", stats.DIRECTION_HIGH_BAD, "dl_compute_tolerance_pct", False),
    ("collective_performance", stats.DIRECTION_HIGH_BAD, "dl_compute_tolerance_pct", False),
    ("overlap_performance", stats.DIRECTION_TWO_SIDED, "dl_overlap_tolerance_pct", False),
)


def _connect_ro(db_path: str | Path | None) -> sqlite3.Connection | None:
    """Open SQLite read-only, or return None when the DB does not exist."""

    if not db_path:
        return None
    path = Path(db_path)
    if not path.exists():
        return None
    return connect_sqlite_file(path, mode="ro", timeout=30)


def _window_cutoff(window_days: int) -> int:
    return int(time.time()) - int(window_days) * 86400


def _stratum_key(parts: dict[str, str | None]) -> str:
    return ",".join(f"{key}={value}" for key, value in parts.items() if value)


def _assemble_record(
    test_type: str,
    metrics: dict[str, Any],
    *,
    window_days: int,
    n_samples: int,
    stratum_key: str,
    baseline_id: str | None,
    method: str = "robust_mad",
) -> dict[str, Any]:
    created_at = int(time.time())
    if not baseline_id:
        baseline_id = f"{test_type}-{stratum_key or 'all'}-{created_at}"
    return {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "baseline_id": baseline_id,
        "test_type": test_type,
        "stratum_key": stratum_key,
        "window_days": int(window_days),
        "created_at": created_at,
        "timestamp": created_at,
        "n_samples": int(n_samples),
        "method": method,
        "metrics": metrics,
    }


def _query_rows(
    db_path: str | Path,
    table: str,
    value_columns: tuple[str, ...],
    timestamp_column: str,
    window_days: int,
    filters: dict[str, str | None],
) -> list[tuple[Any, ...]]:
    """Run a windowed SELECT. Column/table names are internal constants only."""

    connection = _connect_ro(db_path)
    if connection is None:
        raise FileNotFoundError(f"result DB not found: {db_path}")
    try:
        where = [f"{timestamp_column} > ?"]
        params: list[Any] = [_window_cutoff(window_days)]
        for column, value in filters.items():
            if value:
                where.append(f"{column} = ?")
                params.append(value)
        sql = (
            f"SELECT {', '.join(value_columns)} FROM {table} "
            f"WHERE {' AND '.join(where)}"
        )
        rows = connection.execute(sql, params).fetchall()
    finally:
        connection.close()
    return rows


def _collect_positive(
    columns: tuple[str, ...], rows: list[tuple[Any, ...]]
) -> dict[str, list[float]]:
    """Group rows into per-column value lists, dropping non-positive readings."""

    series: dict[str, list[float]] = {column: [] for column in columns}
    for row in rows:
        for column, value in zip(columns, row):
            if value is not None and float(value) > 0.0:
                series[column].append(float(value))
    return series


def build_storage_baseline(
    *,
    config: CvalConfig | None = None,
    db_path: str | Path | None = None,
    window_days: int | None = None,
    min_samples: int | None = None,
    image_name: str | None = None,
    node: str | None = None,
    baseline_id: str | None = None,
) -> dict[str, Any]:
    """Build a storage baseline from ``storage_performance`` (all metrics low-bad)."""

    config = config or load_config()
    db_path = db_path or config.storage.storage_db_path
    window_days = config.baseline.window_days if window_days is None else window_days
    min_samples = config.baseline.min_samples if min_samples is None else min_samples

    rows = _query_rows(
        db_path,
        "storage_performance",
        STORAGE_METRIC_COLUMNS,
        "timestamp",
        window_days,
        {"image_name": image_name, "node": node},
    )
    series = _collect_positive(STORAGE_METRIC_COLUMNS, rows)

    metrics: dict[str, Any] = {}
    for column in STORAGE_METRIC_COLUMNS:
        values = series[column]
        if len(values) >= min_samples:
            metric_stat = stats.summarize_metric(
                column,
                values,
                direction=stats.DIRECTION_LOW_BAD,
                tolerance_pct=config.baseline.storage_peer_tolerance_pct,
                z_threshold=config.baseline.robust_z_threshold,
            ).to_dict()
            metric_stat["source_table"] = "storage_performance"
            metrics[column] = metric_stat

    return _assemble_record(
        "storage",
        metrics,
        window_days=window_days,
        n_samples=len(rows),
        stratum_key=_stratum_key({"image": image_name, "node": node}),
        baseline_id=baseline_id,
    )


def _default_dl_db_paths(config: CvalConfig) -> dict[str, str]:
    return {
        "numerical_correctness": config.storage.dl_numerical_db_path,
        "compute_performance": config.storage.dl_compute_db_path,
        "collective_performance": config.storage.dl_collective_db_path,
        "overlap_performance": config.storage.dl_overlap_db_path,
    }


def build_dl_baseline(
    *,
    config: CvalConfig | None = None,
    db_paths: dict[str, str] | None = None,
    window_days: int | None = None,
    min_samples: int | None = None,
    test_plan: str | None = None,
    baseline_id: str | None = None,
) -> dict[str, Any]:
    """Build a DL baseline across the four tall metric DBs.

    Numerical-correctness metrics keep ``rank`` in the key (different ranks may
    legitimately differ and must stay near-exact per rank); performance metrics
    pool ranks since the 8 GPUs are peers for timing.
    """

    config = config or load_config()
    db_paths = db_paths or _default_dl_db_paths(config)
    initial_generation = validate_dl_metric_generation(db_paths)
    window_days = config.baseline.window_days if window_days is None else window_days
    min_samples = config.baseline.min_samples if min_samples is None else min_samples

    metrics: dict[str, Any] = {}
    run_keys: set[str] = set()

    for table, direction, tolerance_attr, keep_rank in _DL_DB_SPECS:
        connection = _connect_ro(db_paths.get(table))
        if connection is None:
            continue
        try:
            where = ["cval_timestamp > ?"]
            params: list[Any] = [_window_cutoff(window_days)]
            if test_plan:
                where.append("test_plan = ?")
                params.append(test_plan)
            sql = (
                "SELECT task_group, task_name, rank, metric_name, metric_value, run_key "
                f"FROM {table} WHERE {' AND '.join(where)}"
            )
            rows = connection.execute(sql, params).fetchall()
        finally:
            connection.close()

        series: dict[str, list[float]] = {}
        for task_group, task_name, rank, metric_name, metric_value, run_key in rows:
            if metric_value is None:
                continue
            run_keys.add(run_key)
            if keep_rank:
                key = f"{task_group}/{task_name}/rank{rank}/{metric_name}"
            else:
                key = f"{task_group}/{task_name}/{metric_name}"
            series.setdefault(key, []).append(float(metric_value))

        tolerance_pct = getattr(config.baseline, tolerance_attr)
        for key, values in series.items():
            if len(values) >= min_samples:
                metric_stat = stats.summarize_metric(
                    key,
                    values,
                    direction=direction,
                    tolerance_pct=tolerance_pct,
                    z_threshold=config.baseline.robust_z_threshold,
                ).to_dict()
                metric_stat["source_table"] = table
                metrics[key] = metric_stat

    final_generation = validate_dl_metric_generation(db_paths)
    if final_generation != initial_generation:
        raise RuntimeError("DL metric DB generation changed during baseline build")

    return _assemble_record(
        "dltest",
        metrics,
        window_days=window_days,
        n_samples=len(run_keys),
        stratum_key=_stratum_key({"test_plan": test_plan}),
        baseline_id=baseline_id,
    )


def build_baseline(
    test_type: str,
    *,
    config: CvalConfig | None = None,
    window_days: int | None = None,
    min_samples: int | None = None,
    db_path: str | Path | None = None,
    image_name: str | None = None,
    node: str | None = None,
    test_plan: str | None = None,
    dl_db_paths: dict[str, str] | None = None,
    baseline_id: str | None = None,
) -> dict[str, Any]:
    """Dispatch to the per-test-type builder and return a baseline record."""

    if test_type == "storage":
        return build_storage_baseline(
            config=config,
            db_path=db_path,
            window_days=window_days,
            min_samples=min_samples,
            image_name=image_name,
            node=node,
            baseline_id=baseline_id,
        )
    if test_type == "dltest":
        return build_dl_baseline(
            config=config,
            db_paths=dl_db_paths,
            window_days=window_days,
            min_samples=min_samples,
            test_plan=test_plan,
            baseline_id=baseline_id,
        )
    raise ValueError(f"unknown test_type: {test_type!r}")
