"""Classify nodes against a baseline using their recent validation results.

This is the consumer side of the baseline system: given an active (or named)
baseline record, summarize each node's recent metric values and compare them to
the baseline's directional acceptance bands. A node is:

  - ``degraded`` if any metric falls on the failing side of its band;
  - ``improved`` if some metric is better than the p95/p05 good-side tail and
    none are degraded;
  - ``normal`` otherwise.

Node values use the *median* of the node's recent runs so a single noisy run
does not flip the verdict.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cval.baselines import stats
from cval.baselines.build import (
    _DL_DB_SPECS,
    _collect_positive,
    _connect_ro,
    _default_dl_db_paths,
    _query_rows,
    _window_cutoff,
)
from cval.config import CvalConfig, load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS

NCCL_COLUMNS = ("busbw", "latency")


def _median_by_column(
    columns: tuple[str, ...], rows: list[tuple[Any, ...]]
) -> dict[str, float]:
    series = _collect_positive(columns, rows)
    return {column: stats.median(values) for column, values in series.items() if values}


def _node_values_storage(db_path: str | Path, node: str, window_days: int) -> dict[str, float]:
    rows = _query_rows(
        db_path,
        "storage_performance",
        STORAGE_METRIC_COLUMNS,
        "timestamp",
        window_days,
        {"node": node},
    )
    return _median_by_column(STORAGE_METRIC_COLUMNS, rows)


def _node_values_nccl(db_path: str | Path, node: str, window_days: int) -> dict[str, float]:
    rows = _query_rows(
        db_path,
        "nccl_performance",
        NCCL_COLUMNS,
        "timestamp",
        window_days,
        {"node": node},
    )
    return _median_by_column(NCCL_COLUMNS, rows)


def _node_values_dl(
    db_paths: dict[str, str], node: str, window_days: int
) -> dict[str, float]:
    values: dict[str, float] = {}
    cutoff = _window_cutoff(window_days)
    for table, _direction, _tolerance_attr, keep_rank in _DL_DB_SPECS:
        connection = _connect_ro(db_paths.get(table))
        if connection is None:
            continue
        try:
            rows = connection.execute(
                "SELECT task_group, task_name, rank, metric_name, metric_value "
                f"FROM {table} WHERE cval_timestamp > ? AND node = ?",
                (cutoff, node),
            ).fetchall()
        finally:
            connection.close()

        series: dict[str, list[float]] = {}
        for task_group, task_name, rank, metric_name, metric_value in rows:
            if metric_value is None:
                continue
            if keep_rank:
                key = f"{task_group}/{task_name}/rank{rank}/{metric_name}"
            else:
                key = f"{task_group}/{task_name}/{metric_name}"
            series.setdefault(key, []).append(float(metric_value))
        for key, vals in series.items():
            if vals:
                values[key] = stats.median(vals)
    return values


def _node_metric_values(
    test_type: str,
    node: str,
    *,
    config: CvalConfig,
    db_path: str | Path | None,
    dl_db_paths: dict[str, str] | None,
    window_days: int,
) -> dict[str, float]:
    if test_type == "storage":
        return _node_values_storage(db_path or config.storage.storage_db_path, node, window_days)
    if test_type == "nccl":
        return _node_values_nccl(db_path or config.storage.nccl_db_path, node, window_days)
    if test_type == "dltest":
        return _node_values_dl(dl_db_paths or _default_dl_db_paths(config), node, window_days)
    raise ValueError(f"unknown test_type: {test_type!r}")


def classify_node(
    test_type: str,
    node: str,
    baseline: dict[str, Any],
    *,
    config: CvalConfig | None = None,
    db_path: str | Path | None = None,
    dl_db_paths: dict[str, str] | None = None,
    window_days: int | None = None,
) -> dict[str, Any]:
    """Classify one node's recent results against a baseline record."""

    config = config or load_config()
    window_days = config.baseline.window_days if window_days is None else window_days
    baseline_metrics = baseline.get("metrics", {})

    node_values = _node_metric_values(
        test_type,
        node,
        config=config,
        db_path=db_path,
        dl_db_paths=dl_db_paths,
        window_days=window_days,
    )

    metric_reports: list[dict[str, Any]] = []
    n_degraded = 0
    n_improved = 0
    for key, stat in baseline_metrics.items():
        if key not in node_values:
            continue
        value = node_values[key]
        status, pct_diff = stats.classify_value(value, stat)
        if status == "degraded":
            n_degraded += 1
        elif status == "improved":
            n_improved += 1
        metric_reports.append(
            {
                "metric": key,
                "value": value,
                "median": stat.get("median"),
                "status": status,
                "pct_diff": pct_diff,
                "direction": stat.get("direction"),
                "lower_bound": stat.get("lower_bound"),
                "upper_bound": stat.get("upper_bound"),
            }
        )

    if n_degraded:
        overall = "degraded"
    elif n_improved:
        overall = "improved"
    else:
        overall = "normal"

    return {
        "node": node,
        "test_type": test_type,
        "baseline_id": baseline.get("baseline_id"),
        "status": overall,
        "n_metrics": len(metric_reports),
        "n_compared": len(metric_reports),
        "n_degraded": n_degraded,
        "n_improved": n_improved,
        "metrics": metric_reports,
    }


def _distinct_nodes(
    test_type: str,
    config: CvalConfig,
    db_path: str | Path | None,
    dl_db_paths: dict[str, str] | None,
    window_days: int,
) -> set[str]:
    cutoff = _window_cutoff(window_days)
    nodes: set[str] = set()

    def _collect(path: str | Path | None, table: str, ts_column: str) -> None:
        connection = _connect_ro(path)
        if connection is None:
            return
        try:
            rows = connection.execute(
                f"SELECT DISTINCT node FROM {table} WHERE {ts_column} > ?", (cutoff,)
            ).fetchall()
        finally:
            connection.close()
        nodes.update(row[0] for row in rows)

    if test_type == "storage":
        _collect(db_path or config.storage.storage_db_path, "storage_performance", "timestamp")
    elif test_type == "nccl":
        _collect(db_path or config.storage.nccl_db_path, "nccl_performance", "timestamp")
    elif test_type == "dltest":
        paths = dl_db_paths or _default_dl_db_paths(config)
        for table, _direction, _tolerance_attr, _keep_rank in _DL_DB_SPECS:
            _collect(paths.get(table), table, "cval_timestamp")
    else:
        raise ValueError(f"unknown test_type: {test_type!r}")

    return {node for node in nodes if node}


def classify_nodes(
    test_type: str,
    baseline: dict[str, Any],
    *,
    config: CvalConfig | None = None,
    db_path: str | Path | None = None,
    dl_db_paths: dict[str, str] | None = None,
    window_days: int | None = None,
) -> list[dict[str, Any]]:
    """Classify every node seen in the window against a baseline record."""

    config = config or load_config()
    window_days = config.baseline.window_days if window_days is None else window_days
    nodes = _distinct_nodes(test_type, config, db_path, dl_db_paths, window_days)
    return [
        classify_node(
            test_type,
            node,
            baseline,
            config=config,
            db_path=db_path,
            dl_db_paths=dl_db_paths,
            window_days=window_days,
        )
        for node in sorted(nodes)
    ]
