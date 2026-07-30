"""Classify nodes against a baseline using their recent validation results.

This is the consumer side of the baseline system: given an active (or named)
baseline record, summarize each node's recent metric values and compare them to
the baseline's directional acceptance bands. A node is:

- ``degraded`` if enough meaningful metrics fall on the failing side of their bands;
- ``improved`` if some metric is better than the p95/p05 good-side tail and
    none are degraded;
- ``normal`` otherwise.

Node values use the *median* of the node's recent runs so a single noisy run
does not flip the verdict. DL tests add a second aggregation layer so one noisy
metric out of thousands cannot mark a node degraded.
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
from cval.models import (
    DL_COMPONENT_TEST_TYPES,
    dl_component_for_test_type,
    normalize_baseline_test_type,
)
from cval.storage.ingest import STORAGE_METRIC_COLUMNS
from cval.storage.dltest_ingest import validate_dl_metric_generation

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
        "IB_HEALTH",
        ("BUS_BW", "LATENCY"),
        "timestamp",
        window_days,
        {"Node": node},
    )
    source = _median_by_column(("BUS_BW", "LATENCY"), rows)
    return {
        metric: source[source_column]
        for metric, source_column in (("busbw", "BUS_BW"), ("latency", "LATENCY"))
        if source_column in source
    }


def _node_values_dl(
    db_paths: dict[str, str], node: str, window_days: int, component: str | None = None
) -> dict[str, float]:
    initial_generation = validate_dl_metric_generation(db_paths)
    values: dict[str, float] = {}
    cutoff = _window_cutoff(window_days)
    for table, _direction, _tolerance_attr, keep_rank in _DL_DB_SPECS:
        if component and table != component:
            continue
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
    final_generation = validate_dl_metric_generation(db_paths)
    if final_generation != initial_generation:
        raise RuntimeError("DL metric DB generation changed during classification")
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
    baseline_test_type = normalize_baseline_test_type(test_type)
    if baseline_test_type == "storage":
        return _node_values_storage(db_path or config.storage.storage_db_path, node, window_days)
    if baseline_test_type == "nccl":
        return _node_values_nccl(db_path or config.storage.nccl_db_path, node, window_days)
    if baseline_test_type == "dltest":
        return _node_values_dl(
            dl_db_paths or _default_dl_db_paths(config),
            node,
            window_days,
            component=dl_component_for_test_type(test_type),
        )
    raise ValueError(f"unknown test_type: {test_type!r}")


def _metric_component(stat: dict[str, Any]) -> str:
    return str(stat.get("source_table", ""))


def _empty_component_summary(
    component: str,
    *,
    severity_pct: float,
    min_metrics: int,
    fraction: float,
) -> dict[str, Any]:
    return {
        "component": component,
        "status": "normal",
        "n_compared": 0,
        "n_degraded": 0,
        "n_band_degraded": 0,
        "n_improved": 0,
        "degraded_metric_fraction": 0.0,
        "degraded_metric_percent": 0.0,
        "worst_pct_diff": 0.0,
        "dl_degraded_severity_pct": severity_pct,
        "dl_min_degraded_metrics": min_metrics,
        "dl_degraded_metric_fraction_threshold": fraction,
    }


def _summarize_reports(
    reports: list[dict[str, Any]],
    *,
    is_dl: bool,
    config: CvalConfig,
) -> dict[str, Any]:
    n_compared = len(reports)
    n_improved = sum(1 for report in reports if report["status"] == "improved")
    n_band_degraded = sum(1 for report in reports if report["status"] == "degraded")
    worst_pct_diff = max(
        (
            abs(float(report.get("pct_diff") or 0.0))
            for report in reports
            if report["status"] == "degraded"
        ),
        default=0.0,
    )

    if not is_dl:
        n_degraded = n_band_degraded
        degraded_fraction = n_degraded / n_compared if n_compared else 0.0
        if n_degraded:
            overall = "degraded"
        elif n_improved:
            overall = "improved"
        else:
            overall = "normal"
        return {
            "status": overall,
            "n_compared": n_compared,
            "n_degraded": n_degraded,
            "n_band_degraded": n_band_degraded,
            "n_improved": n_improved,
            "degraded_metric_fraction": degraded_fraction,
            "degraded_metric_percent": degraded_fraction * 100.0,
            "worst_pct_diff": worst_pct_diff,
        }

    severity_pct = config.baseline.dl_degraded_severity_pct
    min_metrics = config.baseline.dl_min_degraded_metrics
    fraction_threshold = config.baseline.dl_degraded_metric_fraction
    n_degraded = sum(
        1
        for report in reports
        if report["status"] == "degraded"
        and abs(float(report.get("pct_diff") or 0.0)) >= severity_pct
    )
    degraded_fraction = n_degraded / n_compared if n_compared else 0.0
    enough_metrics = n_degraded >= min_metrics
    enough_fraction = degraded_fraction >= fraction_threshold
    if n_degraded and (enough_metrics or enough_fraction):
        overall = "degraded"
    elif n_improved and not n_degraded:
        overall = "improved"
    else:
        overall = "normal"

    return {
        "status": overall,
        "n_compared": n_compared,
        "n_degraded": n_degraded,
        "n_band_degraded": n_band_degraded,
        "n_improved": n_improved,
        "degraded_metric_fraction": degraded_fraction,
        "degraded_metric_percent": degraded_fraction * 100.0,
        "worst_pct_diff": worst_pct_diff,
        "dl_degraded_severity_pct": severity_pct,
        "dl_min_degraded_metrics": min_metrics,
        "dl_degraded_metric_fraction_threshold": fraction_threshold,
    }


def _dl_component_summaries(
    reports: list[dict[str, Any]], config: CvalConfig
) -> dict[str, dict[str, Any]]:
    severity_pct = config.baseline.dl_degraded_severity_pct
    min_metrics = config.baseline.dl_min_degraded_metrics
    fraction = config.baseline.dl_degraded_metric_fraction
    by_component: dict[str, list[dict[str, Any]]] = {
        component: [] for component in DL_COMPONENT_TEST_TYPES.values() if component is not None
    }
    for report in reports:
        component = str(report.get("component", ""))
        if component in by_component:
            by_component[component].append(report)

    summaries: dict[str, dict[str, Any]] = {}
    for component, component_reports in by_component.items():
        if component_reports:
            summary = _summarize_reports(component_reports, is_dl=True, config=config)
            summary["component"] = component
        else:
            summary = _empty_component_summary(
                component,
                severity_pct=severity_pct,
                min_metrics=min_metrics,
                fraction=fraction,
            )
        summaries[component] = summary
    return summaries


def _dl_overall_summary(
    reports: list[dict[str, Any]],
    config: CvalConfig,
    selected_component: str | None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    component_summaries = _dl_component_summaries(reports, config)
    if selected_component:
        selected = component_summaries.get(selected_component) or _empty_component_summary(
            selected_component,
            severity_pct=config.baseline.dl_degraded_severity_pct,
            min_metrics=config.baseline.dl_min_degraded_metrics,
            fraction=config.baseline.dl_degraded_metric_fraction,
        )
        return selected, component_summaries

    summary = _summarize_reports(reports, is_dl=True, config=config)
    degraded_components = [
        component
        for component, component_summary in component_summaries.items()
        if component_summary["status"] == "degraded"
    ]
    improved_components = [
        component
        for component, component_summary in component_summaries.items()
        if component_summary["status"] == "improved"
    ]
    if degraded_components:
        summary["status"] = "degraded"
    elif improved_components and summary["status"] != "degraded":
        summary["status"] = "improved"
    summary["degraded_components"] = degraded_components
    summary["improved_components"] = improved_components
    return summary, component_summaries


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
    baseline_test_type = normalize_baseline_test_type(test_type)
    selected_component = dl_component_for_test_type(test_type)

    node_values = _node_metric_values(
        test_type,
        node,
        config=config,
        db_path=db_path,
        dl_db_paths=dl_db_paths,
        window_days=window_days,
    )

    metric_reports: list[dict[str, Any]] = []
    for key, stat in baseline_metrics.items():
        component = _metric_component(stat)
        if selected_component and component != selected_component:
            continue
        if key not in node_values:
            continue
        value = node_values[key]
        status, pct_diff = stats.classify_value(value, stat)
        counts_for_degraded_status = (
            status == "degraded"
            and baseline_test_type == "dltest"
            and abs(float(pct_diff)) >= config.baseline.dl_degraded_severity_pct
        )
        metric_reports.append(
            {
                "metric": key,
                "component": component,
                "value": value,
                "median": stat.get("median"),
                "status": status,
                "pct_diff": pct_diff,
                "abs_pct_diff": abs(float(pct_diff)),
                "counts_for_degraded_status": counts_for_degraded_status,
                "direction": stat.get("direction"),
                "lower_bound": stat.get("lower_bound"),
                "upper_bound": stat.get("upper_bound"),
            }
        )

    if baseline_test_type == "dltest":
        summary, components = _dl_overall_summary(metric_reports, config, selected_component)
    else:
        summary = _summarize_reports(metric_reports, is_dl=False, config=config)
        components = {}

    verdict = {
        "node": node,
        "test_type": test_type,
        "baseline_test_type": baseline_test_type,
        "dl_component": selected_component or "",
        "baseline_id": baseline.get("baseline_id"),
        "status": summary["status"],
        "n_metrics": len(metric_reports),
        "n_compared": summary["n_compared"],
        "n_degraded": summary["n_degraded"],
        "n_band_degraded": summary["n_band_degraded"],
        "n_improved": summary["n_improved"],
        "degraded_metric_fraction": summary["degraded_metric_fraction"],
        "degraded_metric_percent": summary["degraded_metric_percent"],
        "worst_pct_diff": summary["worst_pct_diff"],
        "metrics": metric_reports,
    }
    if baseline_test_type == "dltest":
        verdict["components"] = components
        verdict["dl_degraded_severity_pct"] = config.baseline.dl_degraded_severity_pct
        verdict["dl_min_degraded_metrics"] = config.baseline.dl_min_degraded_metrics
        verdict["dl_degraded_metric_fraction_threshold"] = (
            config.baseline.dl_degraded_metric_fraction
        )
    return verdict


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

    baseline_test_type = normalize_baseline_test_type(test_type)
    component = dl_component_for_test_type(test_type)

    if baseline_test_type == "storage":
        _collect(db_path or config.storage.storage_db_path, "storage_performance", "timestamp")
    elif baseline_test_type == "nccl":
        _collect(db_path or config.storage.nccl_db_path, "IB_HEALTH", "timestamp")
    elif baseline_test_type == "dltest":
        paths = dl_db_paths or _default_dl_db_paths(config)
        for table, _direction, _tolerance_attr, _keep_rank in _DL_DB_SPECS:
            if component and table != component:
                continue
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
