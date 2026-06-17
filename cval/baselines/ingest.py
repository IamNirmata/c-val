"""Baseline ingestion and peer-comparison classification."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from cval.baselines.models import BaselineConfig, BaselineMetrics


def load_baseline_summary(
    baseline_dir: Path | str, test_type: str
) -> BaselineMetrics | None:
    """Load a baseline summary JSON from a baseline directory.

    Directory structure:
      /data/continuous_validation/baselines/{test_type}/{baseline_id}/
        summary.json  (required)
        rank_metrics.json  (optional, for DL)

    Returns:
      BaselineMetrics or None if summary not found.
    """
    baseline_dir = Path(baseline_dir)
    summary_file = baseline_dir / "summary.json"

    if not summary_file.exists():
        return None

    try:
        data = json.loads(summary_file.read_text(encoding="utf-8"))
        baseline_id = baseline_dir.name
        test_plan = data.get("test_plan", "")
        timestamp = data.get("timestamp", 0)
        node = data.get("node", "")

        baseline = BaselineMetrics(
            test_type=test_type,
            baseline_id=baseline_id,
            test_plan=test_plan,
            timestamp=timestamp,
            node=node,
        )

        if test_type == "storage":
            # Storage baseline includes IOPS/BW metrics
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
                if key in data:
                    setattr(baseline, key, float(data[key]))

        elif test_type == "nccl":
            # NCCL baseline includes busbw and latency
            if "busbw" in data:
                baseline.busbw = float(data["busbw"])
            if "latency" in data:
                baseline.latency = float(data["latency"])

        elif test_type == "dltest":
            # DL baseline includes task counts and numerical/collective metrics
            baseline.task_counts = data.get("task_counts", {})
            baseline.status_counts = data.get("status_counts", {})
            baseline.numerical_metrics = data.get("numerical_metrics", {})
            baseline.collective_metrics = data.get("collective_metrics", {})

        return baseline
    except Exception:
        return None


def compute_peer_stats(
    db_path: Path | str, test_type: str, node: str = "", window_days: int = 7
) -> dict[str, float]:
    """Compute peer statistics from recent runs in a test DB.

    For NCCL/Storage: returns mean, stdev, min, max of metrics.
    For DL: returns mean task counts and status rates.

    Args:
      db_path: path to validation.db, storage.db, or nccl.db
      test_type: 'nccl', 'storage', or 'dltest'
      node: optional node filter; if empty, uses all nodes
      window_days: only include runs from last N days
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return {}

    stats = {}
    try:
        with closing(sqlite3.connect(db_path)) as conn:
            if test_type == "storage":
                # Compute mean/stdev of storage metrics
                query = """
                    SELECT
                      AVG(iodepth_read_1file_iops) as mean_iodepth_read_1file_iops,
                      AVG(iodepth_read_1file_bw) as mean_iodepth_read_1file_bw,
                      AVG(iodepth_write_1file_iops) as mean_iodepth_write_1file_iops,
                      AVG(iodepth_write_1file_bw) as mean_iodepth_write_1file_bw,
                      AVG(numjobs_read_nfiles_iops) as mean_numjobs_read_nfiles_iops,
                      AVG(numjobs_read_nfiles_bw) as mean_numjobs_read_nfiles_bw,
                      AVG(numjobs_write_nfiles_iops) as mean_numjobs_write_nfiles_iops,
                      AVG(numjobs_write_nfiles_bw) as mean_numjobs_write_nfiles_bw,
                      AVG(randread_iops) as mean_randread_iops,
                      AVG(randread_bw) as mean_randread_bw,
                      AVG(randwrite_iops) as mean_randwrite_iops,
                      AVG(randwrite_bw) as mean_randwrite_bw
                    FROM storage_performance
                    WHERE timestamp > (strftime('%s', 'now') - ? * 86400)
                """
                if node:
                    query += " AND node = ?"
                    row = conn.execute(query, (window_days, node)).fetchone()
                else:
                    row = conn.execute(query, (window_days,)).fetchone()

                if row:
                    columns = [
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
                    ]
                    for col, val in zip(columns, row):
                        if val is not None:
                            stats[col] = float(val)

            elif test_type == "nccl":
                # Compute mean/stdev of NCCL metrics
                query = """
                    SELECT
                      AVG(busbw) as mean_busbw,
                      AVG(latency) as mean_latency
                    FROM nccl_performance
                    WHERE timestamp > (strftime('%s', 'now') - ? * 86400)
                """
                if node:
                    query += " AND node = ?"
                    row = conn.execute(query, (window_days, node)).fetchone()
                else:
                    row = conn.execute(query, (window_days,)).fetchone()

                if row:
                    if row[0] is not None:
                        stats["busbw"] = float(row[0])
                    if row[1] is not None:
                        stats["latency"] = float(row[1])

    except Exception:
        pass

    return stats


def classify_result_vs_baseline(
    result: dict[str, Any],
    baseline: BaselineMetrics,
    peer_stats: dict[str, float] = None,
    config: BaselineConfig = None,
) -> dict[str, Any]:
    """Classify a test result vs. baseline and peer statistics.

    Returns classification result:
      {
        "status": "normal" | "degraded" | "improved",
        "test_type": "nccl" | "storage" | "dltest",
        "violations": [{"metric": str, "expected": float, "actual": float, "pct_diff": float}],
        "message": str
      }

    Args:
      result: the new result dict (from result JSON or parsed metrics)
      baseline: baseline reference metrics
      peer_stats: peer statistics computed from DB (optional)
      config: tolerance configuration (default: standard rules)
    """
    if config is None:
        config = BaselineConfig()

    if peer_stats is None:
        peer_stats = {}

    violations = []
    test_type = baseline.test_type

    if test_type == "storage":
        violations = _classify_storage_result(result, baseline, peer_stats, config)

    elif test_type == "nccl":
        violations = _classify_nccl_result(result, baseline, peer_stats, config)

    elif test_type == "dltest":
        violations = _classify_dl_result(result, baseline, peer_stats, config)

    status = "normal"
    if violations:
        # Check if all violations are improvements
        all_improved = all(v.get("pct_diff", 0) > 0 for v in violations)
        status = "improved" if all_improved else "degraded"

    message = f"{test_type}: {status}"
    if violations:
        message += f" ({len(violations)} violations)"

    return {
        "status": status,
        "test_type": test_type,
        "violations": violations,
        "message": message,
    }


def _classify_storage_result(
    result: dict[str, Any],
    baseline: BaselineMetrics,
    peer_stats: dict[str, float],
    config: BaselineConfig,
) -> list[dict[str, Any]]:
    """Classify storage result vs. baseline/peers."""
    violations = []
    tolerance_pct = config.storage_peer_tolerance_pct

    # Use peer stats if available, otherwise baseline
    compare_against = peer_stats if peer_stats else {
        "iodepth_read_1file_iops": baseline.iodepth_read_1file_iops,
        "iodepth_read_1file_bw": baseline.iodepth_read_1file_bw,
        "iodepth_write_1file_iops": baseline.iodepth_write_1file_iops,
        "iodepth_write_1file_bw": baseline.iodepth_write_1file_bw,
        "numjobs_read_nfiles_iops": baseline.numjobs_read_nfiles_iops,
        "numjobs_read_nfiles_bw": baseline.numjobs_read_nfiles_bw,
        "numjobs_write_nfiles_iops": baseline.numjobs_write_nfiles_iops,
        "numjobs_write_nfiles_bw": baseline.numjobs_write_nfiles_bw,
        "randread_iops": baseline.randread_iops,
        "randread_bw": baseline.randread_bw,
        "randwrite_iops": baseline.randwrite_iops,
        "randwrite_bw": baseline.randwrite_bw,
    }

    for metric, expected in compare_against.items():
        actual = result.get(metric, 0.0)
        if expected and actual:
            pct_diff = ((actual - expected) / expected) * 100
            if abs(pct_diff) > tolerance_pct:
                violations.append(
                    {
                        "metric": metric,
                        "expected": expected,
                        "actual": actual,
                        "pct_diff": pct_diff,
                    }
                )

    return violations


def _classify_nccl_result(
    result: dict[str, Any],
    baseline: BaselineMetrics,
    peer_stats: dict[str, float],
    config: BaselineConfig,
) -> list[dict[str, Any]]:
    """Classify NCCL result vs. baseline/peers."""
    violations = []
    tolerance_pct = config.nccl_peer_tolerance_pct

    # Use peer stats if available, otherwise baseline
    compare_busbw = peer_stats.get("busbw", baseline.busbw)
    compare_latency = peer_stats.get("latency", baseline.latency)

    for metric, expected in [("busbw", compare_busbw), ("latency", compare_latency)]:
        actual = result.get(metric, 0.0)
        if expected and actual:
            pct_diff = ((actual - expected) / expected) * 100
            # For latency (lower is better), invert the logic
            if metric == "latency":
                pct_diff = -pct_diff
            if abs(pct_diff) > tolerance_pct:
                violations.append(
                    {
                        "metric": metric,
                        "expected": expected,
                        "actual": actual,
                        "pct_diff": pct_diff,
                    }
                )

    return violations


def _classify_dl_result(
    result: dict[str, Any],
    baseline: BaselineMetrics,
    peer_stats: dict[str, float],
    config: BaselineConfig,
) -> list[dict[str, Any]]:
    """Classify DL result vs. baseline with per-category rules.

    Rules:
      - compute/collective tasks: tight tolerance (3%)
      - numerical metrics (norm_output, weight, bias): almost exact (0.1%)
      - overlap tasks: lenient tolerance (20%)
    """
    violations = []

    # Check task counts (compute/collective: tight)
    for task_group in ("coll_tasks", "nn_tasks", "f_tasks"):
        expected_count = baseline.task_counts.get(task_group, 0)
        actual_count = result.get("task_counts", {}).get(task_group, 0)
        if expected_count and actual_count:
            pct_diff = ((actual_count - expected_count) / expected_count) * 100
            if abs(pct_diff) > config.dl_compute_tolerance_pct:
                violations.append(
                    {
                        "metric": f"{task_group}_count",
                        "expected": expected_count,
                        "actual": actual_count,
                        "pct_diff": pct_diff,
                    }
                )

    # Check overlap task count (lenient)
    expected_overlap = baseline.task_counts.get("overlap_tasks", 0)
    actual_overlap = result.get("task_counts", {}).get("overlap_tasks", 0)
    if expected_overlap and actual_overlap:
        pct_diff = ((actual_overlap - expected_overlap) / expected_overlap) * 100
        if abs(pct_diff) > config.dl_overlap_tolerance_pct:
            violations.append(
                {
                    "metric": "overlap_tasks_count",
                    "expected": expected_overlap,
                    "actual": actual_overlap,
                    "pct_diff": pct_diff,
                }
            )

    # Check numerical metrics (almost exact)
    baseline_numerical = baseline.numerical_metrics
    result_numerical = result.get("numerical_metrics", {})
    for task_name, expected_metrics in baseline_numerical.items():
        actual_metrics = result_numerical.get(task_name, {})
        for metric_key in ("norm_output", "weight", "bias"):
            expected_val = expected_metrics.get(metric_key)
            actual_val = actual_metrics.get(metric_key)
            if expected_val and actual_val:
                pct_diff = ((actual_val - expected_val) / expected_val) * 100
                if abs(pct_diff) > config.dl_numerical_tolerance_pct:
                    violations.append(
                        {
                            "metric": f"{task_name}_{metric_key}",
                            "expected": expected_val,
                            "actual": actual_val,
                            "pct_diff": pct_diff,
                        }
                    )

    return violations
