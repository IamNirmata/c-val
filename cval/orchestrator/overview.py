"""One-screen operational overview of continuous validation.

Aggregates the read-only views an operator normally checks one at a time:
free nodes, fleet result freshness (valid vs outdated), the live priority
queue, and active validation job phases. Every section degrades gracefully:
a failure in one source is recorded in ``errors`` rather than aborting the
whole overview.
"""

from __future__ import annotations

import datetime
import time
from typing import Any

from cval.config import CvalConfig, load_config
from cval.jobs.monitor import JobPhase, list_job_phases
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import discover_free_nodes, fully_free_node_names
from cval.scheduler.priority import build_priority_queue
from cval.storage.classification_status import get_latest_classification_rows
from cval.storage.status import get_latest_status_rows, latest_status_rows_to_node_map
from cval.validation.operational_targets import (
    BASELINE_CLASSIFY,
    build_operational_target_catalog,
)

ACTIVE_PHASES = ("Pending", "Running")
SECONDS_PER_DAY = 86400


def _freshness_counts(
    status_map: dict[str, int], days_threshold: float, now: float
) -> tuple[int, int]:
    """Return (valid, outdated) node counts relative to the threshold."""

    threshold_seconds = days_threshold * SECONDS_PER_DAY
    valid = 0
    outdated = 0
    for timestamp in status_map.values():
        if timestamp and (now - timestamp) <= threshold_seconds:
            valid += 1
        else:
            outdated += 1
    return valid, outdated


def _summarize_jobs(jobs: list[JobPhase]) -> dict[str, int]:
    """Count jobs by phase."""

    summary: dict[str, int] = {}
    for job in jobs:
        summary[job.phase] = summary.get(job.phase, 0) + 1
    return summary


def _summarize_classifications(rows) -> dict[str, dict[str, int]]:
    """Count latest classification verdicts by test type and status."""

    summary: dict[str, dict[str, int]] = {}
    for row in rows:
        by_status = summary.setdefault(row.test_type, {})
        by_status[row.status] = by_status.get(row.status, 0) + 1
    return summary


def build_overview(
    *,
    config: CvalConfig | None = None,
    node_filter: str | None = None,
    days_threshold: float | None = None,
    queue_limit: int = 10,
    namespace: str | None = None,
    include_jobs: bool = True,
    client: KubectlClient | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Assemble the overview by reading free nodes, status, queue, and jobs."""

    config = config or load_config()
    node_filter = node_filter if node_filter is not None else config.cluster.node_filter
    days_threshold = (
        days_threshold if days_threshold is not None else config.scheduling.days_threshold
    )
    namespace = namespace or config.cluster.namespace
    now = time.time() if now is None else now
    client = client or KubectlClient()

    errors: dict[str, str] = {}

    # 1. Free nodes (live discovery).
    total_nodes = 0
    try:
        nodes, totals = discover_free_nodes(node_name_filter=node_filter)
        total_nodes = len(nodes)
        fully_free = fully_free_node_names(nodes)
    except Exception as exc:  # noqa: BLE001 - surfaced in the report, not raised
        totals = {"capacity": 0, "allocatable": 0, "used": 0, "free": 0}
        fully_free = []
        errors["nodes"] = str(exc)

    # 2. Result freshness from the latest-status DB (read-only via PVC pod).
    status_map: dict[str, int] = {}
    try:
        rows = get_latest_status_rows(
            client=client,
            namespace=namespace,
            pod=config.cluster.pvc_access_pod,
            db_path=config.storage.validation_db_path,
            config=config,
        )
        status_map = latest_status_rows_to_node_map(rows)
    except Exception as exc:  # noqa: BLE001
        errors["status"] = str(exc)
    valid, outdated = _freshness_counts(status_map, days_threshold, now)
    tested_nodes = len(status_map)
    untested_nodes = max(total_nodes - tested_nodes, 0)
    coverage_pct = (tested_nodes / total_nodes * 100.0) if total_nodes else 0.0

    # 3. Priority queue: fully-free nodes that need validation, with reasons.
    queue = build_priority_queue(fully_free, status_map, days_threshold=days_threshold)

    # 4. Active validation jobs.
    jobs: list[JobPhase] = []
    if include_jobs:
        try:
            jobs = list_job_phases(
                namespace=namespace, prefix=config.job.job_prefix, client=client
            )
        except Exception as exc:  # noqa: BLE001
            errors["jobs"] = str(exc)
    job_summary = _summarize_jobs(jobs)

    classification_rows = []
    try:
        classification_rows = get_latest_classification_rows(
            client=client,
            namespace=namespace,
            pod=config.cluster.pvc_access_pod,
            config=config,
        )
        enabled_classification_targets = set(
            build_operational_target_catalog(config.tests.registry).names_for(
                BASELINE_CLASSIFY
            )
        )
        classification_rows = [
            row
            for row in classification_rows
            if row.test_type in enabled_classification_targets
        ]
    except Exception as exc:  # noqa: BLE001
        errors["classifications"] = str(exc)
    classification_summary = _summarize_classifications(classification_rows)

    return {
        "generated_at": int(now),
        "namespace": namespace,
        "days_threshold": days_threshold,
        "nodes": {
            "total_nodes": total_nodes,
            "fully_free": len(fully_free),
            "free_gpus": totals.get("free", 0),
            "total_gpus": totals.get("capacity", 0),
            "fully_free_names": fully_free,
        },
        "freshness": {
            "nodes_with_results": len(status_map),
            "tested_nodes": tested_nodes,
            "total_nodes": total_nodes,
            "untested_nodes": untested_nodes,
            "coverage_pct": coverage_pct,
            "valid": valid,
            "outdated": outdated,
        },
        "queue": {
            "needing_validation": len(queue),
            "candidates": [
                {
                    "priority": candidate.priority,
                    "node": candidate.node,
                    "reason": candidate.reason,
                    "age_days": candidate.age_days,
                    "last_tested_timestamp": candidate.last_tested_timestamp,
                }
                for candidate in queue[:queue_limit]
            ],
        },
        "jobs": {
            "total": len(jobs),
            "by_phase": job_summary,
            "active": [
                {"job_name": job.job_name, "phase": job.phase}
                for job in jobs
                if job.phase in ACTIVE_PHASES
            ],
            "items": [{"job_name": job.job_name, "phase": job.phase} for job in jobs],
        },
        "classifications": {
            "total": len(classification_rows),
            "by_test": classification_summary,
        },
        "errors": errors,
    }


def _fmt_time(epoch: int) -> str:
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%SZ"
    )


def render_overview(overview: dict[str, Any]) -> str:
    """Render the overview dict as a compact operator dashboard."""

    lines: list[str] = []
    lines.append(
        f"c-val overview  |  ns={overview['namespace']}  |  "
        f"threshold={overview['days_threshold']}d  |  {_fmt_time(overview['generated_at'])}"
    )
    lines.append("-" * 76)

    nodes = overview["nodes"]
    lines.append(
        f"NODES     total: {nodes.get('total_nodes', 0):<5}  fully-free: {nodes['fully_free']:<4}  "
        f"free GPUs: {nodes['free_gpus']}/{nodes['total_gpus']}"
    )

    freshness = overview["freshness"]
    lines.append(
        f"RESULTS   tested: {freshness.get('tested_nodes', freshness['nodes_with_results'])}/"
        f"{freshness.get('total_nodes', 0):<5}  "
        f"coverage: {freshness.get('coverage_pct', 0.0):>5.1f}%  "
        f"valid(<thr): {freshness['valid']:<5}  outdated: {freshness['outdated']}"
    )

    classifications = overview.get("classifications", {"by_test": {}})
    if classifications.get("by_test"):
        lines.append("CLASSIFY  latest baseline verdicts")
        for test_type, counts in sorted(classifications["by_test"].items()):
            status_text = ", ".join(
                f"{status}={count}" for status, count in sorted(counts.items())
            )
            lines.append(f"  {test_type:<20} {status_text}")

    queue = overview["queue"]
    lines.append(f"QUEUE     needing validation: {queue['needing_validation']}")
    if queue["candidates"]:
        lines.append(f"  {'PRI':>3} {'NODE':<30} {'REASON':<13} AGE_DAYS")
        for candidate in queue["candidates"]:
            age = "" if candidate["age_days"] is None else f"{candidate['age_days']:.1f}"
            lines.append(
                f"  {candidate['priority']:>3} {candidate['node']:<30} "
                f"{candidate['reason']:<13} {age}"
            )

    jobs = overview["jobs"]
    phase_text = (
        ", ".join(f"{phase}={count}" for phase, count in sorted(jobs["by_phase"].items()))
        or "none"
    )
    lines.append(f"JOBS      total: {jobs['total']}  ({phase_text})")
    for item in jobs["active"]:
        lines.append(f"  {item['phase']:<10} {item['job_name']}")

    if overview["errors"]:
        lines.append("-" * 76)
        for source, message in overview["errors"].items():
            first_line = message.splitlines()[0] if message else ""
            lines.append(f"! {source}: {first_line}")

    return "\n".join(lines)
