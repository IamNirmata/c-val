"""Dry-run workflow planning.

The workflow planner is the package replacement for notebook-local
orchestration. It combines free nodes, validation history, prioritization, and
job rendering into a `WorkflowPlan` that can be inspected before submission.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from cval.config import CvalConfig, load_config
from cval.jobs.renderer import default_template_path, render_validation_job_from_file
from cval.models import PlannedJob, WorkflowPlan
from cval.scheduler.priority import build_priority_queue


def build_workflow_plan(
    free_nodes: Sequence[str],
    latest_status_by_node: Mapping[str, int],
    days_threshold: float | None = None,
    batch_size: int | None = None,
    template_path: Path | None = None,
    timestamp: int | None = None,
    job_prefix: str | None = None,
    git_repo: str | None = None,
    git_ref: str | None = None,
    now: datetime | None = None,
    config: CvalConfig | None = None,
) -> WorkflowPlan:
    """Build a dry-run plan containing prioritized nodes and rendered jobs."""

    active_config = config or load_config()
    resolved_days_threshold = (
        days_threshold if days_threshold is not None else active_config.scheduling.days_threshold
    )
    resolved_batch_size = batch_size if batch_size is not None else active_config.scheduling.batch_size
    resolved_template_path = template_path or active_config.job.template_path or default_template_path()
    resolved_job_prefix = job_prefix if job_prefix is not None else active_config.job.job_prefix
    resolved_git_repo = git_repo if git_repo is not None else active_config.job.git_repo
    resolved_git_ref = git_ref if git_ref is not None else active_config.job.git_ref

    if resolved_batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    # Priority is pure logic: it does not perform Kubernetes or DB calls here.
    queue = build_priority_queue(
        free_nodes,
        latest_status_by_node,
        days_threshold=resolved_days_threshold,
        now=now,
    )
    # Only the first batch is rendered; the full queue remains available for visibility.
    planned_jobs = [
        PlannedJob(
            candidate=candidate,
            rendered_job=render_validation_job_from_file(
                resolved_template_path,
                node_name=candidate.node,
                timestamp=timestamp,
                job_prefix=resolved_job_prefix,
                git_repo=resolved_git_repo,
                git_ref=resolved_git_ref,
                job_template_config=active_config.job_template,
            ),
        )
        for candidate in queue[:resolved_batch_size]
    ]

    return WorkflowPlan(
        free_nodes=list(free_nodes),
        queue=queue,
        planned_jobs=planned_jobs,
        batch_size=resolved_batch_size,
        days_threshold=resolved_days_threshold,
        dry_run=True,
    )


def workflow_plan_to_dict(plan: WorkflowPlan, include_yaml: bool = False) -> dict[str, object]:
    """Convert a workflow plan into JSON-serializable output for CLI/Hermes."""

    return {
        "dry_run": plan.dry_run,
        "batch_size": plan.batch_size,
        "days_threshold": plan.days_threshold,
        "free_nodes_count": len(plan.free_nodes),
        "queue_count": len(plan.queue),
        "planned_jobs": [
            {
                "priority": planned.candidate.priority,
                "node": planned.candidate.node,
                "reason": planned.candidate.reason,
                "last_tested_timestamp": planned.candidate.last_tested_timestamp,
                "age_days": planned.candidate.age_days,
                "job_name": planned.rendered_job.job_name,
                # YAML is large and omitted by default to keep plan summaries readable.
                **({"yaml_text": planned.rendered_job.yaml_text} if include_yaml else {}),
            }
            for planned in plan.planned_jobs
        ],
    }