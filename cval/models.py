"""Shared data models for c-val orchestration.

The rest of the package passes these immutable dataclasses between discovery,
priority, rendering, submission, monitoring, and result-status flows. Keeping
the shape explicit makes dry-run output and tests easy to reason about.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NodeResource:
    """GPU and accelerator resource snapshot for one Kubernetes node."""

    name: str
    capacity: int
    allocatable: int
    used: int

    @property
    def free(self) -> int:
        """Return free GPUs after subtracting active pod requests."""

        return self.allocatable - self.used

    @property
    def is_fully_free(self) -> bool:
        """Return true when a GPU node has no active GPU workload requests."""

        return self.allocatable > 0 and self.free == self.allocatable


@dataclass(frozen=True)
class QueueCandidate:
    """One node selected for validation, with priority metadata."""

    node: str
    priority: int
    last_tested_timestamp: int
    age_days: float | None
    reason: str


@dataclass(frozen=True)
class LatestStatusRow:
    """One row from the validation latest-status view."""

    node: str
    test: str
    latest_timestamp: int | None
    result: str


@dataclass(frozen=True)
class RenderedJob:
    """Rendered Kubernetes/Volcano manifest for one validation job."""

    job_name: str
    node_name: str
    timestamp: int
    yaml_text: str


@dataclass(frozen=True)
class PlannedJob:
    """Queue candidate plus its rendered job manifest."""

    candidate: QueueCandidate
    rendered_job: RenderedJob


@dataclass(frozen=True)
class WorkflowPlan:
    """Dry-run workflow plan produced before any Kubernetes create call."""

    free_nodes: list[str]
    queue: list[QueueCandidate]
    planned_jobs: list[PlannedJob]
    batch_size: int
    days_threshold: float
    dry_run: bool = True