from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NodeResource:
    name: str
    capacity: int
    allocatable: int
    used: int

    @property
    def free(self) -> int:
        return self.allocatable - self.used

    @property
    def is_fully_free(self) -> bool:
        return self.allocatable > 0 and self.free == self.allocatable


@dataclass(frozen=True)
class QueueCandidate:
    node: str
    priority: int
    last_tested_timestamp: int
    age_days: float | None
    reason: str


@dataclass(frozen=True)
class LatestStatusRow:
    node: str
    test: str
    latest_timestamp: int | None
    result: str


@dataclass(frozen=True)
class RenderedJob:
    job_name: str
    node_name: str
    timestamp: int
    yaml_text: str


@dataclass(frozen=True)
class PlannedJob:
    candidate: QueueCandidate
    rendered_job: RenderedJob


@dataclass(frozen=True)
class WorkflowPlan:
    free_nodes: list[str]
    queue: list[QueueCandidate]
    planned_jobs: list[PlannedJob]
    batch_size: int
    days_threshold: float
    dry_run: bool = True