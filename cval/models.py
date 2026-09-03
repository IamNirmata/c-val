"""Shared data models for c-val orchestration.

The rest of the package passes these immutable dataclasses between discovery,
priority, rendering, submission, monitoring, and result-status flows. Keeping
the shape explicit makes queue inspection and cluster evidence easy to reason about.
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
    resource_ready: bool = True

    @property
    def free(self) -> int:
        """Return free GPUs after subtracting active pod requests."""

        return self.allocatable - self.used

    @property
    def is_fully_free(self) -> bool:
        """Return true when a GPU node has no active GPU workload requests."""

        return self.resource_ready and self.allocatable > 0 and self.free == self.allocatable


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
    git_ref: str
    yaml_text: str


@dataclass(frozen=True)
class PlannedJob:
    """Queue candidate plus its rendered job manifest."""

    candidate: QueueCandidate
    rendered_job: RenderedJob


@dataclass(frozen=True)
class WorkflowPlan:
    """Nonmutating workflow plan used for inspection or confirmed submission."""

    free_nodes: list[str]
    queue: list[QueueCandidate]
    planned_jobs: list[PlannedJob]
    batch_size: int
    days_threshold: float


@dataclass(frozen=True)
class NcclMetrics:
    """Latest NCCL performance metrics for one node (from test-nccl.db)."""

    busbw: float | None
    latency: float | None


@dataclass(frozen=True)
class NcclHealthMetric:
    """One consolidated NCCL/IB health row for a node's latest run."""

    node: str
    timestamp: int | None
    la_timestamp: str
    iterations: int | None
    image_name: str
    cuda: str
    pytorch: str
    samples: int | None
    bus_bw: float | None
    latency: float | None
    port_max_gbps: dict[str, float | None]


@dataclass(frozen=True)
class StorageMetrics:
    """Latest FIO storage performance metrics for one node (from test-storage.db)."""

    iodepth_read_1file_iops: float | None
    iodepth_read_1file_bw: float | None
    iodepth_write_1file_iops: float | None
    iodepth_write_1file_bw: float | None
    numjobs_read_nfiles_iops: float | None
    numjobs_read_nfiles_bw: float | None
    numjobs_write_nfiles_iops: float | None
    numjobs_write_nfiles_bw: float | None
    randread_iops: float | None
    randread_bw: float | None
    randwrite_iops: float | None
    randwrite_bw: float | None