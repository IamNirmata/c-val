"""Baseline data models and configuration."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BaselineMetrics:
    """Reference metrics for a single test baseline.

    Attributes:
      test_type: 'nccl', 'storage', or 'dltest'
      baseline_id: identifier (e.g., 'b200-pt2.8.0-cuda12.9')
      test_plan: test plan name (e.g., '80gb-example')
      timestamp: unix timestamp when baseline was captured
      node: node used for baseline (optional; used for classification)

    Storage-specific:
      iodepth_read_1file_iops, iodepth_read_1file_bw, etc.

    NCCL-specific:
      busbw: aggregated bus bandwidth (GB/s)
      latency: latency (us)

    DL-specific:
      task_counts: {'nn_tasks': int, 'f_tasks': int, 'coll_tasks': int, 'overlap_tasks': int}
      status_counts: {'completed': int, other statuses}
      numerical_metrics: dict of {'task_name': {'norm_output': float, 'weight': float, 'bias': float, ...}}
      collective_metrics: dict of {'task_name': {'allreduce_time': float, ...}}
    """

    test_type: str  # 'nccl', 'storage', 'dltest'
    baseline_id: str
    test_plan: str = ""
    timestamp: int = 0
    node: str = ""

    # Storage metrics
    iodepth_read_1file_iops: float = 0.0
    iodepth_read_1file_bw: float = 0.0
    iodepth_write_1file_iops: float = 0.0
    iodepth_write_1file_bw: float = 0.0
    numjobs_read_nfiles_iops: float = 0.0
    numjobs_read_nfiles_bw: float = 0.0
    numjobs_write_nfiles_iops: float = 0.0
    numjobs_write_nfiles_bw: float = 0.0
    randread_iops: float = 0.0
    randread_bw: float = 0.0
    randwrite_iops: float = 0.0
    randwrite_bw: float = 0.0

    # NCCL metrics
    busbw: float = 0.0
    latency: float = 0.0

    # DL metrics
    task_counts: dict = field(default_factory=dict)
    status_counts: dict = field(default_factory=dict)
    numerical_metrics: dict = field(default_factory=dict)
    collective_metrics: dict = field(default_factory=dict)


@dataclass
class BaselineConfig:
    """Configuration for baseline/peer-comparison rules per test type.

    Attributes:
      nccl_peer_tolerance_pct: peer comparison tolerance for NCCL busbw/latency (%)
      storage_peer_tolerance_pct: peer comparison tolerance for storage metrics (%)
      dl_compute_tolerance_pct: DL collective/compute task tolerance (%)
      dl_numerical_tolerance_pct: DL numerical metrics tolerance (very strict, %)
      dl_overlap_tolerance_pct: DL overlap task tolerance (lenient, %)
      classify_outliers: enable/disable outlier classification
    """

    nccl_peer_tolerance_pct: float = 5.0  # strict peer baseline
    storage_peer_tolerance_pct: float = 10.0  # moderate peer baseline
    dl_compute_tolerance_pct: float = 3.0  # tight for compute/collective
    dl_numerical_tolerance_pct: float = 0.1  # almost exact for numerical
    dl_overlap_tolerance_pct: float = 20.0  # lenient for overlap variance
    classify_outliers: bool = True
