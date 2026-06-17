"""Baseline management and peer-comparison classification for c-val metrics.

Baselines provide reference data (historical runs, canonical test-plans) and
peer-comparison rules to classify new results as:
  - normal: within tolerance of baseline/peer behavior
  - degraded: outside tolerance, requires investigation
  - improved: better than baseline (often acceptable)

Per-test rules:
  - NCCL: peer-comparison mode (compare vs. peers on same node/cluster)
  - Storage: peer-comparison mode
  - DL: mixed mode:
    - compute/collective tasks: tight baseline (strict %)
    - numerical (norm_output, weight, bias): exact baseline (very tight %)
    - overlap: lenient baseline (high tolerance for variance)
"""

__all__ = [
    "BaselineMetrics",
    "BaselineConfig",
    "load_baseline_summary",
    "compute_peer_stats",
    "classify_result_vs_baseline",
]

from cval.baselines.models import (
    BaselineConfig,
    BaselineMetrics,
)
from cval.baselines.ingest import (
    load_baseline_summary,
    compute_peer_stats,
    classify_result_vs_baseline,
)
