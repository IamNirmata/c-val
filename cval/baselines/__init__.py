"""Operational storage and DL baseline management.

Baselines provide reference data (historical runs, canonical test-plans) and
peer-comparison rules to classify new results as:
  - normal: within tolerance of baseline/peer behavior
  - degraded: outside tolerance, requires investigation
  - improved: better than baseline (often acceptable)

Per-test rules:
  - Storage: peer-comparison mode
  - DL: mixed mode:
    - compute/collective tasks: tight baseline (strict %)
    - numerical (norm_output, weight, bias): exact baseline (very tight %)
    - overlap: lenient baseline (high tolerance for variance)

The PostgreSQL ``cval.nccl_eval`` subsystem is the only NCCL evaluator.
"""

__all__ = [
    "build_baseline",
    "store_dynamic_baseline",
    "activate_baseline",
    "get_active_baseline",
    "load_dynamic_baseline",
    "list_dynamic_baselines",
    "default_dynamic_baseline_db_path",
    "default_dynamic_baseline_db_paths",
    "default_classification_db_path",
    "default_classification_db_paths",
    "store_classification_results",
    "classify_node",
    "classify_nodes",
]

from cval.baselines.build import build_baseline
from cval.baselines.classify import classify_node, classify_nodes
from cval.baselines.storage import (
    activate_baseline,
    default_classification_db_path,
    default_classification_db_paths,
    default_dynamic_baseline_db_path,
    default_dynamic_baseline_db_paths,
    get_active_baseline,
    list_dynamic_baselines,
    load_dynamic_baseline,
    store_classification_results,
    store_dynamic_baseline,
)
