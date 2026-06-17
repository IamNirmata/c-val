# Baseline Management and Peer-Comparison Classification

c-val 2.0 supports **baselines** and **peer-comparison classification** to identify whether validation results are:
- **normal**: within tolerance of baseline/peer behavior
- **degraded**: outside tolerance, warrants investigation
- **improved**: better than baseline (often acceptable)

## Baseline Directory Structure

Baselines are stored under:

```text
/data/continuous_validation/baselines/{test_type}/{baseline_id}/
  summary.json              (required)
  rank_metrics.json         (optional, for DL)
```

### Example Structure

```
baselines/
  nccl/
    b200-pt2.8.0-cuda12.9/
      summary.json
  storage/
    b200-pt2.8.0-cuda12.9/
      summary.json
  dltest/
    b200-pt2.8.0-cuda12.9/
      summary.json
      rank_metrics.json
```

## Baseline Summary Schema

### NCCL Baseline (summary.json)

```json
{
  "test_plan": "all-reduce-8gpu",
  "timestamp": 1700000000,
  "node": "slc01-cl02-hgx-0001",
  "busbw": 500.0,
  "latency": 25.5
}
```

### Storage Baseline (summary.json)

```json
{
  "test_plan": "80gb-example",
  "timestamp": 1700000000,
  "node": "slc01-cl02-hgx-0001",
  "iodepth_read_1file_iops": 50000.0,
  "iodepth_read_1file_bw": 400.0,
  "iodepth_write_1file_iops": 45000.0,
  "iodepth_write_1file_bw": 350.0,
  "numjobs_read_nfiles_iops": 48000.0,
  "numjobs_read_nfiles_bw": 380.0,
  "numjobs_write_nfiles_iops": 44000.0,
  "numjobs_write_nfiles_bw": 340.0,
  "randread_iops": 35000.0,
  "randread_bw": 300.0,
  "randwrite_iops": 32000.0,
  "randwrite_bw": 280.0
}
```

### DL Test Baseline (summary.json)

```json
{
  "test_plan": "80gb-example",
  "timestamp": 1700000000,
  "node": "slc01-cl02-hgx-0001",
  "task_counts": {
    "nn_tasks": 456,
    "f_tasks": 304,
    "coll_tasks": 192,
    "overlap_tasks": 384
  },
  "status_counts": {
    "completed": 1336
  },
  "numerical_metrics": {
    "task_1": {
      "norm_output": 0.5,
      "weight": 0.1,
      "bias": 0.05
    }
  },
  "collective_metrics": {
    "task_1": {
      "allreduce_time": 1.5
    }
  }
}
```

## Per-Test Comparison Rules

### NCCL: Peer-Comparison Mode

- **tolerance**: 5% (configurable via `nccl_peer_tolerance_pct`)
- **method**: Compare current run against peer baseline or recent average
- **metrics**: `busbw` (bandwidth), `latency`

Example:
```
baseline.busbw = 500 GB/s
result.busbw   = 475 GB/s
pct_diff       = (475 - 500) / 500 = -5% → within 5% tolerance → NORMAL
```

### Storage: Peer-Comparison Mode

- **tolerance**: 10% (configurable via `storage_peer_tolerance_pct`)
- **method**: Compare current run against peer baseline or recent average
- **metrics**: IOPS and bandwidth for all access patterns (read/write, sequential/random)

Example:
```
baseline.iodepth_read_1file_iops = 50000
result.iodepth_read_1file_iops   = 48000
pct_diff                          = (48000 - 50000) / 50000 = -4% → within 10% tolerance → NORMAL
```

### DL Test: Mixed-Mode Baseline

Three different tolerance levels depending on task category:

#### 1. **Compute/Collective Tasks: Tight Baseline (3%)**

Applies to `nn_tasks`, `f_tasks`, `coll_tasks`.

- These tasks should be deterministic or near-deterministic.
- Any significant change may indicate model, driver, or hardware issues.

Example:
```
baseline.nn_tasks     = 456
result.nn_tasks       = 460
pct_diff              = (460 - 456) / 456 = 0.88% → within 3% tolerance → NORMAL
```

#### 2. **Numerical Metrics: Almost-Exact Baseline (0.1%)**

Applies to `norm_output`, `weight`, `bias` in numerical comparison.

- Model numerical outputs should be nearly identical.
- Tiny variations are acceptable (floating-point rounding).
- >0.1% drift suggests numerical instability or precision loss.

Example:
```
baseline.norm_output = 0.5
result.norm_output   = 0.5001
pct_diff             = (0.5001 - 0.5) / 0.5 = 0.02% → within 0.1% tolerance → NORMAL

baseline.norm_output = 0.5
result.norm_output   = 0.51
pct_diff             = (0.51 - 0.5) / 0.5 = 2% → outside 0.1% tolerance → DEGRADED
```

#### 3. **Overlap Tasks: Lenient Baseline (20%)**

Applies to `overlap_tasks` count and overlap variance metrics.

- Overlap varies due to GPU scheduling, memory timing, and network jitter.
- High variance is expected; lenient tolerance avoids false alarms.

Example:
```
baseline.overlap_tasks = 384
result.overlap_tasks   = 410
pct_diff               = (410 - 384) / 384 = 6.77% → within 20% tolerance → NORMAL
```

## Baseline Configuration

Tolerance values are configurable in [config/cval.toml](config/cval.toml):

```toml
[baseline]
nccl_peer_tolerance_pct = 5.0          # NCCL peer comparison
storage_peer_tolerance_pct = 10.0      # Storage peer comparison
dl_compute_tolerance_pct = 3.0         # DL compute/collective tasks
dl_numerical_tolerance_pct = 0.1       # DL numerical metrics
dl_overlap_tolerance_pct = 20.0        # DL overlap tasks
classify_outliers = true               # Enable classification
```

## CLI Commands

### List Stored Baselines

```bash
cval baseline list --test-type nccl
cval baseline list --output json
```

### Load Baseline from Directory

```bash
cval baseline load /data/continuous_validation/baselines/nccl/b200-pt2.8.0-cuda12.9 nccl
```

### Ingest Baseline into DB

```bash
cval baseline ingest /data/continuous_validation/baselines/nccl/b200-pt2.8.0-cuda12.9 nccl
```

### Compare Result vs. Baseline

```bash
cval baseline compare b200-pt2.8.0-cuda12.9 nccl --result-json /path/to/result.json
cval baseline compare b200-pt2.8.0-cuda12.9 storage --output json
```

## Python API

```python
from cval.baselines import (
    BaselineMetrics,
    BaselineConfig,
    load_baseline_summary,
    compute_peer_stats,
    classify_result_vs_baseline,
)
from cval.baselines.storage import (
    store_baseline,
    load_baseline_from_db,
    list_baselines,
)

# Load a baseline from a directory
baseline = load_baseline_summary(
    Path("/data/continuous_validation/baselines/nccl/b200-pt2.8.0-cuda12.9"),
    "nccl"
)

# Store in DB
store_baseline(baseline, db_path=Path("validation.db"))

# Compute peer statistics from recent runs
peer_stats = compute_peer_stats(
    Path("validation.db"),
    test_type="nccl",
    node="slc01-cl02-hgx-0001",
    window_days=7
)

# Classify a new result
config = BaselineConfig(nccl_peer_tolerance_pct=5.0)
result = {"busbw": 495.0, "latency": 26.0}
classification = classify_result_vs_baseline(result, baseline, peer_stats, config)

print(classification["status"])      # "normal", "degraded", or "improved"
print(classification["violations"])  # list of metric violations
```

## Workflow Integration

### Manual Baseline Ingestion

1. Collect a validated set of reference runs (e.g., on a canonical node)
2. Create summary.json in `/data/continuous_validation/baselines/{test_type}/{baseline_id}/`
3. Ingest into DB:
   ```bash
   cval baseline ingest /data/continuous_validation/baselines/{test_type}/{baseline_id} {test_type}
   ```

### Automated Classification in cval-live

When cval-live ingests results, it can classify them:

```python
from cval.baselines.storage import load_baseline_from_db
from cval.baselines.ingest import classify_result_vs_baseline

# After result JSON is written
baseline = load_baseline_from_db("b200-pt2.8.0-cuda12.9", "nccl")
classification = classify_result_vs_baseline(result_dict, baseline)

if classification["status"] == "degraded":
    # Alert or escalate
    log.warning(f"Degraded NCCL result on {node}: {classification['violations']}")
```

### Integration with Hermes

The Hermes `c-val-hpc-engineer` skill can:

1. **list-baselines**: inspect stored baselines
2. **ingest-baseline**: onboard new baseline directories
3. **classify-latest**: compare recent runs against baselines
4. **summarize-outliers**: identify degraded nodes or trends

See [c-val-hpc-engineer/SKILL.md](../skills/c-val-hpc-engineer/SKILL.md) for details.

## Known Limitations and Future Improvements

- **Baseline versioning**: not yet supported; baselines are singletons per test_type/baseline_id
- **Trend analysis**: compute rolling baselines (e.g., last 30 days) to detect slow degradation
- **Multi-dimensional thresholds**: per-node, per-GPU-type, per-test-plan baselines
- **Automatic baseline promotion**: promote "improved" runs to new baseline if stable over time
