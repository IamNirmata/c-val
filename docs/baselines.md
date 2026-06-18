# Baselines and Node Classification

c-val builds **baselines** from historical validation results and uses them to
**classify nodes** as:

- **normal**: within the baseline's acceptance band
- **degraded**: on the failing side of the band; warrants investigation
- **improved**: better than the good-side tail (informational)

There are two layers:

1. **Dynamic baselines (recommended)** — built directly from the result DBs with
   robust statistics, stored as versioned records in SQLite, and used to
   classify nodes. This is the primary system.
2. **Directory baselines (legacy)** — hand-authored `summary.json` references
   loaded from disk, for fixed golden references.

---

## How dynamic baselines are built (the method)

Performance metrics on a GPU fleet are skewed and routinely contaminated by a few
bad nodes. Mean and standard deviation break down in exactly that case — one bad
run shifts the mean and inflates the deviation, hiding the next anomaly — so c-val
uses **robust statistics** instead.

For each metric, over a rolling window per stratum:

1. **Collect** recent values from the result DB (non-positive performance
   readings are dropped as failed/missing runs).
2. **Trim** extreme outliers iteratively using the modified z-score
   (Iglewicz & Hoaglin), threshold `3.5`.
3. **Summarize** with the **median** (center) and **MAD** scaled to sigma
   (`1.4826 × MAD`) for spread, plus IQR, percentiles, skewness, kurtosis, and a
   bootstrap confidence interval for the median.
4. **Set a directional acceptance band** (below).

### Directional acceptance bands

The half-width of the band is:

```text
delta = max(z * 1.4826 * MAD,  tolerance_pct/100 * |median|)
```

with `z = robust_z_threshold` (default 3.5). The relative-tolerance floor means a
freakishly tight MAD can never make classification more sensitive than the
configured engineering tolerance.

Directionality decides which side is a failure:

| Direction | Meaning | Metrics |
| --- | --- | --- |
| `low_bad` | higher is better; only the low side fails | busbw, all storage IOPS/BW |
| `high_bad` | lower is better; only the high side fails | NCCL latency, DL time metrics |
| `two_sided` | any deviation fails | DL numerical correctness, overlap |

### Deterministic metrics (MAD = 0)

DL numerical-correctness outputs should be bit-reproducible for a fixed image and
seed, so more than half the samples are identical and MAD is `0`. c-val detects
this, avoids dividing by zero (MeanAD fallback in the z-score), and uses the tight
relative tolerance (`dl_numerical_tolerance_pct`, default 0.1%) as the band. Any
real spread is itself a signal.

### Stratification

Baselines are computed per stratum so comparisons stay apples-to-apples. The keys
differ by test type because the schemas do:

- **storage / NCCL**: stratify by `image_name` (and optionally `node`).
- **DL**: stratify by `test_plan`.

GPU SKU / topology are not yet recorded per run and are left for future work.

### Sample size

A metric is only baselined when it has at least `min_samples` clean values
(default 8); median/MAD are unstable below that.

---

## Per-test rules

| Test | Source DB | Metrics | Direction | Tolerance floor |
| --- | --- | --- | --- | --- |
| NCCL | `test-nccl.db` (`nccl_performance`) | busbw, latency | busbw `low_bad`, latency `high_bad` | `nccl_peer_tolerance_pct` (5%) |
| Storage | `test-storage.db` (`storage_performance`) | 12 IOPS/BW columns | `low_bad` | `storage_peer_tolerance_pct` (10%) |
| DL numerical | `dltest_numerical_correctness.db` | per `task/rank/metric` | `two_sided` | `dl_numerical_tolerance_pct` (0.1%) |
| DL compute | `dltest_compute_performance.db` | fp/bp cpu/gpu time | `high_bad` | `dl_compute_tolerance_pct` (3%) |
| DL collective | `dltest_collective_performance.db` | cpu/gpu time | `high_bad` | `dl_compute_tolerance_pct` (3%) |
| DL overlap | `dltest_overlap_performance.db` | coll/layer mean/stdev | `two_sided` | `dl_overlap_tolerance_pct` (20%) |

DL numerical keeps `rank` in the metric key (ranks may legitimately differ and
must stay near-exact per rank); DL performance metrics pool ranks since the GPUs
are timing peers.

---

## Versioned baseline records

Baselines are immutable, versioned records in the `baselines` table of
`validation.db`, with a lifecycle status:

```text
candidate  ->  active  ->  superseded
```

`build --store` writes a `candidate`. `activate` promotes it to `active` and
supersedes the previous active baseline **for the same `(test_type, stratum)`**,
so different strata keep independent active baselines. New results are always
classified against the single `active` baseline (or an explicit `--baseline-id`).
Candidates are the default so a slowly degrading fleet cannot silently
re-baseline itself.

### Stored record schema (`metrics_json`)

```json
{
  "schema_version": "cval.baseline.v2",
  "baseline_id": "nccl-image=pytorch:26.05-py3-1781000000",
  "test_type": "nccl",
  "stratum_key": "image=pytorch:26.05-py3",
  "window_days": 30,
  "created_at": 1781000000,
  "n_samples": 42,
  "method": "robust_mad",
  "metrics": {
    "busbw": {
      "metric": "busbw",
      "direction": "low_bad",
      "n": 40, "n_excluded": 2,
      "median": 480.0, "mad": 3.0, "mad_sigma": 4.45, "iqr": 5.0,
      "p01": 470.0, "p05": 473.0, "p25": 478.0, "p50": 480.0,
      "p75": 483.0, "p95": 487.0, "p99": 490.0,
      "minimum": 465.0, "maximum": 492.0,
      "skewness": -0.1, "kurtosis": 0.2,
      "ci_low": 478.0, "ci_high": 482.0,
      "deterministic": false,
      "lower_bound": 456.0, "upper_bound": null,
      "method": "robust_mad"
    }
  }
}
```

`null` bounds mean "unbounded on that side" (one-sided performance metrics).

---

## CLI

### Build a baseline

```bash
# Build from the NCCL DB over the last 30 days, store as a candidate
python -m cval.cli baseline build --test-type nccl --window-days 30 --store

# Stratify storage by image, then store and promote to active in one step
python -m cval.cli baseline build --test-type storage \
  --image-name pytorch:26.05-py3 --baseline-id storage-2026Q2 --activate

# DL baseline for one test plan, JSON output (no store)
python -m cval.cli baseline build --test-type dltest --test-plan 80gb-example --output json
```

Useful flags: `--min-samples`, `--node`, `--source-db`, `--db-path` (where the
baseline is stored), `--store`, `--activate`.

### Activate / show / list

```bash
python -m cval.cli baseline activate storage-2026Q2 storage
python -m cval.cli baseline show storage-2026Q2 storage
python -m cval.cli baseline list --test-type storage --output json
```

### Classify nodes

```bash
# Classify all nodes seen in the window against the active storage baseline
python -m cval.cli baseline classify --test-type storage

# One node, JSON output, against an explicit baseline id
python -m cval.cli baseline classify --test-type nccl \
  --node slc01-cl02-hgx-0009 --baseline-id nccl-2026Q2 --output json
```

Table output:

```text
Classification vs baseline storage-2026Q2 (storage)
NODE                             STATUS    DEGRADED IMPROVED COMPARED
slc01-cl02-hgx-0001              normal           0        0       12
slc01-cl02-hgx-0009              degraded        12        0       12
Degraded nodes: slc01-cl02-hgx-0009
```

A node's value for each metric is the **median of its recent runs** in the window,
so a single noisy run does not flip the verdict. A node is `degraded` if any
metric falls on the failing side of its band, `improved` if some metric beats the
good-side tail (p95/p05) and none are degraded, else `normal`.

---

## Python API

```python
from cval.baselines import (
    build_baseline,
    store_dynamic_baseline,
    activate_baseline,
    get_active_baseline,
    classify_node,
    classify_nodes,
)

# 1. Build a baseline from the result DBs (robust stats).
record = build_baseline("storage", image_name="pytorch:26.05-py3", window_days=30)

# 2. Persist as a candidate, then promote to active.
store_dynamic_baseline(record)                       # status = candidate
activate_baseline(record["baseline_id"], "storage")  # status = active

# 3. Classify nodes against the active baseline.
baseline = get_active_baseline("storage")
verdicts = classify_nodes("storage", baseline)
degraded = [v["node"] for v in verdicts if v["status"] == "degraded"]
```

Low-level robust statistics live in `cval.baselines.stats` (`summarize_metric`,
`classify_value`, `median`, `mad`, `modified_zscores`, `tukey_fences`,
`bootstrap_median_ci`).

---

## Configuration

```toml
[baseline]
nccl_peer_tolerance_pct = 5.0       # NCCL relative-tolerance floor
storage_peer_tolerance_pct = 10.0   # Storage relative-tolerance floor
dl_compute_tolerance_pct = 3.0      # DL compute/collective time
dl_numerical_tolerance_pct = 0.1    # DL numerical correctness (near-exact)
dl_overlap_tolerance_pct = 20.0     # DL overlap (high variance)
classify_outliers = true            # enable classification
robust_z_threshold = 3.5            # modified z-score cutoff
min_samples = 8                     # minimum clean samples per metric
window_days = 30                    # rolling window for building baselines
```

These values feed both baseline building (band width, trimming, window) and
classification.

---

## Directory baselines (legacy)

Hand-authored references are still supported for fixed golden baselines. They live
under `/data/continuous_validation/baselines/{test_type}/{baseline_id}/` with a
`summary.json`, and use the `load` / `ingest` / `compare` commands:

```bash
cval baseline load   /data/continuous_validation/baselines/nccl/<id> nccl
cval baseline ingest /data/continuous_validation/baselines/nccl/<id> nccl
cval baseline compare <id> nccl --result-json /path/to/result.json
```

A `summary.json` holds fixed reference metrics, for example NCCL:

```json
{
  "test_plan": "all-reduce-8gpu",
  "timestamp": 1700000000,
  "node": "slc01-cl02-hgx-0001",
  "busbw": 500.0,
  "latency": 25.5
}
```

The `load_baseline_summary` / `classify_result_vs_baseline` API in
`cval.baselines.ingest` remains available for these fixed references.

---

## Integration with Hermes

The Hermes `c-val-hpc-engineer` skill can build baselines, classify nodes, and
summarize degraded nodes through these commands. See
[c-val-hpc-engineer/SKILL.md](../skills/c-val-hpc-engineer/SKILL.md).

## Limitations and future work

- **GPU SKU / topology strata**: not yet recorded per run; only image/test_plan.
- **Trend analysis**: rolling baselines over time to catch slow degradation.
- **Auto-promotion guardrail**: promote an improved baseline only when stable and
  not drifting too far from the current active one.
- **Live wiring**: auto-classify each completed validation in `cval-live`.
