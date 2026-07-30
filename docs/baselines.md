# Baselines and Node Classification

c-val builds **baselines** from historical validation results and uses them to
**classify nodes** as:

- **normal**: within the baseline's acceptance band
- **degraded**: on the failing side of the band; warrants investigation
- **improved**: better than the good-side tail (informational)

There are three layers:

1. **U8 versioned health classes (implemented locally, not operationally
  activated)** — per-test, environment-combination candidates with stable
  classes 0–5, exact provenance/sample coverage, normalized threshold bands,
  and immutable SQLite evidence.
2. **Compatibility dynamic baselines (current operational system)** — built directly from the result DBs with
   robust statistics, stored as versioned records in SQLite, and used to
  classify nodes through the existing baseline CLI and background scripts.
3. **Directory baselines (legacy)** — hand-authored `summary.json` references
   loaded from disk, for fixed golden references.

The commands later in this document operate the compatibility dynamic-baseline
system. U8 intentionally has no production evaluator/CLI wiring yet; U9 and any
live per-test health DB activation require separate approval.

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

### U8 stratification

Baselines are computed per stratum so comparisons stay apples-to-apples. The keys
differ by test type because the schemas do:

- **storage**: image, CUDA, and PyTorch versions.
- **NCCL**: image, CUDA, PyTorch, iteration count, and BF16 data size.
- **DL**: image, CUDA, PyTorch, test plan, and iteration count.

GPU SKU / topology are not yet recorded per run and are left for future work.

### Sample size

U8 requires at least `min_samples` distinct qualifying results (default 8 in
built-ins), complete expanded-metric result membership, and the same exact
non-empty sample-key set in every result. A pooled metric cannot become nominal
after a rank/sample disappears. A new candidate also requires
`min_new_results` IDs absent from the preceding immutable candidate snapshot.

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

DL metric DBs are rebuilt from remapped rank JSON artifacts before DL baseline
build/classification:

```text
/data/continuous_validation/validation_tests/dltest/runs/<node>/<run-id>/artifacts/workdir/test_plans/<plan>/runs/*.json
```

The scanner also accepts historical
`/data/continuous_validation/dltest/<node>/dltest-*/workdir/...` layouts when
that legacy root is supplied explicitly.

The maintenance command is:

```bash
python -m cval.cli db-rebuild-dltest-metrics \
  --results-root /data/continuous_validation/validation_tests/dltest/runs \
  --output-dir /data/continuous_validation/metadata
```

### DL verdict aggregation

DL has thousands of metrics per node, so c-val does **not** mark a node degraded
because one metric barely crosses a robust band. Each metric is still classified
against its median/MAD band, but the node/component verdict uses two additional
aggregation variables:

- `n_degraded` / `degraded_metric_fraction`: how many metrics are meaningfully bad.
- `worst_pct_diff`: how far the worst out-of-band metric is from the baseline.

For DL only, a metric counts toward the node's degraded verdict when it is both
outside the band and at least `dl_degraded_severity_pct` percent away from the
baseline median. A DL component becomes `degraded` when those severe misses reach
either `dl_min_degraded_metrics` or `dl_degraded_metric_fraction`.

The four DL components can be classified as separate tests:

```text
dltest-numerical
dltest-compute
dltest-collective
dltest-overlap
```

`dltest` remains the aggregate view and includes per-component summaries in JSON
output.

---

## U8 versioned health records

U8 stores one database per test at
`validation_tests/<test-id>/<test-id>_health_classes.db`. Candidate IDs are
SHA-256 content identities over canonical combination factors, descriptor and
health-policy versions, adapter-schema version, robust-z policy, exact source
and durable-receipt provenance, exact observations/sample coverage, statistics,
thresholds, and lifecycle parent. Adapter observation order and wall-clock
storage time do not alter identity.

Metric center/spread reuse the established median/MAD kernel. With
$\sigma_{MAD}=1.4826\,MAD$, the degradation width is:

$$
\Delta = \max\left(z\sigma_{MAD},\;\frac{t}{100}|m|\right)
$$

where $m$ is the median, $z$ is the effective robust-z policy, and $t$ is the
configured tolerance. Classes 2/3/4 occupy the directional $1\Delta$,
$2\Delta$, and beyond-$3\Delta$ degradation bands; class 0 is the good-side
tail where applicable, class 1 is nominal, and class 5 is DNR with no threshold.
For zero-center metrics, severity falls back to delta-relative distance rather
than falsely reporting zero percent.

Declarative aggregation is `max_metric_class.v1`. DL uses the validated
`dl_severity_count_fraction.v1` final aggregation while preserving every
framework-generated metric verdict. Candidate construction is always
framework-owned; custom adapters cannot replace statistics, DNR, or provenance.

The lifecycle remains:

```text
candidate  ->  active  ->  superseded
```

Building never activates. Explicit activation atomically supersedes only the
current parent in the same `(test_id, combination_key)`. Exact SQL triggers
protect all correctness evidence and legal lifecycle transitions; the mutable
build-state row is advisory only. See
[U8 Health Engine Design Report](u8-health-engine-design-report.md).

## Compatibility dynamic baseline records

Baselines are immutable, versioned records in SQLite DBs under
`/data/continuous_validation/baselines`, with a lifecycle status:

```text
candidate  ->  active  ->  superseded
```

`build --store` writes a `candidate`. `activate` promotes it to `active` and
supersedes the previous active baseline **for the same `(test_type, stratum)`**,
so different strata keep independent active baselines. New results are always
classified against the single `active` baseline (or an explicit `--baseline-id`).
Candidates are the default so a slowly degrading fleet cannot silently
re-baseline itself.

Default baseline DBs:

```text
/data/continuous_validation/baselines/
  test-storage-baselines.db
  test-nccl-baselines.db
  dltest_numerical_correctness-baselines.db
  dltest_compute_performance-baselines.db
  dltest_collective_performance-baselines.db
  dltest_overlap_performance-baselines.db
  classification-results.db
  logs/
    build/
    classify/
```

Storage and NCCL have one baseline DB each. DL has four baseline DBs mirroring
the four DL metric DBs. `classification-results.db` stores derived baseline
decisions (`normal`, `improved`, `degraded`), a boolean `passed` column, and
graded score columns such as `n_compared`, `n_degraded`,
`degraded_metric_fraction`, and `worst_pct_diff`. Raw validation
`pass/fail/incomplete` rows stay untouched in `validation.db`.

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

# Persist derived pass/degraded decisions into classification-results.db
python -m cval.cli baseline classify --test-type dltest --store-results --output json

# DL components can be classified separately
python -m cval.cli baseline classify --test-type dltest-compute --store-results
python -m cval.cli baseline classify --test-type dltest-numerical --store-results
```

Table output:

```text
Classification vs baseline storage-2026Q2 (storage)
NODE                             STATUS         BAD BAND_BAD     BAD%   WORST% COMPARED
slc01-cl02-hgx-0001              normal           0        0    0.00%    0.00%       12
slc01-cl02-hgx-0009              degraded        12       12  100.00%   35.00%       12
Degraded nodes: slc01-cl02-hgx-0009
```

A node's value for each metric is the **median of its recent runs** in the window,
so a single noisy run does not flip the verdict. Storage and NCCL still degrade
when any metric crosses the failing side of its band. DL adds the aggregation
rules above so tiny numbers of weak misses remain visible in `BAND_BAD` and
`worst_pct_diff` without flipping the node verdict.

Classification exports:

```bash
python -m cval.cli classifications --test all --type csv
python -m cval.cli classifications --test dltest-compute --type csv
python -m cval.cli results --test dltest-compute --type csv
```

`results` keeps the raw `result` column from `validation.db` and adds
`classification_status`, `classification_passed`, `n_degraded`,
`degraded_metric_fraction`, and `worst_pct_diff` from `classification-results.db`.

### Background scripts

Two tmux-managed loops are provided for the PVC access pod (or any environment
where `/data/continuous_validation` is visible):

```bash
scripts/cval-baseline-build.sh start       # daily baseline build + activate
scripts/cval-baseline-build.sh status
scripts/cval-baseline-build.sh attach

scripts/cval-baseline-classify.sh start    # classify every configured interval
scripts/cval-baseline-classify.sh status
scripts/cval-baseline-classify.sh attach
```

For a one-shot run without starting tmux:

```bash
scripts/cval-baseline-build.sh run-once
scripts/cval-baseline-classify.sh run-once
```

The builder cadence defaults to daily (`build_interval_seconds = 86400`). The
classifier cadence defaults to five minutes (`classify_interval_seconds = 300`).

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

Shared lifecycle and robust-statistics defaults remain global:

```toml
[baseline]
robust_z_threshold = 3.5            # modified z-score cutoff
min_samples = 8                     # minimum clean samples per metric
window_days = 30                    # rolling window for building baselines
```

Test-specific tolerances, directions, combination factors, and rebuild controls
live under `[health]` and `[[health.metrics]]` in each test's
`test_config.toml`. DL verdict aggregation lives under
`[settings.health_aggregation]` in
`validation-tests/dltest/test_config.toml`. The composed config exposes
compatibility values to the current baseline modules, so classification behavior
is unchanged while ownership moves to each test.

Every enabled U8 health descriptor also declares a versioned
`health.policy_version`, matched by the repository adapter. It must change when
observation or aggregation semantics become incompatible.

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
- **U9 evaluator/live wiring**: build and classify completed canonical results
  without weakening default-off write gates or U8 activation controls.
