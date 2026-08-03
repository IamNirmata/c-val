# Baselines and classification

`cval.baselines` is the sole canonical storage/DL evaluator. NCCL is the
explicit PostgreSQL exception described in `docs/evals/nccl-eval-process.md`.

## Method

For each metric, c-val builds robust statistics over the configured rolling
window:

- center: median;
- scale: $1.4826 \times \mathrm{MAD}$;
- engineering floor: configured percentage of the center;
- direction: low-bad, high-bad, or two-sided.

A node is classified from the median of its recent observations:

- `normal` — inside the acceptance band;
- `degraded` — on the failing side;
- `improved` — beyond the good-side tail with no degraded metric.

DL components additionally require sufficiently severe misses by count or
fraction. The four components are evaluated separately: numerical correctness,
compute performance, collective performance, and overlap performance.

## Database layout

Baseline databases under `baseline.baseline_root_path`:

- `test-storage-baselines.db`;
- `dltest_numerical_correctness-baselines.db`;
- `dltest_compute_performance-baselines.db`;
- `dltest_collective_performance-baselines.db`;
- `dltest_overlap_performance-baselines.db`.

Classification databases are per target:

- `storage-classifications.db`;
- `dltest-classifications.db`;
- `dltest-numerical-classifications.db`;
- `dltest-compute-classifications.db`;
- `dltest-collective-classifications.db`;
- `dltest-overlap-classifications.db`.

Extensions use `plugin-<target>-baselines.db` and
`plugin-<target>-classifications.db` by default. Readers enumerate enabled
targets and merge latest rows.

Historical SQLite NCCL baseline/classification files may remain as retained
compatibility evidence, but generic NCCL baseline/classification helpers and
operational targets are removed. Raw `cval results --test nccl` export remains
available.

## Lifecycle

Baselines are immutable and move through:

`candidate → active → superseded`

Activation redefines normal and must remain deliberate.

## Commands

```text
cval baseline build --test-type storage --store
cval baseline activate <baseline-id> storage
cval baseline classify --test-type storage --store-results
cval classifications --test all --type csv
cval results --test storage --type csv
```

The background build/classify scripts enumerate enabled targets from the test
registry. A DL group takes one shared metric-refresh lock.

## Global-classification split preparation

The former global DB is not touched automatically. After a separately approved
whole-root backup, preview a split locally or against an explicit copied tree:

```text
scripts/cval-split-classifications.py \
  --source <copy>/baselines/classification-results.db
```

Apply requires both a backup manifest that contains the source file and exact
`--apply --confirm split-classifications`. The command refuses existing target
files and never deletes the source.
