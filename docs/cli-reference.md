# CLI Reference

Run commands from the c-val repository root.

Use `--config /path/to/cval.toml` before the subcommand to load a non-default
configuration file.

## Public Commands

The public operator/Hermes CLI is intentionally small:

```text
cval config
cval status
cval nodes
cval overview
cval run
cval jobs
cval result
cval results
```

The `baseline` command group (build, classify, activate, show, list) is also a
public command surface; see [`baseline`](#baseline).

Older command names remain available for compatibility, but are hidden from
`--help` and should not be used in new docs or automation.

## `config`

Print the effective TOML configuration:

```bash
python -m cval.cli config
python -m cval.cli --config /path/to/cval.toml config
```

## `status`

Read latest validation status from SQLite through the PVC access pod using
`mode=ro`:

```bash
python -m cval.cli status --output table
python -m cval.cli status --output json
python -m cval.cli status --output tsv
```

## `nodes`

Read pods and nodes, calculate GPU usage, and show schedulable free GPU nodes:

```bash
python -m cval.cli nodes --output table
python -m cval.cli nodes --output json
```

## `overview`

Show one read-only operational dashboard: free nodes, validation freshness,
priority queue, and active Volcano job phases.

```bash
python -m cval.cli overview
python -m cval.cli overview --watch --interval 5
python -m cval.cli overview --output json
```

Useful flags:

- `--threshold-days N`: freshness threshold for valid vs outdated results.
- `--queue-limit N`: number of queued nodes to show.
- `--no-jobs`: skip Volcano job listing for a faster view.

## `run`

Plan a validation batch. This is dry-run by default:

```bash
python -m cval.cli run \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <branch-tag-or-commit> \
  --output json
```

Real submission requires explicit confirmation:

```bash
python -m cval.cli run \
  --free-nodes <node> \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <branch-tag-or-commit> \
  --submit \
  --confirm submit \
  --output json
```

Policy gates:

- namespace must be allowlisted
- planned job count must not exceed max batch size
- confirmation must match `submit`

## `jobs`

Read Volcano job phase once:

```bash
python -m cval.cli jobs --jobs <job-name> --output json
```

Watch until terminal or timeout:

```bash
python -m cval.cli jobs \
  --jobs <job-name> \
  --watch \
  --timeout-seconds 1200 \
  --poll-interval-seconds 30 \
  --output json
```

`jobs --watch` is read-only; it does not delete or cancel timed-out jobs.

## `result`

Inspect structured result JSON in env-line form:

```bash
python -m cval.cli result --result-json <result.json>
```

Expected output:

```text
GCRRESULT1=pass
GCRRESULT2=pass
GCRRESULT3=pass
overall_result=pass
```

Or emit JSON:

```bash
python -m cval.cli result --result-json <result.json> --output json
```

## `results`

Export the latest per-node result rows for one test into a local CSV. The source
is the read-only `latest_status` view in `validation.db`. `overall` maps to the
DB's aggregate `all` row.

```bash
python -m cval.cli results --test overall --type csv
python -m cval.cli results --test dltest --type csv
python -m cval.cli results --test storage --type csv
python -m cval.cli results --test nccl --type csv
```

By default, the CSV is written in the current directory with this filename
shape, using America/Los_Angeles local time:

```text
cval_<test_name>_<YYYYMMDD_HHMMSS_TZ>.csv
```

Use `--output-dir <folder>` to write into another local directory. Columns are:

```text
node,test,db_test,latest_timestamp,latest_time_utc,latest_time_los_angeles,result
```

## `baseline`

Build dynamic baselines from result DBs, manage their lifecycle, and classify
nodes. See [Baselines and Node Classification](baselines.md) for the method.

Build a baseline. Dry-run prints the computed robust metrics; add `--store` to
persist as a candidate, and `--activate` to also promote it to active:

```bash
python -m cval.cli baseline build --test-type nccl --window-days 30 --store
python -m cval.cli baseline build --test-type storage \
  --image-name pytorch:26.05-py3 --baseline-id storage-2026Q2 --activate
python -m cval.cli baseline build --test-type dltest --test-plan 80gb-example --output json
```

Without `--db-path`, dynamic baselines are stored under
`/data/continuous_validation/baselines`:

```text
test-storage-baselines.db
test-nccl-baselines.db
dltest_numerical_correctness-baselines.db
dltest_compute_performance-baselines.db
dltest_collective_performance-baselines.db
dltest_overlap_performance-baselines.db
```

Promote, inspect, and list:

```bash
python -m cval.cli baseline activate storage-2026Q2 storage
python -m cval.cli baseline show storage-2026Q2 storage --output json
python -m cval.cli baseline list --test-type storage --output json
```

Classify nodes against the active (or a named) baseline. With no `--node`, every
node seen in the window is classified:

```bash
python -m cval.cli baseline classify --test-type storage
python -m cval.cli baseline classify --test-type nccl \
  --node <node> --baseline-id <id> --output json
python -m cval.cli baseline classify --test-type dltest --store-results --output json
```

`--store-results` writes derived decisions to
`/data/continuous_validation/baselines/classification-results.db`.

Background loops:

```bash
scripts/cval-baseline-build.sh start
scripts/cval-baseline-classify.sh start
```

Legacy directory-based references remain available:

```bash
python -m cval.cli baseline load   <baseline-dir> <test-type>
python -m cval.cli baseline ingest <baseline-dir> <test-type>
python -m cval.cli baseline compare <baseline-id> <test-type> --result-json <result.json>
```

## Internal Compatibility Commands

These names are hidden from `--help` but still parse for existing scripts and
old notes:

```text
discover-free-nodes
plan
submit-plan
job-status
monitor-jobs
result-env
prioritize
render-job
run-batch
db-add-result
db-add-storage-result
db-add-nccl-result
db-rebuild-dltest-metrics
```
