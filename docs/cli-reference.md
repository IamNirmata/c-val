# CLI Reference

Run commands from the c-val repository root.

Use `--config /path/to/cval.toml` before the subcommand to load a non-default
configuration file.

## Public Commands

The public operator/Hermes CLI is intentionally small:

```text
cval config
cval tests list|describe|validate
cval status
cval history
cval nodes
cval overview
cval validate
cval run
cval jobs
cval result
cval results
cval classifications
cval health evaluate
cval health activate
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

The effective JSON includes composed per-test descriptors and settings.

## `tests`

Inspect and validate the explicit repository-local validation test registry.
These commands are read-only and never execute workloads. `list` and
`describe` do not import adapters; `validate` imports every declared
repository adapter to verify API version, capabilities, required methods, and
adapter-owned config.

```bash
python -m cval.cli tests list
python -m cval.cli tests list --enabled-only --output json
python -m cval.cli tests describe nccl
python -m cval.cli tests describe nccl --output table
python -m cval.cli tests validate --output json
```

Configuration loading validates IDs, schemas, repository-confined paths,
entrypoint/setup files, unique enabled order, and shared resource coverage. An
invalid registry returns exit code `2` with a concise configuration error.

## `status`

Read latest validation status from SQLite through the PVC access pod using
`mode=ro`:

```bash
python -m cval.cli status --output table
python -m cval.cli status --output json
python -m cval.cli status --output tsv
```

## `history`

Read normalized `cval.results.v2` run history from the PVC access pod. This
command opens SQLite in `mode=ro` and never creates a missing database.

```bash
python -m cval.cli history
python -m cval.cli history --node <node> --limit 20
python -m cval.cli history --test nccl --status fail --output json
python -m cval.cli history --run-id <run-id> --output json
```

See [Node Run History](run-history.md) for schema and activation safety.

## `health`

Run the registry-driven U8 evaluator locally or against an explicitly supplied
PVC copy. These commands do not use Kubernetes.

```bash
# Side-effect-free: missing canonical U7 DBs are structured skips.
python -m cval.cli health evaluate --output json

# Derived writes: requires config gate plus exact confirmation.
python -m cval.cli health evaluate --apply --confirm evaluate --output json

# Candidate activation is always separate and dry-run first.
python -m cval.cli health activate <test-id> <hb1:baseline-id> --output json
python -m cval.cli health activate <test-id> <hb1:baseline-id> \
  --apply --confirm activate --output json
```

`health evaluate` selects only enabled tests declaring both `health` and
`ingest`. It validates each existing U7 common+adapter schema in one query-only
in-memory snapshot copied without opening SQLite on the source, invokes adapter
observation APIs against that same snapshot, builds candidates from the full
current-config source catalog without activating them, and classifies an
oldest-pending page bounded by `max_classifications_per_test`. JSON/table output
reports selected/deferred/backlog/remaining/truncation, migration state,
candidate/history inserted and idempotent counts, the failing stage, and
partial durable writes from an earlier cross-DB stage. Routine classification
catalog work uses bounded primary-key result pages plus one indexed exact-target
history lookup per page; it never loads the complete history table. One test
error does not block later tests. Apply may perform only the exact additive U7
v1→v2 migration, U8 candidate persistence, and append-only classification
history. It never updates `test_results.health_*` cache columns.

Deferred passing rows retain their bounded action rows and reasons. Their full
count is included in `classification_remaining`, so zero actionable backlog
does not imply that deferred work has drained.

## Hidden in-pod ingestion hooks

The following commands stay out of public `--help` and are called only by the
validated in-pod `db-update.sh` sequence:

```text
db-preflight-compatibility-result
db-preflight-test-results
db-add-storage-result
db-add-nccl-health
db-add-run-results
db-upsert-run-history
db-ingest-test-results
```

Both preflight commands are read-only and bind the complete result digest,
immutable config snapshot, canonical evidence paths, and all configured DB
targets before the first v2 write. Required compatibility metrics are written
first; `db-add-run-results` then commits all fixed compatibility status rows in
one transaction. The older hidden `db-add-result` hook remains compatibility
code but is not the active `db-update.sh` status path. `db-upsert-run-history` requires
`run_history_enabled=true`; `db-ingest-test-results` requires the independent
`per_test_ingestion_enabled=true` gate. Both are `false` by default. These are
not migration or activation commands and should not be invoked manually on the
live PVC.

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

## `validate`

Validate one specific node on demand, end to end: it prints the node's
schedulability, submits a single validation job immediately (no free-node
search), live-tracks job phase and per-test progress, then classifies the fresh
result against the active baselines on the PVC access pod and prints a pass/fail
+ degraded-metric report.

```bash
python -m cval.cli validate --node slc01-cl02-hgx-0186
python -m cval.cli validate --node slc01-cl02-hgx-0186 --output json
python -m cval.cli validate --node slc01-cl02-hgx-0186 --dry-run
```

Flow:

1. Print the node's status: `ready`, `cordoned`, `unschedulable`, `not_ready`,
   `busy`, `resource_pressure`, `not_gpu`, or `not_found` — plus `ready`,
   `cordoned`, `schedulable`, `resource_ready`, and `gpus_free/allocatable`.
2. Submit the job regardless of node state (the explicit `validate` is the approval).
3. Every `--poll-interval` seconds (default 3), print job phase and which tests
   (`storage`, `nccl`, `dltest`) have finished, parsed from the in-pod logs.
4. When the job is terminal, refresh the node's DL metric DBs (scoped, under the
   shared DL lock) and run `baseline classify` for `storage`, `nccl`, and
   `dltest` on the PVC pod, storing verdicts.
5. Print a report: raw pass/fail and baseline verdict per test, DL component
   breakdown, and the degraded metrics with their percentage deviation from
   baseline.

### Validating cordoned nodes

When a node is suspected unhealthy it is usually **cordoned** (`kubectl cordon`)
so user workloads drain off it. `validate` is built for exactly this: it reports
`status=cordoned [CORDONED]` and still targets the node, because the rendered
validation job tolerates the `node.kubernetes.io/unschedulable:NoSchedule` taint
that cordon adds. No manual template edits are needed — cordoned and uncordoned
nodes use the same job. (Automatic discovery in `run`/`cval-live` still skips
cordoned nodes; only the targeted `validate` runs on them.)

Useful flags:

- `--git-ref <ref>`: pin the runtime checkout the job clones (default config).
- `--poll-interval N`: live status cadence in seconds (default 3).
- `--timeout-seconds N`: overall live-tracking timeout.
- `--pending-timeout N`: warn if the job is still `Pending` after N seconds (it stays queued).
- `--skip-dl-rebuild`: classify DL against existing metric DBs without refreshing.
- `--pvc-pod` / `--pod-repo-dir` / `--pod-config`: override the classification pod and its c-val checkout.
- `--dry-run`: render the job and show node state without submitting.
- `--download`: after the run, save this node's logs, results, and baseline
  comparison as a local zip (`cval-<node>-<timestamp>.zip`); `--download-dir`
  sets the destination (default: current directory).

The downloaded zip mirrors the PVC layout and bundles the baseline comparison:

```text
storage/<node>/storage-<node>-<ts>/   # FIO JSONs, storage log + summary
nccl/<node>/nccl-<node>-<ts>/          # nccl log, ibbw log, nccl-summary.json
dltest/<node>/dltest-<node>-<ts>/      # dltest log + summary + per-rank JSONs
results/<node>/cval-results-<node>-<ts>.{json,env}
report.json                            # structured verdicts (raw + baseline)
report.txt                             # the rendered operator report
```

The job is submitted into the policy-allowlisted namespace; classification runs
in the PVC access pod because that is where `/data/continuous_validation` is
mounted.

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

Inspect `cval.results.v2` or historical v1 JSON. The default env-line form is a
temporary storage/NCCL/DL projection used by `db-update.sh`:

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

JSON output preserves the full dynamic test map and should be used by new
automation.

## `results`

Export the latest per-node result rows for one test into a local CSV. The raw
pass/fail source is the read-only `latest_status` view in `validation.db`.
`overall` maps to the DB's aggregate `all` row. Baseline classification columns
are joined from `classification-results.db` by default. Per-test choices are
derived from enabled plugins declaring `export`; `overall` and `all` remain
fixed aggregate aliases.

```bash
python -m cval.cli results --test overall --type csv
python -m cval.cli results --test dltest --type csv
python -m cval.cli results --test dltest-compute --type csv
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
node,test,db_test,latest_timestamp,latest_time_utc,latest_time_los_angeles,result,
classification_status,classification_passed,classification_baseline_id,
classified_timestamp,classified_time_los_angeles,n_compared,n_degraded,
n_band_degraded,degraded_metric_fraction,degraded_metric_percent,worst_pct_diff
```

Use `--no-classification` to export only the raw pass/fail status columns.

For `storage` and `overall`, per-metric FIO columns (IOPS/bandwidth) are joined
from `test-storage.db`. For `overall`, aggregate NCCL `nccl_busbw`/`nccl_latency`
columns are joined from `test-nccl.db`. Use `--no-metrics` to skip the metric join.

### NCCL / IB health output

`results --test nccl` mirrors the consolidated `IB_HEALTH` table: one wide row
per node's latest test. Each HCA column contains only that port's maximum
observed transmit bandwidth (GB/s).

```text
node,test,latest_timestamp,latest_time_utc,latest_time_los_angeles,result,
la_timestamp,iterations,image_name,cuda,pytorch,samples,BUS_BW,LATENCY,
mlx5_0,mlx5_1,...,mlx5_13,
classification_status,...,worst_pct_diff
```

- `BUS_BW` / `LATENCY` are aggregate 8-GPU all-reduce metrics (GB/s and ms).
- `mlx5_0` ... `mlx5_13` are the per-device maximum bus bandwidths in GB/s.
- `samples` is the number of HCA counter samples; `iterations` is the NCCL
  all-reduce iteration count.
- Missing historical per-port values remain blank; aggregate values are not
  copied into device columns.

## `classifications`

Export the latest baseline classification verdicts directly from
`classification-results.db`:

```bash
python -m cval.cli classifications --test all --type csv
python -m cval.cli classifications --test storage --type csv
python -m cval.cli classifications --test dltest-compute --type csv
```

Available tests are the enabled compatibility-classification targets derived
from `baseline` capabilities. The built-in set remains `storage`, `nccl`,
`dltest`, `dltest-numerical`, `dltest-compute`, `dltest-collective`, and
`dltest-overlap`.

## `baseline`

Build dynamic baselines from result DBs, manage their lifecycle, and classify
nodes. Choices are derived from enabled plugin `baseline` capabilities and are
re-resolved by handlers. See [Baselines and Node Classification](baselines.md)
for the method.

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
python -m cval.cli baseline classify --test-type dltest-compute --store-results
python -m cval.cli baseline classify --test-type dltest-overlap --store-results
```

`--store-results` writes derived decisions to
`/data/continuous_validation/baselines/classification-results.db`.

Background loops:

```bash
scripts/cval-baseline-build.sh start
scripts/cval-baseline-classify.sh start
```

The loops consume a hidden read-only `operational-targets` TSV/JSON interface;
it emits plain data and is not a shell-eval contract. TSV rows use the fixed
seven-field `cval.operational-target.v1` format. The classify environment list
remains an allowlist, DL refresh/lock work is once per group/cycle, and a cycle
reports nonzero only after attempting every selected target. Empty/malformed
catalogs, an empty allowlist intersection, or unavailable/failed `flock` are
nonzero fail-closed outcomes; DL work never runs unlocked.

Built-in sources remain `metadata/test-storage.db`, `metadata/test-nccl.db`,
and `metadata/dltest_*`. U10 performs no U7/U8/U9 source cutover and no live
loop restart.

`db-rebuild-dltest-metrics` is a separate hidden maintenance hook used by the
baseline loops. All hidden hooks are implementation details, not operator
commands; completed migrations and removed aliases stay absent.
