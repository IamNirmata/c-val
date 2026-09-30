# Configuration

The operator configuration is [config/cval.toml](../config/cval.toml). Select a
reviewed alternative with `cval --config <path> <command>` or `CVAL_CONFIG`.
Public commands use that configuration for namespace, PVC reader, database paths,
job template/repository/prefix, namespace allowlist, node filter, scheduling and monitoring;
duplicate command-line overrides for those settings have been removed.

## Retired Setting Flags

C1 removes flags that previously had to equal the reviewed TOML values. Remove
them from scripts; use a complete `--config` file to select different settings.

| Removed CLI Flag | Configuration Owner |
| --- | --- |
| `nodes/plan/run --node-filter` | `cluster.node_filter` |
| `plan/run --threshold-days` | `scheduling.days_threshold` |
| `run --batch-size` | `scheduling.batch_size` |
| `jobs/validate --timeout-seconds` | `monitoring.timeout_seconds` |
| `jobs --poll-interval-seconds` | `monitoring.poll_interval_seconds` |
| `validate --poll-interval` | `monitoring.validation_poll_seconds` |
| `validate --pending-timeout` | `monitoring.validation_pending_seconds` |

`plan --batch-size` remains a read-only inspection limit, not a submission override.
Node/run identity, exact Git ref, export selection/output and confirmation inputs
remain explicit. Existing pinned snapshots retain the same encoding and digest;
editing TOML never changes an already running session.

## Strict Two-Level Contract

The main TOML owns framework/cluster settings and explicit test registrations.
Each registered test's TOML owns its requirements, execution settings and artifacts.
Missing files, sections or required keys, unknown keys and invalid types are errors;
Python dataclasses and shell launchers no longer supply setting defaults. Start an
alternate configuration from the complete checked-in TOML, not a partial override.

An absent optional plugin means no plugin, not a default adapter. A declared plugin
must explicitly list `support_files` (use `[]` when empty). Blank source ConfigMap/hash
values explicitly select Git transport; blank NCCL device-range endpoints explicitly
select all detected ports. Omission is not equivalent to either choice.

The main-file location, schema/format names, canonical derived paths, safety limits
and arithmetic remain program rules. Assigned node, timestamp and observed software
versions are runtime evidence, not configurable defaults. Exact source/run identity
and independent confirmations remain explicit command inputs.

## Sections

- `[cluster]`: namespace, PVC reader, node filter, tolerated taints and kubectl timeout;
- `[scheduling]`: stale threshold, concurrent batch size, submission cooldown and coverage scan bound;
- `[job]`: template, prefix, recorded image name, source repository/ref and explicit source transport;
- `[policy]`: namespace allowlist, maximum batch size, confirmation phrase;
- `[monitoring]`: loop/targeted polling, pending/overall deadlines, ingestion grace,
  log-tail size and explicit verification byte/file limits;
- `[live]`: session name, absolute private state and public status directories,
  status refresh interval, runner worktree, sleep,
  plan limit, remote branch, designated `control_host` and credential-free HTTPS `cluster_server`;
- `[maintenance]`: separately invoked backup/inventory/journal bounds and staging directory;
- `[sqlite_retry]`: SQLite lock retry policy used by raw ingestion;
- `[storage]`: authoritative raw SQLite paths;
- `[runtime]`: repository/evidence paths and required runner/ingestion child deadlines;
- `[tests.<id>]`: `enabled` and descriptor `config_path` only;
- `[job_template]`: validation pod resources, image and explicit single-attempt lifecycle settings.

These sections support validation and raw evidence only. Evaluation settings
have been removed; this repository does not classify, score or rank nodes.

## Scheduling

The checked-in values are `batch_size = 3`, `node_cooldown_seconds = 14400` and
`live.plan_limit = "all"`. These are TOML values, never fallback values. Set a positive
integer string for a bounded `live.plan_limit`. Commands read operational settings
directly from TOML; the read-only plan's candidate count remains an inspection limit.

`scheduling.coverage_max_runs_per_node` is explicitly 100 in the main TOML.
Coverage scans newest runs first and stops at the first complete passing requested-test
run. Exhausting the bound without a decision is unavailable history, never an
empty node. Artifact byte/file limits and the configured Kubernetes deadline also
bound this read. Old snapshots lacking the new field remain with their pinned
source and need approved turnover; no missing-setting defaults are supplied.

Coverage uses `successful_complete_run`, with a labeled legacy passing-status
fallback. Failed/incomplete attempts never refresh successful coverage; an older
qualifying pass remains usable. Dependency comparisons are not silently activated,
and historical identities are not backfilled. Ordinary `status` remains a
latest-outcome view, not a scheduling coverage calculation.

The shared admission manager enforces `node_cooldown_seconds` for manual and loop
submitters using the existing table and confirmed creation times retained in intents.
It does not derive cooldown from failed preflight or an uncertain create attempt.

`job.git_ref` is an all-zero placeholder. Every real submission supplies an
exact published lowercase 40-hex commit.

## Single-Attempt Validation

Both pinned and scheduler-selected validation templates use these required
`[job_template]` settings. They are included in the immutable configuration snapshot:

```toml
pod_restart_policy = "Never"
job_max_retry = 1
task_max_retry = 1
failure_action = "TerminateJob"
failure_events = ["PodFailed", "PodEvicted", "TaskFailed", "Unknown"]
```

`Unknown` is the serialized Volcano event; the upstream constant is named
[`JobUnknownEvent`](https://pkg.go.dev/volcano.sh/apis@v1.15.2/pkg/apis/bus/v1alpha1#JobUnknownEvent).
`JobUnknown` is not a valid policy value. The 2026-09-25 attempt at `2992f19`
was rejected before Job creation for that spelling. New source/config snapshots
use `Unknown`; old snapshots and the rejected intent stay with their original
source and evidence. Do not replay that run or silently rewrite its snapshot.

`maxRetry = 0` is not a no-retry option in the inspected Volcano admission code:
it is replaced with the default `3`. The value `1` avoids that defaulting. The
explicit `TerminateJob` policy is the primary failure control, not a claim that
`maxRetry = 1` alone disables every restart. Job-level policy is inherited by the
single task; task policy overrides are rejected because Volcano gives them priority.

The renderer validates the decoded output, requiring one replica, `Never`, exact
Job/task limits and the configured termination events. It rejects additional task
policies, per-container restart overrides and `ttlSecondsAfterFinished` cleanup.
Direct callers cannot bypass the config constraints with alternate template settings.
These settings are intentionally constrained to the single-attempt contract, not
general-purpose retry tuning. Existing templates or snapshots lacking them must
be explicitly migrated; no operational defaults are supplied by Python.

Upstream `TerminateJob` moves a Job through Terminating to Terminated and retains
already Failed/Succeeded pods. It may terminate remaining live pods of that Job;
it cannot preserve a pod already evicted/deleted externally. Neither c-val nor this
policy recreates the failed attempt. A later scheduled test is a new approved run
after the normal checks. This does not change SQLite lock retries, test-failure
process exit semantics or the separately implemented successful-coverage policy.

Source references: Volcano v1.15.2
[defaulting](https://github.com/volcano-sh/volcano/blob/v1.15.2/pkg/webhooks/admission/jobs/mutate/mutate_job.go),
[policy dispatch](https://github.com/volcano-sh/volcano/blob/v1.15.2/pkg/controllers/job/job_controller_util.go),
and [termination](https://github.com/volcano-sh/volcano/blob/v1.15.2/pkg/controllers/job/state/terminating.go).
The deployed controller version and CRD could not be read with the current RBAC.
Rendered-source checks are not deployed-policy acceptance; verify admission and
actual single-attempt behavior on the installed version before activation.

## Frozen Sessions and Runs

At admission, the effective main config and full descriptors are encoded into the
existing immutable snapshot, now including retry/live/maintenance settings. The
rendered Job binds its actual source ref to that snapshot. Discovery, clients,
policy, monitoring, readers and ingestion use the resolved settings.

New live sessions retain `config_snapshot_b64` in their existing state record.
All cycles and child commands use that snapshot rather than rereading the operator
TOML. Descriptor/source disagreement fails closed. A changed TOML takes effect only
in a separately approved new session. Legacy sessions without a snapshot remain
readable for status; this revision refuses to resume them silently.

### Shared Admission Scope

`live.log_dir` must be an absolute, non-symlinked control-host directory shared
by all worktrees and supported submitters. It contains the existing `current-session`
pointer and session records, plus `.cval-admission.lock`, separate from the loop's
long-lived `.cval-live.lock`. Do not delete or replace a lock while writers exist.
The checked-in path is now `.cval-state/cval-live` under the source repository;
changing this value does not move or activate an existing session.
Changing a prefix, namespace or evidence root is not permission to run independent
submitters over overlapping nodes.

`live.control_host` must match the submitting host's hostname. `live.cluster_server`
must match kubectl's selected API endpoint; no credentials are stored. These fields
are required in complete TOMLs and new snapshots. Old snapshots lacking them must
use their pinned source for read-only inspection; they cannot initialize or resume
new-source writers.

New sessions use `cval.live-state.v2`, retaining `admission_scope` and durable
submission intents in the existing state file. Historical v1 remains readable but
cannot admit new-source writes. The
shared manager requires that scope, exact source and the complete pinned config
snapshot to match each request. `run`, targeted `validate` and live-loop creates
share the same short lock; session writers use it too. Missing/corrupt records,
legacy scope, configuration disagreement or lock contention block admission.
Lock acquisition is nonblocking; no timeout setting is invented.

The existing state helper's `init` refuses to replace `current-session`. Turnover
requires separately approved stopping/reconciliation and retention of the previous
pointer, followed by approved initialization with the reviewed snapshot. No state
is migrated automatically. The old controller is not fenced by new local code;
all operational writers must be turned over together. Activation is deferred until
Phase 1 is finished. V2 records do not provide cross-host fencing or automatic
recreation after an uncertain create.

Resume first uses the existing state helper's `update --reconcile` operation under
the same admission lock. It reconstructs the jobs projection from authoritative v2
state; missing CLI receipt files or a failed CSV write do not erase reservations.
Missing/corrupt authoritative state still blocks. The reconciliation pass shares
one `monitoring.ingestion_grace_seconds` deadline across retained intents.

### Node and Cluster Status CSVs

`live.status_dir` is a separate absolute directory, checked in as the repository's
`run-logs` directory. New status output consists of `node-status.csv` and
`cluster-status.csv`, readable in Excel without an Excel dependency. Both status
and private state directories are Git-ignored. Historical directories are not
automatically deleted or moved by a source or configuration update.

`live.status_refresh_seconds = 300` controls the passive reader owned by the live
loop. Startup checks existing CSVs against the authoritative submission journal
and refreshes before submission. The reader then refreshes at this interval while
the loop watches Jobs, scans candidates or waits between cycles. It stops with
the controller. Intent/observation writes also republish from the latest collected
snapshot, including prepared intent before Job creation. Refresh scheduling uses
the collection start, while cached evidence expires one interval after collection
finishes, so a slow refresh does not immediately invalidate the preceding result.
The node observation timestamp remains the actual collection start. Historical
caches without a completion timestamp expire from their observation time.
Unavailable and forbidden reads are not empty results; refresh output reports
coverage state and reason separately from inventory state.

Read-only evidence responses use compressed, checksummed transport with a bounded
decoded size. Truncated or corrupt responses remain unavailable. Status collection
and publication wait at most five seconds for the admission lock; a still-busy
watcher retries after at most five seconds instead of skipping a full interval.
Job submitters retain fail-fast locking. No collection runs while holding the lock.

The node CSV includes node name/UID, latest complete recorded run epoch and
Los Angeles time, coverage and priority, current/latest Job name/UID/run epoch/phase,
recording state, test outcome, cooldown deadline, separate Ready and scheduling
states, GPU/CPU/memory/RDMA availability, blocked reason, observation timestamp,
source SHA and configuration digest. Node creation time prevents inherited coverage
from an earlier node incarnation. Missing coverage precedes the oldest expired
coverage using the existing scheduler; fresh and unverified nodes have no rank.

Coverage still means successful requested-test coverage. Recording is independent:
`no_results`, `results_without_db_update`, `partial_db_update`, `fully_recorded`,
or `unverified`. A fully recorded failure remains `fail`. A complete record requires
a canonical terminal envelope, matching exact status rows and its digest-bound
ingestion receipt. Legacy status-only evidence is not counted as fully recorded.
These operational summaries do not replace the separate exact raw-row audit.

The single cluster row counts unique nodes recorded in the last 24 hours, coverage
states, known HGX nodes, Ready/NotReady/readiness-unknown nodes and scheduling-disabled
nodes. Ready and scheduling-disabled counts may overlap. Generation timestamp,
inventory state and observation completeness accompany the counts. This is cluster
status, not health classification.

Writers use the shared admission lock and atomic replacement. Both CSVs contain
the same `snapshot_id`; readers must reject a mismatched pair. Neither CSV can
authorize admission or replace durable intents, receipts, manifests, canonical
logs or raw SQLite. An unresolved CSV attempt without its journal blocks startup.
Historical snapshots lacking both new status settings retain their exact transport
and digest with status refresh disabled; enabling it requires approved turnover.

New initialization also requires `--approval-record <path>` on the existing state
helper. The live launcher passes the explicit locator `CVAL_APPROVAL_RECORD`.
The approval and rollback text is retained with its digest; each intent binds that
digest. This is an operational input, not a setting fallback or an authorization
granted by the presence of a file. Existing mutation approval gates still apply.

Requested manifests and create responses are retained in v2 intents. The existing
jobs CSV is a compatible projection, not authoritative proof of an outcome.
Successful Job UID/creation time observations are preserved; unknown times remain
unknown. A single NotFound never permits reuse, and attempted intents reserve
their node/slot until a bound terminal evidence decision permits release. Do not point a
v1-only writer at v2 state or rewrite v2 into v1 for rollback.

Keep `CVAL_CONFIG`, `CVAL_GIT_REF`, source/session locators and approval variables.
Move former `CVAL_BATCH_SIZE`, `CVAL_PLAN_LIMIT`, `CVAL_DAYS_THRESHOLD`,
`CVAL_TMUX_SESSION`, `CVAL_LIVE_LOG_DIR`, timeout/cooldown/namespace/prefix and
worktree overrides into their main TOML fields. The live launcher rejects those
setting overrides. In-pod environment settings are snapshot transport and must
match; they are not a second configuration layer.

### Verification Limits

`monitoring.verification_max_bytes` bounds total canonical/artifact bytes read by
one audit; the checked-in value is 67,108,864. `verification_max_files` bounds unique
files at 5,000. These required TOML settings have no code fallback. Reaching a limit
is an unavailable observation, not proof that evidence is absent. Old snapshots
lacking these settings remain with their pinned source; do not silently enrich them.

Generation retries use `sqlite_retry.attempts` and `sqlite_retry.delay_seconds`.
Each read uses the lesser of its configured SQLite/Kubernetes bound and the remaining
verification deadline. Targeted validation shares its overall deadline and caps the
verification portion by `monitoring.ingestion_grace_seconds`; terminal reconciliation
uses that configured grace budget. No retry resets the deadline.

### In-Pod Child Deadlines

`runtime.runner_timeout_seconds` and `runtime.ingestion_timeout_seconds` are
required positive integers. The checked-in values are 6000 and 300 seconds.
The supervisor terminates its owned child process group when the corresponding
child wait expires, then drains available logs. Termination/drain cleanup has its
own bounded overhead. Inner per-test setup/workload deadlines remain unchanged.

These limits are not the read-only `monitoring.*` watcher deadlines. Adjust them
explicitly for longer selected workloads or ingestion; stopping a host watcher
still does not cancel a Job. Old snapshots lack these required fields and must use
their original source; do not silently enrich or resume them with new code.
Ingestion interruption can leave committed stores intact and others incomplete;
no automatic rollback, workload replay or evidence deletion occurs.

Images, CUDA, PyTorch and iteration settings remain variable test inputs. Their
existing observed/configured values continue into run evidence and raw records.
No fixed environment version or new image is required by the runtime cleanup.

## Storage paths

`[storage]` declares `validation_db_path`, `storage_db_path`, `nccl_db_path`,
and the four `dl_*_db_path` values. The raw DL ingestion lock is
`<runtime.validation_root>/metadata/.dl-metric-ingest.lock`.

## Test descriptors

A `cval.test.v1` descriptor requires `[test]`, `[requirements]`,
`[settings]`, `[artifacts]` and may declare `[plugin]`. Supported raw plugin
capabilities are `config`, `runtime`, `export`, `log` and `ingest`; export is
read-only. The `runtime` hook projects test settings and reserved evidence paths;
it cannot replace another owner's runtime variables. The `log`
hook delegates consolidated test output and optional line formatting to that test's `logger.py`, using the
reserved destination and shared safe writer. Test-owned `summary.py` writes
`summary.json` alongside the test's `job.log` and artifacts under
`<runtime.validation_root>/logs/<test>/<node>/<run-id>/`.

The `ingest` hook declares `raw_stores(config)` and implements `ingest(context)`.
The context contains the immutable result authorization, read-only environment
and the shared persistent metric lock. Each test's `ingest.py` owns artifact
parsing, SQL mapping and store receipts; every raw writer still independently
validates provenance. The coordinator rejects undeclared/duplicate stores and
invalid receipts, then commits built-in status last. This is not a generic DB
generator or schema migration: the same seven raw stores remain authoritative.
List each imported test support module in `plugin.support_files`; the read-only
exact-run collector packages those sources in memory without writing to the PVC.

New default `runtime.dl_results_root_path` is
`/data/continuous_validation/logs/dltest`. This changes new output, not historical
PVC contents. Old pinned sessions must retain their original descriptors/source;
do not resume them against these changed snapshots without reviewed turnover.

Storage FIO parameters now live in `[settings.fio]` and `[settings.workloads.*]`;
the six independent `.fio` config files are replaced, with the same measured fields
and workload order. NCCL explicitly configures sampling interval, sysfs root and
device range. Each test's runtime hook projects these values into existing child environment
variables; scripts reject missing inputs instead of inventing benchmark settings.

Old partial-config/default-based test helpers and operator TOMLs need migration to
this strict contract. No synthetic runtime suite is claimed as acceptance; real-node
validation and live turnover remain independently approval-gated.

Inspect effective inputs with:

```text
cval config
cval tests list
cval tests describe <id>
cval tests validate
```