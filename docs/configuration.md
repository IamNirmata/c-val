# Configuration

The operator configuration is [config/cval.toml](../config/cval.toml). Select a
reviewed alternative with `cval --config <path> <command>` or `CVAL_CONFIG`.
Public commands use that configuration for namespace, PVC reader, database paths,
job template/repository/prefix, namespace allowlist and maximum batch size;
duplicate command-line overrides for those settings have been removed.

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
- `[scheduling]`: stale threshold, concurrent batch size, submission cooldown;
- `[job]`: template, prefix, recorded image name, source repository/ref and explicit source transport;
- `[policy]`: namespace allowlist, maximum batch size, confirmation phrase;
- `[monitoring]`: loop/targeted polling, pending/overall deadlines, ingestion grace and log-tail size;
- `[live]`: session name, absolute shared log directory, runner worktree, sleep,
  plan limit, remote branch, designated `control_host` and credential-free HTTPS `cluster_server`;
- `[maintenance]`: separately invoked backup/inventory/journal bounds and staging directory;
- `[sqlite_retry]`: SQLite lock retry policy used by raw ingestion;
- `[storage]`: authoritative raw SQLite paths;
- `[runtime]`: repository and retained evidence paths;
- `[tests.<id>]`: `enabled` and descriptor `config_path` only;
- `[job_template]`: validation pod resources and image.

These sections support validation and raw evidence only. Evaluation settings
have been removed; this repository does not classify, score or rank nodes.

## Scheduling

The checked-in values are `batch_size = 3`, `node_cooldown_seconds = 14400` and
`live.plan_limit = "all"`. These are TOML values, never fallback values. Set a positive
integer string for a bounded `live.plan_limit`. Operational CLI tuning flags must
agree with TOML; the read-only plan's candidate count remains an inspection limit.

`job.git_ref` is an all-zero placeholder. Every real submission supplies an
exact published lowercase 40-hex commit.

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

New initialization also requires `--approval-record <path>` on the existing state
helper. The live launcher passes the explicit locator `CVAL_APPROVAL_RECORD`.
The approval and rollback text is retained with its digest; each intent binds that
digest. This is an operational input, not a setting fallback or an authorization
granted by the presence of a file. Existing mutation approval gates still apply.

Requested manifests and create responses are retained in v2 intents. The existing
jobs CSV is a compatible projection, not authoritative proof of an outcome.
Successful Job UID/creation time observations are preserved; unknown times remain
unknown. A single NotFound never permits reuse, and all attempted intents reserve
their node/slot until the later evidence-release implementation. Do not point a
v1-only writer at v2 state or rewrite v2 into v1 for rollback.

Keep `CVAL_CONFIG`, `CVAL_GIT_REF`, source/session locators and approval variables.
Move former `CVAL_BATCH_SIZE`, `CVAL_PLAN_LIMIT`, `CVAL_DAYS_THRESHOLD`,
`CVAL_TMUX_SESSION`, `CVAL_LIVE_LOG_DIR`, timeout/cooldown/namespace/prefix and
worktree overrides into their main TOML fields. The live launcher rejects those
setting overrides. In-pod environment settings are snapshot transport and must
match; they are not a second configuration layer.

## Storage paths

`[storage]` declares `validation_db_path`, `storage_db_path`, `nccl_db_path`,
and the four `dl_*_db_path` values. The raw DL ingestion lock is
`<runtime.validation_root>/metadata/.dl-metric-ingest.lock`.

## Test descriptors

A `cval.test.v1` descriptor requires `[test]`, `[requirements]`,
`[settings]`, `[artifacts]` and may declare `[plugin]`. Supported raw plugin
capabilities are `config` and `export`; export is read-only.

Storage FIO parameters now live in `[settings.fio]` and `[settings.workloads.*]`;
the six independent `.fio` config files are replaced, with the same measured fields
and workload order. NCCL explicitly configures sampling interval, sysfs root and
device range. The runtime projects these values into existing child environment
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