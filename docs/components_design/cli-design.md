# CLI Interface Decisions

## Scope

[cval/cli.py](../../cval/cli.py) is the installed `cval` entry point, serving
three audiences:

1. operators reading cluster/raw state;
2. confirmed job submission workflows;
3. individual raw-writer compatibility and maintenance hooks.

The CLI must preserve exact-commit submission, node eligibility checks,
read-only planning/monitoring, raw result export, immutable write provenance,
and fail-closed behavior.

The planning, CLI and ingestion simplifications were published in `1abde89`
and passed exact-commit cluster acceptance on 2026-09-23. See the
[cleanup review](../cleanup-review.md) for release evidence and remaining scope.
This note owns interface-retention decisions and migration guidance, not live
procedures or an approved redesign roadmap.

## Default Planning

The [runbook's candidate-inspection contract](../operations-runbook.md#candidate-inspection)
defines `plan` output and its boundary with submission. Removing candidate-wide
rendering avoids building Jobs twice without introducing a mode selector or
another scheduler. No measured end-to-end speedup is claimed.

## Mode Inventory

Inventory of the current c-val CLI and live-loop entry point, checked against
their parsers on 2026-09-23 after removing redundant settings. These controls
remain because they support operator inspection, the live loop, or selected-node
validation. Benchmark settings and standalone backup/migration tools are outside
this CLI inventory.

| Command or area | Current modes | Decision guidance |
| --- | --- | --- |
| `plan` | One default: candidate queue | Done; no rendering selector |
| `nodes` | Default discovery; `--inventory-only`; `--check-node NODE` | Keep: the live loop uses inventory and targeted checks for different stages |
| `plan`, `run` node input | Default discovery; explicit `--free-nodes` snapshot | Keep: the controller supplies its selected inputs; an empty explicit list must stay empty |
| `plan`, `run` history | No source (empty history); `--live-status`; `--db-status-json FILE` | Keep live reads and the loop's JSON snapshots; TSV input is removed |
| `tests` | `list`, `describe`, `validate`; `list --enabled-only` | Useful local inspection and validation; removing the subcommands is a separate interface decision |
| `jobs` | One snapshot by default; `--watch` | Keep: status snapshot and bounded monitoring are distinct |
| `validate` | One workflow: submit, watch, verify and report | Keep selected-node acceptance; ZIP download and its flags are removed |
| `results` content | `--test overall/all/storage/nccl/dltest`; `--no-metrics` | Tests choose data; `all`/`overall` is an alias pair. Preserve the ability to inspect status without joining metrics |
| `results` format | CSV, no selector | `--type` removed |
| `config` format | JSON, no selector | `--output` removed for this command |
| Other output formats | `--output table/json` for tests, nodes, status, plan, jobs, run and validate | Keep human-readable and automation output; TSV output is removed |
| Submission gates | `run` and `validate` require `--submit --confirm submit` and an exact `--git-ref` | Required safeguards, not redundant execution modes |
| Live-loop commands | `start`, `stop`, `status`, `attach`, `run-once`, internal `run-loop`, help | Keep lifecycle commands; `start` relies on internal `run-loop` |
| Live-loop mode controls | `CVAL_PLAN_LIMIT=all` or a positive limit; optional separately confirmed pruning | Retain bounded scanning and independent deletion authorization; pruning stays off by default |

Retained runtime controls are `--config`, `--node-filter`, `--threshold-days`,
`--batch-size`, exact revision/run timestamp, watch deadlines and export
destination. The live loop uses its existing config and snapshot inputs;
selected-node validation retains its explicit target and evidence verification.

### Removed Settings and Migration

| Removed public control | Replacement |
| --- | --- |
| `config --output json`, `results --type csv` | Fixed JSON and CSV output respectively |
| `status --output tsv`, `plan/run --db-status-tsv` | JSON output and `--db-status-json`, or `--live-status` |
| `plan --candidates-only`, `--include-yaml` | Candidate inspection is the only default; `candidates` replaces `planned_jobs` |
| `plan --timestamp`, `--template`, `--job-prefix`, `--git-repo` | No rendering during inspection; use `run --timestamp` and reviewed job configuration for submission |
| `validate --download`, `--download-dir` | Retained canonical PVC evidence; no automatic ZIP export |
| `validate --pvc-pod`, `status/results --pod`, `plan/run --status-pod` | `cluster.pvc_access_pod` |
| Public `--namespace`/`-n`, `plan/run --status-namespace` | `cluster.namespace` in the reviewed config |
| Public `--db-path`, `--nccl-db-path`, `--storage-db-path`, `--status-db-path` | The corresponding `[storage]` path |
| `run --template`, `--job-prefix`, `--git-repo` | `job.template_path`, `job.job_prefix`, `job.git_repo` |
| `run --allowed-namespace`, `--max-batch-size` | `policy.namespace_allowlist`, `policy.max_batch_size` |

Choose an alternate config with `--config` before the subcommand. The TOML
sections were not removed: cluster, scheduler, resources, timeouts, retry policy,
raw paths and the independent evaluator still use them. The config supplied to
`run` is passed through to rendering, including the exact source bundle and
resource settings. Oversized batches still fail policy validation before creation.
Hidden ingestion hooks retain explicit evidence/DB arguments and authorization.

### Internal Ingestion and Recovery

| Hook | Behavior selectors | Decision guidance |
| --- | --- | --- |
| `db-add-run-results` | Explicit result values; no inspection mode | Retained compatibility hook; the runtime coordinator calls the writer directly |
| `db-add-storage-result` | `--immutable`, optional `--run-id` | Retained compatibility hook with evidence/identity protection |
| `db-add-nccl-health` | `--immutable`, `--require-hca-samples`, `--recover-descriptor-ibbw-log` with `--confirm-recovery` | Recovery and completeness checks have distinct safety purposes |
| `db-add-dltest-run` | `--output table/json` | Retained individual writer hook; no dry-run/alternate storage mode |
| `db-rebuild-dltest-metrics` | `--only-missing`; `--output table/json` | Explicit maintenance policy, not ordinary live ingestion; do not remove as CLI clutter |

Result paths/digests, run IDs, versions and metric settings on these hooks are
provenance/data parameters. They are not candidates for silent removal.

## Implementation Boundary

Five hidden commands share the same parser: `db-add-run-results`,
`db-add-storage-result`, `db-add-nccl-health`, `db-add-dltest-run`, and
`db-rebuild-dltest-metrics`. They remain compatibility/maintenance interfaces;
normal ingestion no longer calls them. See the
[runtime ownership map](../architecture.md#repository-boundaries) and
[raw evidence contract](../result-schema.md) for the direct coordinator/writer
path. Retiring individual hooks or moving rebuild tooling is a separate
interface decision.

Splitting `cli.py` into more files would improve navigation but not reduce the
codebase. Reduction requires deleting duplicate paths or optional behavior.

## Deferred Decisions

These are unapproved interface proposals, not current behavior or scheduled
cleanup work:

- Merge `validate` into `run` with explicit single-node watch/verify behavior.
  Preserve the existing acceptance contract before retiring any command.
- Reduce config/registry inspection commands or human output formats only
  after deciding which operator interfaces remain supported.
- Retire individual hidden writer hooks only after checking external usage;
  keep rebuild/recovery capabilities explicitly owned.
- Replace plugin-defined CSV export with fixed built-in exports only if
  third-party exporters are no longer a supported contract.

No line-count target justifies removing a supported behavior. See the
[cleanup review](../cleanup-review.md) for the bounded approved work instead.

## Do Not Trim

- Separate read-only `plan` from confirmed mutation.
- Exact 40-hex commit checks and latest-published-tip enforcement.
- Node GPU/CPU/memory/RDMA eligibility checks at submission time.
- `jobs`, `status`, and exact-timestamp acceptance evidence.
- Immutable config/result authorization and canonical evidence validation.
- Registry-driven execution, secure supervisor descriptors, or raw DB writers.