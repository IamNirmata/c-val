# c-val Modular Framework Update Tracker

**Created:** 2026-07-28  
**Status:** Active — U0–U10 accepted locally; U11 local preparation validated after audit, with every live rollout criterion still blocked
**Source draft:** `docs/todo/cval-3.md`  
**Goal:** Make c-val modular, deterministic, easier to extend, and safer to operate without breaking current validation, ingestion, baseline, or classification behavior.

## How this tracker will be used

Work proceeds one item at a time.

1. The operator approves this tracker and the first work item.
2. Only that item is implemented.
3. Local validation results and the relevant diff are reported.
4. The item is not closed and the next item is not started until the operator approves moving forward.
5. Production database writes, Kubernetes changes, deployment, pod replacement, and loop restarts always require their own explicit approval even if the local implementation item was approved.

### Status values

- `PROPOSED` — documented but not approved.
- `APPROVED` — approved for implementation.
- `IN PROGRESS` — implementation is underway.
- `VALIDATED` — local implementation and tests passed; awaiting approval to close.
- `DONE` — accepted by the operator.
- `BLOCKED` — cannot proceed until the listed dependency or decision is resolved.

## Non-negotiable safety rules

- Preserve dry-run-first job submission and the existing confirmation gates.
- Keep SQLite migrations additive. Do not rename or drop production tables, columns, databases, or artifacts in place.
- Preserve historical results and baseline versions.
- New validation tests are disabled by default and cannot execute merely because a directory exists.
- Resolve repository-relative paths against the c-val repository root, not the current working directory.
- Reject paths that escape the repository.
- Keep raw deterministic test status separate from derived health classifications.
- Pin jobs and services to a verified Git commit during deployment.
- Do not modify the live cluster or PVC databases during local development items.

---

# Target Architecture

## 1. Global configuration and explicit test registry

The existing canonical global file remains `config/cval.toml`; it will not be renamed to `main_config.toml` because the current CLI, scripts, documentation, and deployments already use its name.

The global file will own only shared orchestration and infrastructure settings:

- Cluster namespace and node-selection policy.
- Scheduling and submission safety policy.
- Shared job CPU, GPU, memory, RDMA, image, PVC, and `/dev/shm` allocation.
- Repository checkout and Git reference.
- Shared validation root and run-history database path.
- Monitoring and background evaluator cadence.
- An explicit activation/configuration entry for every validation test.

Example target registry:

```toml
[tests.storage]
enabled = true
config_path = "validation-tests/storage/test_config.toml"

[tests.nccl]
enabled = true
config_path = "validation-tests/nccl/test_config.toml"

[tests.dltest]
enabled = true
config_path = "validation-tests/dltest/test_config.toml"
```

Rules:

- TOML booleans are lowercase: `true` and `false`.
- `config_path` is relative to the c-val repository root so the checkout can move safely.
- At least one test must remain enabled.
- A test ID must be unique and must match the ID declared by its test configuration.
- Tests run in deterministic order declared in their test configuration.
- Directory auto-discovery does not activate tests. Registration in the global configuration is required.
- The registry validates that the shared job allocation covers every enabled test's declared requirements.

## 2. Validation test directory contract

Target codebase structure:

```text
validation-tests/
  storage/
    README.md
    test_config.toml
    setup.sh
    run-test.sh
    plugin.py              # optional test-specific Python adapter
    tests/
    fio_jobs/
  nccl/
    README.md
    test_config.toml
    setup.sh
    run-test.sh
    plugin.py
    tests/
    ibbw.sh
    single-node-allreduce.py
  dltest/
    README.md
    test_config.toml
    setup.sh
    run-test.sh
    plugin.py
    tests/
    summarize_results.py
```

### Required files

- `README.md`
  - Purpose and behavior.
  - Dependencies and execution instructions.
  - Result schema and metrics.
  - Health-class strategy and combination factors.
  - Troubleshooting and expected duration.
- `test_config.toml`
  - Stable test ID, display name, schema version, and execution order.
  - Entrypoint and timeout.
  - Dependency/setup policy.
  - Minimum shared resource requirements.
  - Test-specific workload parameters.
  - Artifact/result database paths.
  - Health-class and baseline settings.
- `setup.sh`
  - Idempotent dependency and preflight setup.
  - May be a no-op when the image already provides all dependencies.
- `run-test.sh`
  - Executes only this validation test.
  - Writes artifacts only under the paths assigned by the framework.
  - Exits `0` for pass and non-zero for fail.

### Optional files

- `plugin.py` for test-specific config validation, metric ingestion, health
  observations, optional final health aggregation, or exports. U8 candidate
  generation, DNR, metric verdicts, and persistence remain framework-owned.
- Additional sub-runners, workload programs, and assets.

### Refinement from the initial draft

A separate custom `logger.sh` is not mandatory for every test. The framework must own generic stdout/stderr capture, structured result writing, and run-history persistence so every test behaves consistently. A test may provide a post-processing hook, but it must not bypass the central logger or write directly to unrelated databases.

Similarly, a custom final aggregation hook is required only when the default
versioned most-severe-metric rule is insufficient. U8 does not permit custom
candidate builders/evaluators; simple tests require no duplicated health
boilerplate.

## 3. Test configuration contract

Each `test_config.toml` will use a versioned schema such as `cval.test.v1`.

Proposed sections:

```toml
schema_version = "cval.test.v1"

[test]
id = "nccl"
display_name = "NCCL and HCA health"
order = 20
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 1200

[requirements]
gpu_count = 8
rdma_count = 1

[settings]
iterations = 20
data_size_gb = 8
ibbw_enabled = true
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"

[artifacts]
results_db_path = "validation_tests/nccl/nccl_results.db"
health_classes_db_path = "validation_tests/nccl/nccl_health_classes.db"

[health]
enabled = true
min_samples = 8
min_new_results = 10
combination_factors = ["image_name", "cuda_version", "pytorch_version"]
target_class_count = 5
auto_activate = false
```

Per-test database paths are relative to the globally configured validation root;
the resolved canonical paths remain under
`/data/continuous_validation/validation_tests/<test_id>/` in the default setup.

Test-specific settings will no longer be expanded as individual Kubernetes YAML placeholders. The test config path and a small shared runtime context will be passed to the generic runner.

## 4. Generic validation execution

The framework runner will:

1. Load and validate the explicit test registry.
2. Select enabled tests.
3. Sort tests deterministically.
4. Create the global and per-test artifact directories.
5. Execute setup and the test entrypoint without interpolated shell commands.
6. Apply the test timeout.
7. Capture stdout, stderr, exit code, timestamps, and duration.
8. Atomically update structured run state after every test.
9. Continue to later tests after an individual test failure unless a future explicit `fail_fast` policy says otherwise.
10. Invoke test-specific metric ingestion only when the test produced a valid result.

Structured progress events replace hard-coded log-message parsing:

```text
CVAL_EVENT {"event":"test_started","test":"nccl"}
CVAL_EVENT {"event":"test_finished","test":"nccl","status":"pass"}
```

Existing progress markers remain temporarily during migration.

## 5. Canonical run and log directories

A stable `run_id` will identify one c-val execution on one node. Initial format:

```text
<node>-<epoch_timestamp>
```

### Global job logs

Full stdout/stderr and framework events for the entire job:

```text
/data/continuous_validation/logs/job_logs/<node>/<run_id>/
  job.log
  stdout.log
  stderr.log
  events.jsonl
  result.json
```

### Per-test logs

Only the selected test's stdout/stderr and framework events:

```text
/data/continuous_validation/logs/<test_id>/<node>/<run_id>/
  stdout.log
  stderr.log
  events.jsonl
```

### Per-test result artifacts

Machine-readable summaries and workload-specific files:

```text
/data/continuous_validation/validation_tests/<test_id>/runs/<node>/<run_id>/
  result.json
  summary.json
  artifacts/
```

The logger creates paths and captures output. Tests receive paths and do not construct unrelated global paths themselves.

## 6. Structured result schema

Introduce `cval.results.v2` with arbitrary registered test IDs while retaining a read-only parser for historical `cval.results.v1` files.

Every test result records:

- Test ID and display name.
- Enabled state.
- `pass`, `fail`, or `incomplete` raw status.
- Start/end timestamps and duration.
- Exit code.
- Stdout, stderr, combined log, summary, and artifact paths.
- Test config schema version and config digest.
- Optional failure message.

The aggregate result is computed only from enabled tests.

## 7. Node run-history database

Proposed path:

```text
/data/continuous_validation/metadata/node-run-history.db
```

The initial draft placed this database directly under the validation root. Keeping it under `metadata/` is proposed because it matches the existing c-val data layout and separates databases from artifacts.

### `runs` table — one row per c-val run

Proposed columns:

- `run_id` — primary key.
- `node`.
- `started_timestamp` — UTC epoch.
- `started_timestamp_la`.
- `completed_timestamp`.
- `overall_status`.
- `tests_requested` — stable JSON array for operator readability.
- `image_name`.
- `pytorch_version`.
- `cuda_version`.
- `git_ref`.
- `global_config_digest`.
- `created_at` and `updated_at`.

### `run_tests` table — normalized membership and state

One row per `(run_id, test_id)` records:

- Enabled/selected state and order.
- Raw status.
- Start/end timestamps and duration.
- Exit code.
- Result, log, and summary paths.
- Test config digest.

This table avoids relying on an unqueryable comma-separated list while preserving exactly one primary `runs` row for each c-val execution. A view can provide a comma-separated `tests_ran` display when required.

The existing `validation.db` remains readable during migration. It will not be deleted or destructively converted.

## 8. Per-test results databases

Canonical path:

```text
/data/continuous_validation/validation_tests/<test_id>/<test_id>_results.db
```

Every result database has common run identity columns, while metric tables remain test-owned.

Required common result fields:

- `run_id`, `node`, and timestamps.
- Raw test status.
- Image, CUDA, and PyTorch versions.
- Test config digest and relevant combination factors.
- Summary/artifact path.
- Nullable `health_class_name`.
- Nullable `health_class_numerical`.
- Nullable `health_baseline_id`.
- Nullable `evaluated_at`.

The nullable health columns are a convenient latest-assignment cache. Raw status remains authoritative raw evidence. Classification history must be preserved in an append-only history table or classification database so reevaluation does not erase earlier verdicts.

Existing storage, NCCL, and DL databases are migrated additively. Old databases are retained until compatibility and rollback requirements are explicitly closed.

## 9. Per-test health-class databases

Canonical path:

```text
/data/continuous_validation/validation_tests/<test_id>/<test_id>_health_classes.db
```

Health classes:

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | Excellent | Exceeds the active baseline materially. |
| 1 | Nominal | Within the accepted baseline band. |
| 2 | Underperforming | Slight but actionable degradation. |
| 3 | Very Bad | Significant performance degradation. |
| 4 | Terrible | Extreme outlier or failure-range performance. |
| 5 | DNR | Did not run or has no evaluable metric result. |

`DNR` does not convert a missing test into a measured performance result. The raw status remains `incomplete`; code 5 is an operator-facing health assignment.

### Versioned baseline model

Baselines remain immutable and follow:

```text
candidate -> active -> superseded
```

New qualifying results create a new candidate baseline version rather than mutating threshold rows in place.

Proposed tables:

- `health_baselines`
  - Baseline ID, test ID, lifecycle state, method, combination/stratum key, sample counts, source range, created/updated/activated timestamps, and config digest.
- `health_thresholds`
  - Baseline ID, metric name, units, direction, class code, lower bound, upper bound, median, MAD/IQR, and tolerance.
- `health_build_state`
  - Last evaluated result ID/count for each environment combination.

Thresholds are stored as normalized rows, not dynamic metric columns or Python-style dictionaries inside cells. This keeps schemas stable as metrics are added or removed.

### Combination factors and rebuild triggers

Each test declares factors such as:

- Container image.
- CUDA version.
- PyTorch version.
- GPU SKU/topology when available.
- Test plan, iterations, message size, or other result-affecting settings.

A candidate is generated when `min_new_results` qualifying results have arrived and the total clean sample count satisfies `min_samples`.

Automatic candidate generation is allowed. `cval.test.v1` rejects
`auto_activate=true` because activation redefines normal; a future descriptor
schema would need an explicitly implemented, quality-gated, approved contract.

## 10. Evaluator architecture

The evaluator is CPU-only and PVC-mounted. It builds health-class candidates, activates approved/eligible baselines, evaluates unevaluated results, and writes derived health assignments.

It must not need `kubectl` and must not perform cluster scheduling.

Target responsibilities:

1. Enumerate enabled health-capable tests from the registry.
2. Identify environment combinations with enough new results.
3. Build versioned candidate health classes.
4. Activate according to the configured and approved policy.
5. Evaluate pending test results against the active compatible baseline.
6. Persist append-only classification history. U9 deliberately does not update
  nullable latest-assignment cache columns; a later cutover may propose that
  only with separate compatibility evidence.
7. Emit structured logs and cycle summaries.

The current `gcr-admin-pvc-access` workload is not renamed in place during development. The target `cval-evaluator` workload is introduced and validated separately, followed by an explicitly approved cutover. This avoids breaking live PVC access and current background loops.

For production hardening, a Kubernetes `CronJob` or supervised deployment is preferred over relying only on an unobserved tmux process.

---

# Sequential Implementation Backlog

## U0 — Approve architecture tracker

**Status:** `DONE` — approved 2026-07-28  
**Scope:** This document only.  
**Completion:** Operator accepts the architecture direction, approval workflow, canonical paths, and backlog order.

Approval of U0 authorizes U1 only. It does not authorize code changes beyond U1, cluster actions, database writes, deployment, or service restarts.

## U1 — Freeze contracts and schemas in documentation

**Status:** `DONE` — approved, validated, and accepted 2026-07-28  
**Risk:** Low; documentation and test fixtures only.  
**Depends on:** U0.

Deliverables:

- Final `cval.test.v1` TOML contract.
- Final `cval.results.v2` contract.
- Directory contract and path-resolution rules.
- Run-history and per-test database schema diagrams.
- Plugin adapter Python protocol.
- Compatibility map from current storage/NCCL/DL behavior.
- Synthetic `smoke` test specification used by future contract tests.

Delivered:

- `docs/modular-validation-contract.md`
- `docs/result-schema-v2.md`
- `docs/database-schema-v3.md`
- Target-contract links and current-vs-target warning in `docs/README.md`

Acceptance criteria:

- Every field has type, required/default behavior, and validation rules.
- No unresolved ownership between core and test plugins.
- Historical v1 results and databases have a documented compatibility path.
- No runtime behavior changes.

Validation evidence:

- All Markdown files report no editor errors.
- Local documentation links and fenced code blocks validated.
- All 11 proposed SQL DDL blocks executed successfully against an in-memory SQLite database.
- Shell syntax checks passed for scripts and validation tests.
- `164` unit tests passed.
- Python compilation passed for `cval`, `tests`, and operator scripts.

## U2 — Implement configuration composition and test registry

**Status:** `DONE` — approved, validated, and accepted 2026-07-28  
**Risk:** Medium; configuration loading and job validation.  
**Depends on:** U1.

Deliverables:

- Generic registry and typed test descriptors.
- Repository-relative path validation.
- Per-test config files for storage, NCCL, and DL.
- Main config reduced to activation plus `config_path` for each test.
- `cval tests list`, `cval tests describe`, and `cval tests validate`.
- Compatibility properties or adapters so existing execution still works.

Delivered:

- Typed `cval.test.v1` registry and descriptor loader in
  `cval/validation/registry.py`.
- Strict ID, schema, duplicate, repository-path confinement, entrypoint/setup,
  artifact-path, health, and Kubernetes resource validation.
- Per-test storage, NCCL, and DL configuration files plus staged setup
  entrypoints.
- Global `[tests.<id>]` tables reduced to `enabled` and `config_path`.
- Test-owned workload settings, health tolerances, combination factors, and DL
  aggregation controls with current compatibility dataclasses preserved.
- Read-only `cval tests list|describe|validate` commands and concise config
  error handling.
- Baseline services changed to consume composed effective config instead of
  parsing only the global TOML.
- Configuration, CLI, DL, baseline, cheatsheet, agent, and operator-skill docs
  updated.

Acceptance criteria:

- Existing default configuration produces equivalent effective values.
- Missing, duplicate, mismatched, or escaping configs fail before rendering.
- New tests default to disabled.
- Full unit suite passes.

Validation evidence:

- `cval tests validate --output json`: 3 registered, 3 enabled, valid.
- Missing, duplicate-path, mismatched-ID, path-escape, duplicate-order,
  unknown-global-setting, and resource-under-provisioning tests pass.
- Existing renderer compatibility tests preserve current storage/NCCL/DL
  environment values.
- No-submit dry-run rendering passed with an explicit local node list.
- Shell syntax checks passed for every script under `scripts/` and
  `validation-tests/`.
- `180` unit tests passed.
- Python compilation passed for `cval`, `tests`, and operator scripts.
- Editor diagnostics and `git diff --check` report no errors.

## U3 — Standardize current validation test directories

**Status:** `DONE` — approved, validated, and accepted 2026-07-28  
**Risk:** Medium; in-pod script paths.  
**Depends on:** U2.

Deliverables:

- Bring storage, NCCL, and DL directories into the approved minimum footprint.
- Move phase-specific code out of the top-level monolithic runner where safe.
- Add setup scripts, README contracts, and test-local fixtures.
- Keep temporary compatibility wrappers for existing pod entrypoints.

Delivered:

- Standard `README.md`, `test_config.toml`, `setup.sh`, canonical
  `run-test.sh`, and test-local `tests/` footprint for storage, NCCL, and DL.
- Storage setup owns idempotent fio preparation; canonical runner owns all six
  FIO profiles and summary generation.
- NCCL setup owns dependency preflights; canonical runner owns all-reduce,
  IBBW monitor lifecycle, signal cleanup, and NCCL log assembly.
- DL setup owns package/plan preflights; canonical runner owns isolated workdir,
  torchrun, rank artifacts, and summary generation.
- Existing `storage.sh`, `run-nccl-allreduce.sh`, and `dltest.sh` paths retained
  as thin compatibility wrappers.
- Top-level `run-test.sh` reduced to phase ordering, setup/run delegation,
  stable progress markers, and v1 result persistence.
- Runtime, architecture, validation, skill, and engineering docs updated.

Acceptance criteria:

- Current storage, NCCL, and DL commands produce equivalent artifacts.
- Shell syntax and Python compilation pass.
- Checked-in tests prove old wrapper paths still function.

Validation evidence:

- All validation and background shell scripts pass `bash -n` recursively.
- Contract tests verify every test directory's required footprint and canonical
  descriptor entrypoints.
- Compatibility-wrapper tests verify every old path delegates to the canonical
  runner.
- Isolated tests verify top-level phase delegation and disabled-test behavior.
- Synthetic NCCL execution verifies monitor-disabled torchrun/log behavior.
- Synthetic storage execution preserves six JSON artifacts and summary fields.
- Synthetic DL execution preserves rank JSON and summary contract.
- `186` unit tests passed.
- Python compilation, registry validation, no-submit dry-run rendering, editor
  diagnostics, and `git diff --check` passed.

## U4 — Simplify job rendering and runtime context

**Status:** `DONE` — approved, validated, and accepted 2026-07-28  
**Risk:** Medium; rendered Volcano manifests.  
**Depends on:** U2 and U3.

Deliverables:

- Keep shared hardware/software provisioning in global config.
- Replace test-specific YAML placeholders with a small generic runtime context.
- Pass the global config path, enabled registry, run ID, and validation root.
- Validate shared job resources against enabled test requirements.

Delivered:

- Generic `cval.validation.runtime` context builder with stable effective-config
  digest, ordered enabled tests, registry metadata, and validated environment
  names/values.
- One shell-quoted, deterministic base64 compatibility payload replaces all
  per-test Kubernetes YAML placeholders.
- Manifest-level `CVAL_RUN_ID=<node>-<timestamp>`, shared config/root fields,
  and post-checkout runtime payload decode.
- Pinned job script hardened with `set -euo pipefail`.
- Shared CPU, GPU, memory, RDMA, `/dev/shm`, PVC, sysfs, tolerations, and safety
  behavior preserved.
- Current v1 `RUN_*`/`CVAL_*` values retained inside the compatibility payload
  until the U5 dynamic runner replaces them.
- Runtime, renderer, architecture, configuration, design-decision, contract,
  and operator-skill documentation updated.

Acceptance criteria:

- Dry-run manifests contain no unresolved placeholders.
- No real submissions occur during development.
- Existing safety gates and cordoned-node toleration remain intact.
- Golden manifest tests pass.

Validation evidence:

- Runtime payload round-trips complex shell values and is successfully sourced
  by a real Bash subprocess; malformed names, NUL values, and invalid base64 are
  rejected.
- Effective config digest is stable and changes when composed activation state
  changes.
- Renderer tests verify current compatibility values after decoding and confirm
  all old test-specific manifest environment entries/placeholders are absent.
- Renderer tests verify unchanged shared resource reservations and cordon
  toleration.
- Client-only `kubectl create --dry-run=client --validate=false` parsed the
  rendered Volcano manifest successfully; no cluster resource was created.
- No-submit plan generated no unresolved placeholder tokens.
- Recursive shell syntax checks passed.
- `192` unit tests passed.
- Python compilation, registry validation, editor diagnostics, and
  `git diff --check` passed.

## U5 — Implement generic runner, structured progress, and canonical logs

**Status:** `DONE` — approved, validated, and accepted 2026-07-28  
**Risk:** High; changes in-pod execution.  
**Depends on:** U3 and U4.

Deliverables:

- Generic ordered test execution.
- Per-test timeouts and isolated output paths.
- Central stdout/stderr/event capture.
- Canonical global and per-test log directories.
- Atomic `cval.results.v2` updates after each test.
- v1 read compatibility.
- Generic targeted-validation progress display.

Delivered:

- Registry-driven Python execution with deterministic order, one setup+workload
  deadline per test, process-group timeout/SIGTERM handling, and continuation
  after individual failures.
- Canonical framework-owned global logs, per-test stdout/stderr/events, per-test
  summaries/artifacts/results, exclusive run/per-test evidence reservation, and
  safe path confinement.
- Atomic, strictly validated `cval.results.v2`, dynamic test IDs, per-test state
  and config digests, v1 read compatibility, and fixed env projection only for
  transitional ingestion.
- `cval.event.v1` run/test/ingestion lifecycle events and dynamic targeted
  progress rendering.
- Synthetic fourth-test coverage through render, execution, result, progress,
  and compatibility aggregate ingestion without core test-name changes.
- Canonical storage FIO scratch targeting on the PVC, confined cleanup, NCCL
  summary validation, exact DL rank/task/summary validation, and safe DL
  workdir cleanup.
- Fail-closed identity-checked compatibility ingestion, atomic four-row status
  writes after required metrics, exact-run targeted freshness gates, and no
  stale classification after failed refresh/ingestion.
- Canonical and legacy DL scanners, invalid/deleted-run reconciliation, exact
  configured DB paths, cross-DB generation stamps/checks, and shared targeted
  refresh/classification locking.
- Optional IBBW range restored to genuine auto-detection by default, bounded
  targeted log polling, hard tracking deadlines, and timeout forwarding.
- Runtime/result/path/baseline/operations documentation and repository/agent
  operator guidance updated.

Acceptance criteria:

- A synthetic fourth test runs without changing core runner code.
- Individual test failure is persisted and does not erase later results.
- Interrupted runs leave valid partial state.
- Disabled tests do not execute and do not masquerade as pass/fail.

Validation evidence:

- Three independent read-only release-blocker reviews were completed. The final
  certification found no remaining P0/P1 blocker and declared U5 ready.
- Adversarial tests cover false-pass state combinations, malformed/mismatched
  results, missing NCCL evidence, missing/duplicate DL ranks, malformed tasks,
  invalid/deleted DL runs, cross-DB generations, evidence reuse, unsafe paths,
  TERM-ignoring descendants, SIGTERM interruption, and stale targeted rows.
- End-to-end synthetic tests cover `0-env.sh` paths, the generic shell wrapper,
  v2 result, compatibility ingestion events, and atomic SQLite status rows.
- Recursive shell syntax checks passed for all validation/background scripts.
- `246` unit tests passed.
- Python compilation passed for package, tests, and operator scripts.
- Registry validation reported 3 registered/3 enabled and valid.
- Offline no-submit rendering and local `ruamel.yaml` parsing passed with no
  unresolved placeholders.
- Editor diagnostics and `git diff --check` passed.
- No cluster submission, deployment, or live PVC database write was performed.

## U6 — Add node run-history database

**Status:** `DONE` — approved, validated, and accepted 2026-07-28; live PVC creation/write remains unapproved and default-off  
**Risk:** High; new production write surface.  
**Depends on:** U5.

Deliverables:

- Additive schema and tested writer for `runs` and `run_tests`.
- Read-only CLI/reporting command for run history.
- Idempotent run updates keyed by `run_id`.
- Compatibility reader for current `validation.db` status.

Delivered:

- Additive schema-v1 `runs`, normalized `run_tests`, `schema_migrations`,
  indexes, foreign keys, and ordered `node_run_history` view in
  `cval/storage/run_history.py`.
- Transactional v2 ingestion, exact run/test-set/terminal-state conflict
  rejection, and idempotent `run_id` retries.
- Hidden `db-upsert-run-history` in-pod hook and public read-only
  `cval history` filters/table/JSON output.
- PVC history reader sends Python over stdin and opens SQLite with `mode=ro`;
  missing DBs return no rows and are not created.
- Newer/missing schema metadata fails closed for readers and writers, before
  persistent journal-mode changes.
- `run_history_enabled=false` production write gate in global config and runtime
  context. Deploying the code alone cannot create/write the live history DB.
- Explicit test/status filter semantics: with `--test`, status filters that
  test; without it, status filters the aggregate run.
- End-to-end v2 ingestion coverage plus dedicated schema, idempotency,
  conflict, reader, custom path, and default-off tests.
- `docs/run-history.md` production activation runbook with preflight, backup,
  dry-run, explicit live-write approval, observation, and rollback stages.
- Configuration, CLI, architecture, database schema, README, agent, live-ops,
  engineering, and in-pod guidance updated.

Acceptance criteria:

- One primary run row is produced per c-val execution.
- Test membership and status are queryable without parsing comma-separated text.
- Repeat ingestion cannot duplicate a run.
- Production DB creation/migration has a backup and dry-run procedure.

Validation evidence:

- Independent read-only U6 certification found no P0/P1 blocker and declared
  local U6 ready; live activation remains unauthorized.
- Default-off end-to-end test confirms v2 compatibility ingestion proceeds
  while `node-run-history.db` is not created.
- Identical retries retain one run and one row per registered test; identity,
  status, terminal state, and registered test-set conflicts roll back.
- Missing DB reads do not create files; newer schema readers/writers reject
  safely and preserve WAL mode.
- Remote-reader tests verify stdin transport and SQLite `mode=ro`.
- Recursive shell syntax checks passed.
- `258` unit tests passed.
- Python compilation, registry validation, offline no-submit YAML parsing,
  editor diagnostics, documentation structure, and `git diff --check` passed.
- No live PVC history database was created, migrated, or written.

**Separate approval required before creating or writing this DB on the live PVC.**

## U7 — Modularize per-test result ingestion

**Status:** `DONE` — accepted 2026-07-29; live PVC migration/write remains unapproved and default-off  
**Risk:** High; raw metric persistence.  
**Depends on:** U5 and U6.

Deliverables:

- Generic ingestion adapter protocol.
- Storage, NCCL, and DL adapters.
- Canonical per-test result DB locations.
- Common raw-result and nullable latest-health fields.
- Additive migration/compatibility strategy for existing databases.

Delivered:

- `cval.plugin.v1` loader/protocol with exact API, capability, method, config,
  repository-path, immutable context, and receipt validation.
- Registry-driven v2 dispatcher with complete test-set/descriptor/config/result
  digest checks and canonical evidence/target preflight before any DB write.
- Common versioned `test_results`, `metric_ingestion_receipts`,
  `adapter_schema_versions`, exact indexes/constraints/foreign keys, and nullable
  latest-health cache columns in every canonical per-test DB.
- Built-in storage adapter: exact six-file FIO evidence, twelve finite
  non-negative metrics, immutable `storage_performance` rows.
- Built-in NCCL adapter: strict summary/HCA evidence, immutable wide `IB_HEALTH`
  rows, exact `LATEST_NODE_STATUS` and `NODE_RANKING` views.
- Built-in DL adapter: one complete current run, exact plan/iteration/GPU/rank/
  invocation/task coverage, and all four component tables in one transaction.
- Parent-owned adapter metric transactions. Ingestion and existing-schema
  validation execute in fresh spawn workers behind SQL RPC; adapters never
  receive/inherit the parent's raw SQLite connection. DDL, rows, schema version,
  and durable receipt commit or roll back together.
- Immutable retry semantics for raw envelope, parsed evidence, metric rows,
  adapter schema, and receipt. One adapter failure is isolated and never changes
  deterministic raw status or another test's result.
- Pass→fail/interrupted behavior that stores common raw rows, validates existing
  adapter schema, and never invokes metric parsing for non-passing tests.
- Shared compatibility write capabilities bound to exact result/config/path/DB
  identity; v1 storage/NCCL evidence rejects noncanonical paths, wrong types,
  and every final/ancestor symlink before parsing.
- DL rebuild root/target preflight and authorization-to-ingestion revalidation,
  including symlink-swap rejection before any metric DB open/write.
- Exact full table/index/view/trigger and raw DDL manifests for local/remote U6
  history readers and existing-DB writers; partial, extra, future,
  `sqlite_sequence`-only, and alternate-index schemas fail closed.
- Duplicate-rejecting semantic YAML validation of both source and fully rendered
  manifests, including actual one-task/one-container structure and escaped,
  tagged, explicit, flow-style, and post-substitution key collisions.
- Hidden read-only `db-preflight-test-results` and gated
  `db-ingest-test-results` hooks integrated after compatibility status in
  `db-update.sh`.
- Independent strict `per_test_ingestion_enabled=false` gate transported in the
  immutable config snapshot. Deploying U7 code alone cannot create/write live
  canonical DBs; U6 remains separately gated.
- Contract, schema, result, architecture, configuration, CLI, design,
  per-test README, repository skill, and agent/operator guidance synchronized.

Acceptance criteria:

- Existing metrics are preserved exactly.
- Raw status is never overwritten by health classification.
- Re-ingesting the same run is idempotent.
- Test adapters cannot write outside their declared paths.

Validation evidence:

- Multiple independent read-only adversarial audits were run against path,
  schema, transaction, renderer, retry, and default-off boundaries. The final
  targeted certification reported `READY` with no P0/P1/P2 finding under the
  explicit trusted-repository-plugin threat model.
- Method-global introspection tests prove ingestion and schema preflight workers
  cannot recover the parent's raw SQLite connection; failed metric DDL is fully
  rolled back while the common raw row remains.
- Fresh, retry, pass→fail, pass→interrupted, corrupt row/receipt/schema, trigger,
  partial-index, fourth-test, and per-adapter isolation cases are covered.
- V1 storage/NCCL final/ancestor symlink and noncanonical evidence attacks,
  DL root symlink and post-authorization swap, unsafe DB targets, and malformed
  rank/summary/component evidence are covered before-write.
- Renderer tests cover comments, sidecars, init containers, duplicate/flow/
  quoted/escaped/tagged/explicit keys, and resource-name collisions both before
  and after substitution.
- Local and executable remote run-history tests reject partial/extra/exact-DDL
  drift; sequence-only and semantically similar alternate-index DBs are rejected
  before writer mutation.
- `363` unit tests passed. Python 3.13 emitted known non-failing
  test-harness-owned SQLite `ResourceWarning` noise.
- Recursive Bash syntax, Python compile, registry/plugin validation (3
  registered, 3 enabled, 3 loaded), editor diagnostics, and `git diff --check`
  gates pass locally.
- No Kubernetes submission, live PVC DB creation/migration/write, commit, push,
  deployment, pod update, or loop restart was performed.

**Separate approval required before any live PVC migration or write.**

## U8 — Implement versioned health-class engine

**Status:** `DONE` — accepted 2026-07-29; local engine/storage only, no live health DB creation, migration, evaluation, or activation  
**Risk:** High; changes health interpretation.  
**Depends on:** U7.

Deliverables:

- Codes 0–5 and normalized threshold schema.
- Environment-combination stratification.
- Minimum sample and new-result triggers.
- Candidate/active/superseded lifecycle.
- Declarative metric directions and custom adapter support.
- Quality gates preventing invalid or under-sampled activation.

Acceptance criteria:

- Deterministic fixtures produce expected class boundaries.
- Missing data yields DNR/incomplete rather than nominal.
- Building a candidate never silently replaces the active baseline.
- Existing robust median/MAD behavior has regression coverage.

Implemented result:

- Stable 0–5 classes, canonical combination keys, exact normalized bands, and
  versioned declarative/custom aggregation provenance.
- Candidate identity binds current config/health policy/adapter schema,
  source/raw/receipt provenance, exact persisted observations and stable sample
  membership, reconstructed robust statistics, thresholds, and lifecycle parent.
- Framework-owned public APIs invoke canonical adapter observation readers;
  low-level caller-observation builders, classifiers, identities, and bare
  candidate persistence are private implementation/testing primitives.
- Minimum/new-result trigger evidence, one unbranched chain, candidate/active/
  superseded lifecycle, one-active index, complete ancestor validation, and
  signed activation evidence backed by an owner-only external key fail closed.
- U7 raw/version/receipt/built-in metric evidence is append-only under exact
  conflict-INSERT/UPDATE/DELETE trigger manifests, including hidden-rowid and
  `INSERT OR REPLACE` defenses. U8 reads one query-only source snapshot.
- Missing/extra metrics, result IDs, or rank/sample keys produce DNR rather than
  nominal. Raw failed/incomplete precedence cannot invoke or be overridden by a
  plugin. Floating interpolation noise is bounded without scale-dependent
  percentile disorder.

Validation evidence:

- Multiple independent read-only adversarial audits were repeated after each
  hardening cycle. The final audit reported `READY` with no P0/P1/P2 findings
  under the documented SQL-only integrity and trusted owner/plugin boundaries.
- The final full local suite passed `512` tests, including a run with
  `ResourceWarning` promoted to an error after deterministic connection cleanup.
- Recursive Bash syntax, Python compilation, registry/plugin validation (3
  registered, 3 enabled), editor diagnostics, and `git diff --check` pass.
- No Kubernetes submission, live PVC health DB creation/migration/write,
  evaluator cycle, activation, deployment, loop restart, commit, or push was
  performed.

**Separate approval is required for U9 or any live health DB operation.**

## U9 — Implement modular evaluator service

**Status:** `DONE` — accepted locally 2026-07-30; no live DB migration/write, write-gate enablement, activation, or deployment authorized
**Risk:** High; derived database writes and background processing.  
**Depends on:** U8.

Deliverables:

- Registry-driven evaluator cycle.
- Candidate build triggers.
- Pending-result classification.
- Append-only classification history.
- Structured cycle output and failure summaries.
- Locking so two evaluator cycles cannot process the same DB concurrently.

Acceptance criteria:

- Local/PVC-copy run works without Kubernetes access.
- Repeated cycles are idempotent.
- One broken test adapter does not corrupt other test results.
- Writes use transactions and bounded lock waits.

Implemented locally during the current `IN PROGRESS` stage:

- Independent strict `[health_evaluator]` config, default-off write gate,
  bounded catalog size, and bounded owner-only per-test lock.
- Public `cval.health.evaluator` cycle/report API plus `health evaluate` and
  deliberate `health activate`; both are dry-run-first and apply requires the
  gate plus exact operation-specific confirmation.
- Registry enumeration of enabled `health`+`ingest` tests, canonical confined
  U7/U8 paths, side-effect-free missing-DB skips, exact common+adapter schema
  reads, query-only catalog snapshots, canonical source/receipt provenance, and
  one-test failure isolation.
- Authoritative immutable-chain cursor, typed candidate persistence result, and
  read-only activation preflight. Advisory `health_build_state` is not used for
  correctness; built-ins remain `auto_activate=false`.
- Exact additive U7 schema v2 migration only during evaluator apply. Ordinary
  ingestion accepts exact v1/v2 without migrating. Append-only classification
  history supports no-baseline DNR, canonical metric/details evidence,
  idempotent same-baseline retries, new-baseline reevaluation, strict types,
  rollback, and exact conflict INSERT/UPDATE/DELETE/REPLACE guards.
- Raw fail/incomplete and missing combination/no active baseline DNR behavior;
  pass-without-receipt/config-drift deferral; bounded classification; no update
  of the U7 nullable latest-health cache.
- Local tests cover gates, dry-run side effects, candidates, classes/DNR,
  migration/history integrity, locking, rollback, isolation, fourth-test
  enumeration, activation, path safety, and cache immutability.

Independent U9 audit remediation implemented locally on 2026-07-30:

- Replaced source `mode=ro` evaluator/adapter reads with one checkpointed-main
  in-memory snapshot. WAL-header normalization happens only in memory; absent
  source WAL/SHM sidecars and unchanged source atime/mtime/ctime are
  regression-tested. Linux reads require `O_NOATIME` and fail closed when the
  process does not own the source or lacks equivalent permission.
- Candidate catalogs are complete cumulative current-config sets per
  combination. Only oldest-pending classification targets are page-bounded;
  reports expose backlog, remaining work, and truncation so baseline
  reevaluation drains over repeated cycles without starving older rows.
- Common schema validates first. Wholly absent adapter state permits raw
  fail/incomplete DNR history while passing rows defer; partial adapter schema
  or version state still fails closed.
- Versioned target identity now binds test/config/health policy/evaluator,
  adapter, combination/category, and baseline. Target/evidence digests make
  exact retries idempotent, changed same-target evidence a conflict, and
  version/policy/baseline changes append. A bounded existing-target page is
  reclassified and its canonical verdict digest compared before history
  idempotency is reported; identity-only prefiltering is not used.
- `cval.test.v1` rejects `auto_activate=true` rather than silently ignoring it.
- All possible reads/plugin/build/classification validation runs before any
  migration or persistence. Stage-aware reports preserve migration, candidate,
  and history effects on later failures and explicitly state that cross-DB
  commits are non-atomic.
- History work uses indexed per-selected-target lookups rather than loading the
  full history table. DNR reasons are restricted to stable `DnrReason` values
  and exact canonical details.
- Added audit-probe regressions for WAL/SHM/mtime behavior, full candidates,
  paging/backlog drain, absent/partial adapters, idempotency conflicts/version
  append, automatic activation rejection, preflight ordering, partial writes,
  bounded history lookups, and DNR reason/detail enforcement.

Second U9 audit blockers 1–6 remediated locally on 2026-07-30:

- Activation keys are read only through strict owner/mode/length-checked
  `O_NOATIME|O_NOFOLLOW` descriptors; key reads preserve atime and reject
  symlinks or non-owner-only mode.
- U7/U8/evaluator filesystem opens use one percent-encoded SQLite URI helper
  that asserts `PRAGMA database_list` resolves to the exact intended canonical
  path and device/inode, including filenames containing `?`, `#`, `%`, or
  spaces.
- The evaluator captures complete selected U7 raw and absent/present receipt
  evidence plus source inode. Migration/history revalidate it under
  `BEGIN IMMEDIATE`; candidate persistence holds the same U7 reservation for
  the complete store operation.
- Active baselines and their U8 generation are read coherently from one
  immutable snapshot. The complete current U8 generation is revalidated after
  evaluator-owned candidate stores and immediately before U7 history append;
  a changed active generation aborts classification.
- Adapter initialization is absent only when metric tables, adapter version,
  and receipts are all absent. Any partial combination, including orphan
  receipts, fails closed; every optional receipt row is type/manifest checked.
- First health DB/key creation is staged and atomically linked into final names.
  Failures remove staging and any pair members published by that operation;
  evaluator reports expose final DB/key presence and whether first-creation
  cleanup completed rather than claiming a stored candidate.
- Reproducer coverage was added for every blocker, including inode/evidence and
  U8-generation races, reserved URI characters, no-atime/no-follow keys,
  receipt-only adapter state, and failed first-create cleanup/reporting.

Remaining second-audit P2/P3 findings remediated locally on 2026-07-30:

- Routine schema/evaluator paths no longer load all `classification_history`
  rows or issue owner/target N+1 queries. Structural schema validation remains
  constant-memory; result catalogs use bounded primary-key keyset pages with one
  indexed exact `(run_id, baseline_identity)` history lookup per page; history
  stores use bounded owner/key/target batches.
- Full classification-history content integrity is a separate explicit audit
  that streams bounded `classification_id` keyset pages joined to
  `test_results`, preserving typed evidence and owner checks without an
  unbounded fetch or owner N+1.
- History persistence returns ordered per-record `stored`/`idempotent`
  outcomes. Exact concurrent appends preserve mixed action labels instead of
  relabeling every planned record as stored.
- Human-readable cycle output now includes selected/backlog/remaining/
  truncation, migration state, candidate action/insert/idempotent counts,
  history insert/idempotent counts, and partial durable writes.
- Large-history, indexed query-plan, bounded page-count, mixed race-outcome,
  streamed-integrity-audit, and table-output regressions were added.

Final three U9 audit blockers remediated locally on 2026-07-30:

- Classification-history precommit revalidation now reads the U7 catalog from
  the already-open `BEGIN IMMEDIATE` connection and projects adapter evidence
  from that transaction into memory. It no longer snapshots/reopens the U7
  source while WAL/SHM sidecars are live; checkpointed-WAL migration plus
  history append is regression-tested through sidecar cleanup.
- The remaining per-test ingestion preflight raw SQLite URI was replaced by the
  percent-encoding helper with exact `main` path/inode binding. Two successive
  built-in ingestions under a validation root containing `%`, `?`, and `#` are
  regression-tested.
- SQLite source-change identity no longer includes atime, which is not content
  identity. Source reads still require `O_NOATIME|O_NOFOLLOW`, and explicit
  source DB plus activation-key atime preservation tests remain. Repeated
  corrupt-plugin and existing-key dry-run stress runs are included in local
  validation.

Final U9 audit closure blockers remediated locally on 2026-07-30:

- Eligible candidate apply now yields the selected-result guard's active U7
  transaction and projects that connection for the complete catalog/source
  rebuild plus adapter observations. A 10-source eligible checkpointed-WAL
  apply stores the candidate, completes U7 migration/history, and leaves no
  WAL/SHM/journal or staging sidecars.
- U7 migration/history and U8 candidate/activation writes capture their
  expected SQLite identity before open and assert it immediately before every
  commit or exact retry return. U8 exposes a precommit callback so evaluator
  candidate persistence revalidates U7 identity immediately before U8 commit.
  In-transaction rename/replacement regressions prove rollback, unchanged
  replacement content, and no false success.
- Deferred passing rows are retained as bounded catalog/action entries with
  reasons. Reports and table/JSON totals expose `deferred_count`, and
  `classification_remaining` continues to include deferred work after the
  actionable backlog is stored.

Last two U9 P2 races remediated locally on 2026-07-30:

- Activation-key loads now return bytes bound to the exact canonical
  path/device/inode/mode/size identity. Candidate and activation transactions
  revalidate that identity, current key bytes, and immutable DB-owner digest
  binding beside the SQLite identity immediately before every commit and exact
  retry return. Transaction-open unlink/replacement, mode, and size regressions
  prove rollback/no false success and restored DB/key readability.
- The per-test evaluator lock captures canonical path/device/inode/link-count
  identity and revalidates its owner-only regular `0600` single-link state while
  acquiring and immediately before completion/unlock. The yielded callable
  guard is also invoked by U7 migration/history and U8 candidate/activation
  transactions at commit/retry boundaries. An unlink/recreate split-lock
  regression proves the stale cycle reports a lock error and cannot commit.

Final two U9 P2 publication/reporting blockers remediated locally on 2026-07-30:

- First-create U8 staging now carries the evaluator lock guard through canonical
  publication. The guard is revalidated immediately before and after the DB
  hardlink, activation-key hardlink, and directory fsync. Failure removes only
  identity-matching published artifacts and durably fsyncs cleanup. A staged-
  commit/pre-publication split-lock regression lets a competitor acquire the
  replacement lock and proves that the stale cycle returns failure with no
  canonical DB, key, or staging artifacts.
- Evaluator apply retains its stage-aware `TestEvaluationReport` across lock
  context teardown. A final guard failure reports `lock-finalization`, preserves
  source schema, migration/candidate/history facts, completed stages, and the
  operation error, and derives partial-write/atomicity state from durable work.
  A migration-committed split-lock regression proves the v2 migration remains
  reported as a partial durable write without a false successful cycle.

Status remains `IN PROGRESS` until independent certification and operator
acceptance. No live DB operation, Kubernetes access, deployment, background
service, commit, or push is authorized by this implementation.

Current local validation evidence:

- Recursive Bash syntax checks passed for `scripts/` and `validation-tests/`.
- `569` unit tests passed with warnings treated as errors, preserving all prior
  U0–U8 tests and covering the final U9 audit closure regressions above.
- Python compilation passed for `cval`, `tests`, and operator scripts.
- No Kubernetes, live PVC/DB, archive, commit, push, or deployment access was
  performed.

## U10 — Modularize baselines, exports, and background loops

**Status:** `DONE` — accepted locally 2026-07-30; no live loop restart, source cutover, or deployment authorized
**Risk:** High; existing live classification services.  
**Depends on:** U8 and U9.

Deliverables:

- Derive supported baseline/classification/export test choices from capabilities.
- Replace fixed storage/NCCL/DL loops with registry enumeration.
- Preserve special DL metric refresh locking.
- Keep existing commands compatible during transition.

Acceptance criteria:

- Existing storage, NCCL, and four DL component verdicts match regression fixtures.
- Disabled tests are skipped everywhere.
- A simple new metric plugin can participate without core CLI edits.

Implemented and accepted locally in U10:

- Immutable `OperationalTarget` catalog in registry order, with operation sets
  derived from enabled `baseline` and existing `export` capabilities.
- One central DL compatibility overlay for aggregate/component aliases, exposed
  only when the `dltest` owner is enabled and eligible; reserved `all`/
  `overall` and canonical/alias collisions fail during config loading.
- Strict immutable baseline/classification contexts plus read-only
  `ExportContext` and rectangular `ExportRows`; built-in adapters now declare
  and implement compatibility baseline/classification/export hooks.
- Dynamic argparse choices and handler-time target re-resolution for baseline
  build/list/show/activate/classify, results, and classifications while
  retaining aggregate aliases, current flags, CSV columns, LA filenames, and
  the wide NCCL `IB_HEALTH` output.
- Hidden read-only operational-target enumeration in TSV/JSON for local loops;
  TSV is a fixed seven-field `cval.operational-target.v1` contract, output is
  plain data, and it is never shell-evaluated.
- Safe default `plugin-<test-id>-baselines.db` placement for new plugin targets under
  the configured baseline root; read paths do not create missing baseline DBs.
- Registry-driven build/classify loops with the existing classify environment
  allowlist, no environment re-enable of disabled tests, one shared DL lock and
  refresh decision per cycle/group, per-target failure isolation, and a final
  nonzero cycle status after all selected work.
- Compatibility baseline persistence is immutable conflict-`INSERT`: exact
  retries preserve lifecycle and changed same-ID content conflicts. Strict
  baseline/verdict schemas, finite JSON-safe metrics, identities, and score
  invariants are checked at plugin return and again before persistence.
- Filesystem SQLite opens cited by U10 use canonical percent-encoded URIs with
  exact `main` path and device/inode checks. In-pod readers carry the same
  dependency-free fail-closed checks. DL loops refuse work when locking fails;
  empty/malformed catalogs and empty classify allowlist intersections fail.
- Synthetic metric-plugin coverage for catalog, parser, baseline build/store/
  activate/classify, export, and safe paths without a core CLI name edit;
  exact built-in storage/NCCL/four-component+aggregate DL dispatch fixtures,
  byte-for-byte CSV checks, and executable fake-catalog loop coverage.
- Remaining audit hardening uses transaction-safe conflict insert/re-read for
  concurrent compatibility-baseline writers; recursively freezes descriptor
  settings/config mappings across every plugin context; derives and reconciles
  complete DL component/aggregate verdicts; and filters overview history through
  the enabled classifiable catalog.
- The shared DL loop lock now flocks the existing canonical baseline directory
  opened with `O_DIRECTORY|O_NOFOLLOW`, not a replaceable child lock file. The
  helper requires effective-user ownership and no group/other write permission,
  launches work in a dedicated process session/group, forwards `INT`/`TERM`,
  and revalidates pathname/device/inode before, during, and after execution.
  On replacement, error, signal, or command-parent exit it terminates, bounded-
  grace kills, and reaps shell wrappers and descendants before releasing the
  directory flock. Marker replacement cannot split two helpers; canonical
  directory replacement is reported as a failure.

U10 is a compatibility selection/dispatch change only. Built-ins continue to
read `metadata/test-storage.db`, `metadata/test-nccl.db`, and the four
`metadata/dltest_*` DBs and continue to store under `baselines/`. There is no
U7/U8/U9 source cutover, live Kubernetes/PVC/DB access, deployment, or live
loop restart in this item. The fixed targeted-`validate` compatibility report
remains outside U10 as planned.

Current local validation evidence:

- `616` unit tests passed with warnings treated as errors, preserving the U9
  evaluator/gate/race suite and adding the U10 catalog, synthetic extension,
  exact built-in parity, CSV, executable loop cases, directory-replacement
  descendant cleanup/no-overlap, and signal forwarding above.
- Recursive Bash syntax checks passed for every shell script under `scripts/`
  and `validation-tests/`.
- Python compilation passed for `cval`, `tests`, and repository operator skill
  scripts.
- Registry/plugin validation reports 3 registered, 3 enabled, and 3 loaded.
- `git diff --check` passed.
- No Kubernetes, live PVC/DB, archive, commit, push, deployment, pod update, or
  loop restart was performed.

U10 is accepted locally. Its explicit live loop restart, source cutover, and
deployment exclusions remain in force.

## U11 — Introduce and cut over to `cval-evaluator`

**Status:** `BLOCKED` — local preparation independently certified READY 2026-07-30; no published image, Kubernetes, PVC, live backup, shadow, apply, or cutover approval is implied
**Risk:** Production Kubernetes and service cutover.  
**Depends on:** U9 and U10.

Deliverables:

- [x] Production-hardened, suspended `batch/v1` CronJob Kustomize base with
  read-only shadow and separately reviewed read-write apply variants.
- [x] Tokenless ServiceAccount without Role/binding, evaluator-scoped deny-all
  NetworkPolicy, Restricted security context, read-only root, bounded `/tmp`,
  CPU/memory/deadline/history/TTL limits, and no ports/GPU/RDMA/host path.
- [x] One-shot startup-verified `cval.evaluator-cycle.v1` stdout service with
  release/config identity, preflight, duration, U9 report, and exit code.
- [x] Side-effect-free local deployment preflight and deterministic copied-input
  U8/compatibility shadow parity report preserving original labels and DNR.
- [x] Dry-run-first, separately confirmed, evaluator-lock-aware local/disposable
  U7/U8+key backup and copied-unit restore validation without key hash logging.
- [x] Strict-JSON hidden CLI entry points; manifest/static, service, preflight,
  parity, backup, no-side-effect, determinism, idempotency, forced-termination,
  and offline rollback tests.
- [x] Current-UID/safe-mode/single-link RW preflight, orphan WAL/SHM/journal
  rejection, metadata-preserving shadow checks, and logical backup restore
  inventories with atomic destination reservation and identity-scoped cleanup.
- [x] Rendered-manifest assertions for exact image-reference declaration,
  Restricted/tokenless/deny-all policy, mode gates, mounts, suspension, and
  ephemeral-storage bounds; runtime image attestation remains a live blocker.
- [x] Architecture/config/CLI/operations rollout and rollback documentation plus
  agent/instruction/skill synchronization for locally completable preparation.
- [x] Audited copied U8 readers stream exact classification-history integrity
  checks from the same immutable snapshot before latest-row selection, bind
  result/run/test ownership, and reject malformed history; compatibility rows
  require exact SQLite storage classes, non-empty identities, and stable status.
- [x] Strict evaluator-service CLI boundary suppresses dependency stdout/stderr,
  redacts `BaseException`/`SystemExit`, preserves `KeyboardInterrupt`, and emits
  one JSON error envelope with exit code 2.
- [x] Preflight validates every existing component from the explicitly
  exact-mode runtime root through U7/U8 parents as current UID, exact descendant
  `0700`, searchable, no-symlink, and not group/world writable; it revalidates
  device/inode/mode after reads and rejects unsafe intermediate directories,
  foreign owners, and symlink swaps.
- [x] `BUILD_COMMIT` is wheel/sdist package data. A checked-in multi-stage image
  recipe consumes only the built cval wheel plus exact-version/hash-verified
  offline dependency wheels, injects the exact commit and OCI revision label,
  installs matching config/descriptors/plugins read-only, and emits a
  distroless UID/GID 65532 final image. Base and manifest digests remain
  fail-closed placeholders; build/sign/SBOM/provenance steps are documented but
  unexecuted.
- [x] Shared strict U7 owner integrity validates every raw owner, adapter schema
  owner, durable receipt, and receipt parent against the registered test;
  preflight and parity invoke it before any optional history path, so mixed
  owner schema-v1 databases fail closed.
- [x] JSON parity matches SQLite strictness for exact non-empty node/test/run
  text, required matching class code/name, stable class-5-only `DnrReason`,
  `hb1` baseline/null semantics, and exact non-negative optional integer
  timestamps without Boolean/float/coercion acceptance.
- [ ] Verify live U7 availability, PVC ownership/modes/sidecars, Kubernetes/CNI/
  admission facts, and real namespace/PVC claim.
- [ ] Build/sign/scan/publish the CPU-only image; replace digest and embedded
  commit placeholders only with independently verified release evidence.
- [ ] Approve and rehearse the live backup/restore unit, apply suspended shadow,
  accept bounded shadow parity, then separately approve apply and cutover.

Acceptance criteria:

- Shadow output matches approved evaluator results.
- Current PVC access remains available through the transition.
- Rollback has been rehearsed without deleting data.
- New service is pinned to a verified commit.

**Exact Kubernetes commands and risk summary must be approved before execution.**

All currently identified locally remediable findings are implemented and
tested, but U11 remains
`IN PROGRESS/BLOCKED` for every live criterion above. No container image build/publication,
live Kubernetes/PVC/DB or archive access,
commit, push, deployment, apply, or cutover was performed. See
`docs/u11-evaluator-rollout.md` for exact local commands, reviewed live phases,
risks, blockers, and rollback.

Current 2026-07-31 audit validation evidence:

- `81` focused U11/per-test storage tests passed with warnings treated as errors.
- `666` full unit tests passed with warnings treated as errors.
- A wheel and sdist were built without package-index access on a disposable
  source copy; both contained the fail-closed packaged `BUILD_COMMIT` marker.
- Recursive Bash syntax checks passed for `scripts/` and `validation-tests/`.
- Python compilation passed for `cval`, `tests`, and repository operator skill
  scripts.
- Registry/plugin validation reported 3 registered, 3 enabled, and 3 loaded.
- Both suspended Kustomize variants rendered locally and passed static YAML,
  resource-kind, tokenless, read-only-root, image-digest, and suspension checks.
- `git diff --check` passed.
- No Kubernetes API, live PVC/DB, archive, commit, push, deploy, pod update, or
  loop restart was performed.

## U12 — Compatibility cleanup and framework documentation

**Status:** `PROPOSED`  
**Risk:** Medium to high; removal of old surfaces.  
**Depends on:** U11 and an agreed compatibility period.

Deliverables:

- Remove fixed three-test assumptions and obsolete wrappers.
- Remove deprecated environment variables only after confirming no pinned jobs use them.
- Keep historical schema readers where needed.
- Complete operator guide for adding, updating, disabling, and removing a test.
- Add a test scaffold command or checked-in template.
- Update architecture, configuration, result schema, operations, skills, and agent instructions.

Acceptance criteria:

- Adding the synthetic fourth test requires only its directory and global registry stanza.
- Removing a test does not delete historical results.
- Full validation cadence passes.
- Deployment and rollback documentation is complete.

---

# Global Definition of Done

Each implementation item must include the tests relevant to its scope and pass the repository validation cadence:

```bash
find scripts validation-tests -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q cval tests
```

Additional requirements:

- Behavior or CLI changes update the matching documentation in the same item.
- Result/DB changes update result-schema and in-pod contract documentation.
- Background service changes update operations and deployment runbooks.
- No production action is inferred from passing local tests.

# Primary End-to-End Acceptance Scenario

A synthetic fourth validation test must be addable by:

1. Adding one compliant directory under `validation-tests/`.
2. Adding one disabled `[tests.<id>]` stanza to `config/cval.toml`.
3. Running `cval tests validate`.
4. Explicitly enabling it.

Without editing framework core code, the test must then appear in:

- Dry-run job configuration.
- Ordered execution.
- Structured progress.
- Global and per-test logs.
- `cval.results.v2`.
- Node run history.
- Raw per-test result persistence.
- Health evaluation when the plugin declares that capability.
- CLI status/export output.

# Decisions Requested with U0 Approval

Approval of this document confirms these proposed decisions:

1. Keep `config/cval.toml` as the global configuration filename.
2. Use explicit test registration with repository-relative `config_path` values.
3. Use `test_config.toml` inside each validation test directory.
4. Keep generic logging and result persistence in the framework rather than duplicating `logger.sh` in every test.
5. Use `/data/continuous_validation/metadata/node-run-history.db` for one-row-per-run history plus a normalized `run_tests` table.
6. Use `/data/continuous_validation/validation_tests/<test_id>/` for canonical per-test databases and artifacts.
7. Store health thresholds as normalized rows rather than dynamic columns or dictionaries in a single cell.
8. Preserve immutable versioned baseline lifecycle; automatic generation creates candidates, while automatic activation remains disabled by default during rollout.
9. Introduce `cval-evaluator` through a separate validated workload and approved cutover rather than renaming the live PVC pod in place.
10. Execute U1–U12 strictly one at a time with an approval gate between items.

# Research References

- [Python Packaging User Guide: creating and discovering plugins](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/)
- [pytest plugin architecture](https://docs.pytest.org/en/stable/how-to/writing_plugins.html)
- [Pluggy host/plugin and hook contracts](https://pluggy.readthedocs.io/en/stable/)
- [TOML 1.0 specification](https://toml.io/en/v1.0.0)
