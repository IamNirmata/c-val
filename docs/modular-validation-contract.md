# Modular Validation Contract

**Contract version:** `cval.test.v1`  
**Implementation status:** U2–U7 registry, boundaries, generic runtime, dynamic execution, v2 evidence, run history, and modular per-test ingestion are implemented and locally validated; production U6/U7 writes remain independently default-off  
**Compatibility target:** Existing storage, NCCL, and DL behavior remains available throughout migration.

This document defines the locally implemented repository contract. It does not
authorize production activation, database creation, migration, or deployment.

## Design principles

1. Registration is explicit. A directory cannot activate itself.
2. Activation and test configuration are separate.
3. Shared Kubernetes resources stay in the global configuration.
4. Test-specific behavior stays with the test.
5. Core code owns ordering, timeouts, logging, result state, and raw run history.
6. Test adapters own only test-specific parsing, metrics, health observations, and exports.
7. Paths are resolved against declared roots, never the process working directory.
8. New contract versions are additive; old results remain readable.
9. A new pass/fail-only test does not require Python adapter code.
10. One failing plugin cannot change another plugin's files or results.

## Terminology

| Term | Meaning |
| --- | --- |
| Repository root | The checkout containing `pyproject.toml`, `cval/`, and `validation-tests/`. |
| Global config | The operator-owned `config/cval.toml`. |
| Test directory | One direct child of `validation-tests/` containing a registered test. |
| Test config | The test-owned `test_config.toml`. |
| Test ID | Stable lowercase identifier used in config, paths, results, and databases. |
| Run ID | Stable identifier for one c-val execution on one node. |
| Adapter | Optional trusted Python code providing declared test-specific capabilities. |
| Raw status | Deterministic `pass`, `fail`, or `incomplete` result from execution. |
| Health class | Derived class assigned against a compatible active baseline. |

## Global test registry

The global configuration contains one table per registered test:

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

### Registry field definitions

| Field | Type | Required | Rules |
| --- | --- | --- | --- |
| `enabled` | Boolean | Yes | Must be a TOML Boolean. New tests are added as `false`. |
| `config_path` | String | Yes | Repository-relative path to `test_config.toml`; absolute paths and root escape are rejected. |

Unknown fields under `[tests.<id>]` are rejected in `cval.test.v1`. This prevents test settings from drifting back into the global file.

### Registry validation

Before job rendering, c-val must reject:

- No enabled tests.
- Duplicate test IDs.
- IDs that fail `^[a-z][a-z0-9-]{0,62}$`.
- A registry ID that differs from `[test].id`.
- Missing, non-regular, or unreadable config files.
- Absolute `config_path` values.
- Symlink or `..` resolution outside the repository root.
- Duplicate execution orders among enabled tests.
- An unsupported test schema or adapter API version.
- Enabled tests whose minimum resources exceed the shared job allocation.

Disabled test configs are still validated by `cval tests validate`; this catches
errors before activation. `tests list` and `tests describe` do not import
adapters. `tests validate` deliberately imports every declared repository
adapter and verifies its API, capabilities, and config validator.

## Test directory contract

```text
validation-tests/<test-id>/
  README.md
  test_config.toml
  setup.sh
  run-test.sh
  plugin.py              # optional
  tests/                 # contract and workload tests
  ...                    # test-owned assets and sub-runners
```

### Required files

#### `README.md`

Must document:

- Purpose and failure meaning.
- Dependencies and setup behavior.
- Workload steps and expected duration.
- Test-specific configuration fields.
- Produced summary, metrics, and artifacts.
- Health metrics, directions, combination factors, and baseline method.
- Troubleshooting and safe standalone invocation.

#### `test_config.toml`

Must satisfy the schema in this document.

#### `setup.sh`

Contract:

- Idempotent when called repeatedly in the same image.
- Performs only declared dependency checks or installation.
- Does not mutate Kubernetes or shared databases.
- Writes normal output to stdout/stderr for central capture.
- Returns `0` on success and non-zero on setup failure.
- May be a no-op when the image already contains all dependencies.

#### `run-test.sh`

Contract:

- Runs only its own test.
- Reads the assigned test config and runtime context.
- Writes only under assigned log/output paths, except explicitly declared read-only inputs.
- Does not write raw run history or another test's database.
- Returns `0` for pass and non-zero for fail.
- May write an optional `cval.test-result.v1` summary; the framework still owns the authoritative `cval.results.v2` envelope.

### Optional files

#### `plugin.py`

Provides one or more adapter capabilities defined below. It is trusted repository code loaded only from a registered, validated path in the pinned checkout.

#### Test-owned assets

Sub-runners, programs, workload plans, FIO profiles, and fixtures stay inside the test directory. A test can use read-only shared inputs declared by global runtime configuration.

## `test_config.toml` schema

### Root

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `schema_version` | String | Yes | — | Exactly `cval.test.v1`. |

Unknown root keys are rejected.

### `[test]`

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `id` | String | Yes | — | Matches `^[a-z][a-z0-9-]{0,62}$` and registry table name. Immutable after release. |
| `display_name` | String | Yes | — | Trimmed, 1–100 characters. |
| `description` | String | No | `""` | Short operator-facing description, at most 500 characters. |
| `order` | Integer | Yes | — | Non-negative and unique among enabled tests. Lower runs first. |
| `entrypoint` | String | Yes | — | Relative regular file inside the test directory. |
| `setup` | String | Yes | — | Relative regular file inside the test directory. |
| `timeout_seconds` | Integer | Yes | — | Positive and no greater than global job timeout. |
| `continue_on_failure` | Boolean | No | `true` | `false` requires explicit future policy approval; v1 runtime accepts only `true`. |

Entrypoint and setup paths cannot be absolute, symlink outside the test directory, or contain unresolved traversal.

### `[requirements]`

These are minimums used to validate the shared job allocation. They do not independently change Kubernetes requests.

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `cpu` | String | No | `"0"` | Valid Kubernetes CPU quantity. |
| `memory` | String | No | `"0"` | Valid Kubernetes memory quantity. |
| `gpu_count` | Integer | No | `0` | Non-negative. |
| `rdma_count` | Integer | No | `0` | Non-negative. |
| `shared_memory` | String | No | `"0"` | Valid Kubernetes memory quantity. |
| `read_sysfs` | Boolean | No | `false` | Requests read-only access to the globally approved sysfs mount. |

Unknown requirement keys are rejected. The registry compares each enabled minimum with the global shared job allocation and approved mounts.

### `[settings]`

An arbitrary TOML table owned by the test. The framework preserves TOML scalar, array, and table types and passes the resolved test config path to the test. Core code does not convert every setting into an environment variable.

The test or adapter must reject unknown settings. Secrets are prohibited.

### `[artifacts]`

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `results_db_path` | String | Yes | — | Relative to the global validation root and confined below `validation_tests/<test-id>/`. |
| `health_classes_db_path` | String | Conditional | — | Required when health is enabled; same confinement rule. |
| `summary_filename` | String | No | `"summary.json"` | Basename only. |

Example:

```toml
[artifacts]
results_db_path = "validation_tests/nccl/nccl_results.db"
health_classes_db_path = "validation_tests/nccl/nccl_health_classes.db"
summary_filename = "summary.json"
```

At runtime these resolve beneath `/data/continuous_validation` or the configured validation root. Root-relative values keep the test portable when the PVC mount changes.

### `[plugin]`

Optional. Omit it for a pass/fail-only test with no custom ingestion, health, or export behavior.

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `adapter` | String | Yes | — | Relative Python file inside the test directory. |
| `api_version` | String | Yes | — | Exactly `cval.plugin.v1`. |
| `capabilities` | Array of strings | Yes | — | Unique values from `config`, `ingest`, `health`, `export`. |

The adapter is imported only from its validated repository-confined path.
Import failure, API drift, capability drift, or invalid adapter-owned config is
a configuration error before submission.

### `[health]`

Required when the adapter declares `health`; otherwise it may be omitted or set `enabled=false`.

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `enabled` | Boolean | Yes | — | Enables health candidate generation and classification. |
| `policy_version` | String | Conditional | — | Required when enabled; stable adapter/aggregation semantics such as `nccl.health.v1`. Must match `^[a-z][a-z0-9_.-]*\.v[1-9][0-9]*$` and the adapter's `health_policy_version`. Bump for an incompatible health observation or aggregation change. |
| `strategy` | String | No | `"declarative"` | `declarative` or `custom`. In v1, `custom` changes only final verdict aggregation; candidate statistics and metric verdicts remain framework-owned. |
| `min_samples` | Integer | Yes | — | At least 3; normal fleet default is 8 or more. |
| `min_new_results` | Integer | Yes | — | Positive trigger count for a new candidate. |
| `target_class_count` | Integer | No | `5` | Exactly 5 in v1; DNR is code 5 and has no metric threshold. |
| `combination_factors` | Array of strings | Yes | — | Non-empty, unique, and supported by the adapter/result schema. |
| `auto_activate` | Boolean | No | `false` | `cval.test.v1` accepts only false; activation is deliberate and approval-gated. |
| `robust_z_threshold` | Float | No | Global default | Positive. |

Recommended initial combination factors include `image_name`, `cuda_version`, and `pytorch_version`, plus workload parameters that materially affect results.

### `[[health.metrics]]`

Required for every enabled health test, including custom aggregation.

| Field | Type | Required | Default | Rules |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | — | Stable unique metric name. |
| `source` | String | Yes | — | Adapter-owned observation key, not raw SQL supplied by an operator. |
| `direction` | String | Yes | — | `low_bad`, `high_bad`, `two_sided`, or `absolute`. |
| `units` | String | No | `""` | Stable display units. |
| `tolerance_pct` | Float | Yes | — | Non-negative engineering tolerance floor. |
| `weight` | Float | No | `1.0` | Positive; reserved for approved aggregation policies. |

SQL table/column names are not accepted directly from untrusted config. The adapter maps known source keys to internal queries.

## Runtime context

U4 implements this pod-level generic context:

| Variable | Meaning |
| --- | --- |
| `CVAL_RUN_ID` | `<node>-<timestamp>` identity assigned in the manifest. |
| `CVAL_CONFIG_PATH` | Canonical config location in the pinned checkout. |
| `CVAL_CONFIG_DIGEST` | SHA-256 of canonical composed effective config. |
| `CVAL_ENABLED_TESTS` | Ordered comma-separated enabled IDs. |
| `CVAL_TEST_REGISTRY_JSON` | ID-keyed activation, config path, and order metadata. |
| `CVAL_VALIDATION_ROOT` | Shared validation root. |
| `CVAL_RUNTIME_ENV_B64` | Transport-only encoded context sourced after checkout. |

The payload temporarily includes current fixed `RUN_*` and `CVAL_*` exports so
the compatibility ingestion path remains behaviorally compatible. The generic
runner assigns these stable per-test variables to setup and entrypoint
processes:

| Variable | Meaning |
| --- | --- |
| `CVAL_RUN_ID` | Current run ID. |
| `CVAL_TEST_ID` | Current test ID. |
| `CVAL_NODE` | Target node name. |
| `CVAL_TIMESTAMP` | Run start epoch used for compatibility/artifact identity. |
| `CVAL_REPO_DIR` | Absolute repository checkout. |
| `CVAL_TEST_DIR` | Absolute registered test directory. |
| `CVAL_TEST_CONFIG` | Absolute validated test config path. |
| `CVAL_VALIDATION_ROOT` | Absolute shared validation root. |
| `CVAL_TEST_OUTPUT_DIR` | Absolute assigned test artifact directory. |
| `CVAL_TEST_LOG_DIR` | Absolute assigned per-test log directory. |
| `CVAL_TEST_SUMMARY_FILE` | Absolute assigned summary path. |
| `CVAL_IMAGE_NAME` | Validation image identity. |
| `CVAL_PYTORCH_VERSION` | Best-effort detected PyTorch version. |
| `CVAL_CUDA_VERSION` | Best-effort detected CUDA version. |

The runner invokes scripts as argument arrays with the test directory as the working directory. It does not use settings to construct an interpolated `bash -c` command.

## Logging and execution ownership

Core framework responsibilities:

- Create global and per-test directories.
- Capture stdout and stderr separately and in an ordered combined stream where practical.
- Emit structured framework events.
- Measure timestamps and duration.
- Convert timeout, setup failure, signal, and missing summary into explicit result state.
- Atomically update `cval.results.v2` after each state transition.
- Write run history and dispatch declared adapters.

Test responsibilities:

- Emit useful workload output.
- Produce a machine-readable summary when metrics are available.
- Return a correct exit code.
- Avoid writing framework databases directly.

## Adapter protocol

An adapter file exports exactly one object named `PLUGIN` and a constant:

```text
CVAL_PLUGIN_API = "cval.plugin.v1"
```

The adapter object exposes:

```text
plugin_id: str
capabilities: frozenset[str]
health_policy_version: str  # required with the health capability
```

`plugin_id` must equal the test ID. Capabilities must equal the config declaration.
`health_policy_version` must exactly equal `[health].policy_version`.

### Common value objects

The framework supplies immutable value objects rather than internal managers
or raw database connections:

- `TestDefinition`: validated test metadata and test-owned settings.
- `RunContext`: run ID, node, versions, timestamps, and confined paths.
- `TestExecutionResult`: raw status, exit code, paths, timing, and summary payload.
- `IngestionContext`: execution result plus declared result DB path.
- `HealthContext`: validated definition, read-only result DB path, canonical
  combination, exact source snapshot, lifecycle parent, evaluator/robust-z
  policy, raw status, and optional deterministic creation timestamp.
- `ExportContext`: read-only result source and selected output destination.

Adapters must not receive a Kubernetes client.

### `config` capability

```text
validate_config(definition: TestDefinition) -> tuple[ConfigIssue, ...]
```

- Returns all deterministic validation issues.
- An empty tuple means valid.
- Does not perform network, Kubernetes, or database writes.
- May check declared local files.

### `ingest` capability

```text
validate_schema(connection, allow_missing: bool) -> bool
ingest(context: IngestionContext) -> IngestionReceipt
```

- Parses only the current test's declared artifacts.
- Uses an idempotency key based on `(test_id, run_id)`.
- Opens only the declared result database through
  `metric_ingestion_transaction()`.
- Returns `true` from `validate_schema` only when all adapter-owned tables and
  objects are present and exact for the declared adapter version.
- Returns inserted/updated counts and metric names.
- Raises a typed adapter error on invalid input; the framework records ingestion failure without changing raw test status.

### `health` capability

```text
metric_specs(definition: TestDefinition) -> tuple[MetricSpec, ...]
load_observations(context: HealthContext) -> tuple[MetricObservation, ...]
```

Custom aggregation method (required only for `strategy="custom"`):

```text
classify(context: HealthContext, active_candidate, observations,
         framework_verdict) -> HealthVerdict
```

Rules:

- Framework callers use `build_candidate_from_plugin()` and
  `classify_from_plugin()`, which invoke `load_observations()` themselves. Pure
  observation builders are internal/testing primitives and are not an
  orchestration boundary for untrusted caller-supplied values.
- All tests implement metric specifications and observation loading. The core
  validates and canonically orders observations, computes robust statistics,
  builds content-addressed candidates, creates normalized bands, and produces
  per-metric verdicts.
- `build_candidate` is not a v1 adapter hook. A plugin exporting it is rejected;
  this prevents custom code from replacing source provenance, quality gates,
  statistics, or immutable candidate identity.
- A custom classifier may aggregate only the framework-generated metric
  verdicts. It must preserve those verdicts, return a versioned `aggregation`
  identifier in canonical `details_json`, and cannot hide framework DNR.
- Observations use stable metric names, stable per-result `sample_key` values,
  and finite numeric values; arbitrary SQL fragments are prohibited. Every
  expanded metric must expose the same exact non-empty sample-key set in each
  qualifying result. Missing or extra current samples are DNR, not nominal.
- Source snapshots bind raw/result/config/combination digests, one uniform
  positive adapter schema version, and durable receipt evidence.
- Health operations write through framework-owned storage services.

### `export` capability

```text
export_rows(context: ExportContext) -> tuple[dict[str, scalar], ...]
```

- Read-only.
- Returns JSON/CSV-safe scalar values.
- Does not create the destination file directly; the framework owns output naming and writing.

### Adapter isolation and failure behavior

Repository adapters are trusted code, not an operating-system security
sandbox. Ingestion and existing-schema validation run in fresh
`multiprocessing` spawn workers. Workers receive immutable contexts plus an RPC
SQLite facade; the parent process retains the only live raw
`sqlite3.Connection`, transaction, authorizer, commit, and rollback authority.
Direct plugin filesystem/process abuse or opening an independent SQLite
connection is outside this trusted-plugin contract.

Isolation and failure rules:

- A failure is attributed to one test and logged.
- Other tests continue when safe.
- Adapter DDL, metric rows, schema-version row, and durable ingestion receipt
  share one parent-owned transaction.
- Adapter `commit`, `rollback`, transaction SQL, `ATTACH`, `VACUUM`, authorizer
  replacement, and SQL scripts are rejected.
- A failed adapter transaction is rolled back completely; the separately
  committed common raw result row remains authoritative.
- Passing retries are immutable and idempotent. A changed raw result, parsed
  evidence digest, persisted metric row, or durable receipt is a conflict.
- Common raw rows, adapter schema-version rows, durable receipts, and built-in
  metric rows have exact framework-owned conflict-INSERT/UPDATE/DELETE triggers.
  This blocks `INSERT OR REPLACE` as well as ordinary mutation; writer
  connections also enable recursive triggers. Missing, altered, or extra
  triggers fail exact schema validation before read/write reuse.
- Failed or interrupted tests persist common raw rows and validate any existing
  adapter schema without invoking metric ingestion.
- Adapter errors never convert a raw failed test to pass.
- A health adapter failure leaves the latest raw result unevaluated.
- U9 enumerates only enabled tests with both `health` and `ingest`; one test
  error cannot stop another. It copies each checkpointed canonical U7 main
  image into one query-only in-memory catalog snapshot without opening SQLite
  on the source during dry-run.
- Candidate construction/classification always invokes the plugin's canonical
  `load_observations()` API. Callers cannot supply observations to the public
  evaluator.
- Passing rows without a durable receipt are deferred. Raw fail/incomplete,
  missing combinations, and absent active baselines are deterministic DNR.
- Apply uses an owner-only bounded per-test lock and exact v1→v2 migration.
  Classification history is immutable; U7 latest-health cache columns are not
  updated. The final history revalidation uses the already-open U7 write
  transaction for the catalog and an in-memory projection of that connection
  for plugin evidence; it does not reopen the canonical WAL source after
  `BEGIN IMMEDIATE`.
- Routine classification uses bounded primary-key result pages and one indexed
  exact-target history lookup per page. Full history validation is a separate
  streamed owner-joined audit. History stores return ordered per-record
  `stored`/`idempotent` outcomes so exact races do not relabel a mixed batch.

`storage.per_test_ingestion_enabled` is an independent production write gate.
It defaults to `false`; deploying U7 code alone cannot create canonical
per-test databases. The compatibility status and metric writers remain active
until a separately approved dual-write activation and migration.

Future externally packaged adapters may use the `cval.validation_tests` entry-point group, but entry-point discovery is outside `cval.test.v1` implementation scope.

## Canonical path resolution

### Repository-relative paths

Applies to registry config paths, entrypoints, setup scripts, adapters, and test assets.

Resolution algorithm:

1. Reject an absolute input.
2. Join input to its declared root (`repository root` or `test directory`).
3. Resolve symlinks and `..` components.
4. Require the result to remain under the declared root.
5. Require expected file type and permissions.

### Validation-root-relative paths

Applies to result and health database paths from test config.

Resolution algorithm:

1. Reject an absolute input.
2. Join to configured `runtime.validation_root`.
3. Normalize path components.
4. Require the result below `validation_tests/<test-id>/`.
5. Reject symlink escape when parent paths exist.

The U9 evaluator applies the same algorithm to both canonical result and health
DB paths. Missing canonical U7 DBs are dry-run skips; no alternate or discovered
database is accepted.

### Assigned run paths

Core computes these; tests cannot override them:

```text
logs/job_logs/<node>/<run-id>/
logs/<test-id>/<node>/<run-id>/
validation_tests/<test-id>/runs/<node>/<run-id>/
```

Node, test, and run IDs are validated path segments without slash, NUL, `.` or `..` values.

## Synthetic `smoke` contract test

The modular framework acceptance fixture is a fourth test named `smoke`:

```text
validation-tests/smoke/
  README.md
  test_config.toml
  setup.sh
  run-test.sh
  tests/
```

Registry entry:

```toml
[tests.smoke]
enabled = false
config_path = "validation-tests/smoke/test_config.toml"
```

Test config:

```toml
schema_version = "cval.test.v1"

[test]
id = "smoke"
display_name = "Framework smoke test"
order = 999
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30

[requirements]
gpu_count = 0
rdma_count = 0

[settings]
message = "ok"

[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
```

Behavior:

- Setup exits successfully without installation.
- Runner writes a small summary to `CVAL_TEST_SUMMARY_FILE`, writes stdout and stderr markers, and exits successfully.
- No adapter or health section is needed.
- Contract tests enable it only in a temporary config.

Acceptance assertions:

1. Registry discovery requires only its directory and global stanza.
2. Disabled smoke does not execute.
3. Enabled smoke appears in deterministic order.
4. Logs and artifacts use assigned paths.
5. `cval.results.v2` includes smoke without changes to core test-name lists.
6. Run history contains smoke membership and status.
7. No health record is expected because the fixture has no health capability.

## Current-to-target compatibility map

| Current surface | Target surface | Migration rule |
| --- | --- | --- |
| `[tests.storage]` includes `install_fio` | `storage/test_config.toml [settings]` | Registry provides temporary compatibility property/env value. |
| `[tests.nccl]` includes workload settings | `nccl/test_config.toml [settings]` | Preserve values exactly during U2. |
| `[tests.dltest]` includes plan/iterations | `dltest/test_config.toml [settings]` | Preserve values exactly during U2. |
| Fixed `TestsConfig` dataclass attributes | Mapping-like typed `TestRegistry` | Compatibility access remains until consumers are migrated. |
| Test-specific YAML environment placeholders | Generic config/runtime context | Removed in U4; encoded compatibility exports remain while pinned v1 jobs and compatibility consumers are supported. |
| Top-level monolithic `run-test.sh` | Generic runner plus per-test entrypoints | Existing path remains a temporary wrapper. |
| `GCRRESULT1/2/3` and `RUN_*` | Dynamic test map in `cval.results.v2` | Retain only for v1 jobs during compatibility period. |
| `cval.results.v1` fixed tests | `cval.results.v2` arbitrary test IDs | v1 parser remains read-only; no historical rewrite. |
| Hard-coded progress markers | `CVAL_EVENT` JSON lines | Parse old markers while pinned v1 jobs may run. |
| `validation.db` row per fixed test | Node run history plus per-test result DBs | Default-off dual-write is implemented; do not activate, delete, or rewrite old DBs without separate approval. |
| Fixed storage/NCCL/DL ingestion branches | Adapter capabilities | Storage, NCCL, and one-run DL adapters are implemented; compatibility writers remain the current production surface. |
| Fixed baseline CLI choices | Registry health capabilities | Preserve aliases during compatibility period. |
| Global `classification-results.db` | Per-test health DB plus result cache/history | Keep old DB readable until reporting parity and approved cleanup. |

## Contract evolution

- `schema_version` and adapter API are independently versioned.
- Adding optional fields with defined defaults may remain within v1.
- Changing field meaning, required behavior, path ownership, or status semantics requires a new major contract version.
- Readers reject unsupported versions with a precise error.
- Writers emit only one current version.
- Historical configs and artifacts are never rewritten merely to upgrade the reader.
