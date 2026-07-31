# Configuration

c-val uses TOML as its canonical operator configuration format.

Configuration is composed from the global operator file and explicitly
registered per-test files. The global file owns orchestration, infrastructure,
activation, and shared job resources. Each validation test owns its workload
and health settings in `validation-tests/<test-id>/test_config.toml`.

Default config:

```text
config/cval.toml
```

Override config path:

```bash
python -m cval.cli --config /path/to/cval.toml config
```

or:

```bash
export CVAL_CONFIG=/path/to/cval.toml
python -m cval.cli config
```

## Why TOML

TOML is the best fit for c-val's durable configuration because:

- it is readable and reviewable in Git
- it supports comments and typed nested sections
- Python 3.11+ parses it with stdlib `tomllib`
- operator TOML parsing remains independent of YAML parsing
- it maps cleanly to dataclasses in `cval.config`

PyYAML is a runtime dependency only for safe Kubernetes manifest validation:
the renderer parses both the source template and final substituted YAML with a
duplicate-rejecting loader before any manifest can be submitted.

Other formats still have roles:

| Format | c-val use |
| --- | --- |
| TOML | Human-owned defaults and environment profiles. |
| YAML | Kubernetes and Volcano manifests only. |
| JSON | Machine output, structured result artifacts, and API-like CLI output. |
| env vars | Last-mile runtime overrides such as `CVAL_CONFIG`, pod env, and CI settings. |
| txt | Human logs and summaries, not structured config. |

## Sections

```toml
[cluster]
namespace = "gcr-admin"
pvc_access_pod = "gcr-admin-pvc-access"
node_filter = "hgx"
tolerated_no_schedule_taints = ["nvidia.com/gpu", "rdma"]

[scheduling]
days_threshold = 7
batch_size = 5

[job]
template_path = "ymls/specific-node-job.yml"
job_prefix = "cval"
git_repo = "https://github.com/IamNirmata/c-val.git"
git_ref = "main"

[policy]
namespace_allowlist = ["gcr-admin"]
max_batch_size = 5
confirmation_phrase = "submit"

[monitoring]
timeout_seconds = 6000
poll_interval_seconds = 60

[storage]
validation_db_path = "/data/continuous_validation/metadata/validation.db"
run_history_enabled = false
run_history_db_path = "/data/continuous_validation/metadata/node-run-history.db"
per_test_ingestion_enabled = false
storage_db_path = "/data/continuous_validation/metadata/test-storage.db"
nccl_db_path = "/data/continuous_validation/metadata/test-nccl.db"
dl_numerical_db_path = "/data/continuous_validation/metadata/dltest_numerical_correctness.db"
dl_compute_db_path = "/data/continuous_validation/metadata/dltest_compute_performance.db"
dl_collective_db_path = "/data/continuous_validation/metadata/dltest_collective_performance.db"
dl_overlap_db_path = "/data/continuous_validation/metadata/dltest_overlap_performance.db"

[runtime]
repo_dir = "/workspace/c-val"
validation_root = "/data/continuous_validation"
validation_tests_dir = "/workspace/c-val/validation-tests"
dl_unit_test_dir = "/data/continuous_validation/deep-learning-unit-test-main"
dl_results_root_path = "/data/continuous_validation/validation_tests/dltest/runs"

[tests.storage]
enabled = true
config_path = "validation-tests/storage/test_config.toml"

[tests.nccl]
enabled = true
config_path = "validation-tests/nccl/test_config.toml"

[tests.dltest]
enabled = true
config_path = "validation-tests/dltest/test_config.toml"

[job_template]
namespace = "gcr-admin"
queue = "gcr-admin"
app_label = "hari-gcr-bonete-test"
pvc_claim = "pvc-vast-gcr-admin"
container_image = "nvcr.io/nvidia/pytorch:26.05-py3"
shared_memory_size = "256Gi"
gpu_resource_name = "nvidia.com/gpu"
gpu_count = "8"
cpu = "100"
memory = "1500Gi"
rdma_resource_name = "rdma/rdma_shared_device_a"
rdma_count = "1"

[baseline]
baseline_root_path = "/data/continuous_validation/baselines"
robust_z_threshold = 3.5
min_samples = 8
window_days = 30
build_interval_seconds = 86400
classify_interval_seconds = 300
```

## Test registry and switches

Each test is explicitly registered and can be independently enabled or disabled:

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

`config_path` is resolved against the c-val repository root, not the current
working directory or the location of an override global config. Absolute paths,
missing files, duplicate paths, mismatched IDs, and paths escaping the checkout
are rejected before job rendering. Merely adding a directory cannot activate a
test.

During the compatibility stage, activation and settings for the three existing
tests are still rendered into the pod as current `RUN_*` and `CVAL_*` aliases.
They are derived from the complete composed registry and carried in one
deterministic generic runtime payload rather than individual Kubernetes YAML
placeholders. A disabled phase is not executed and does not write metric
rows. Its structured result is `status="incomplete", enabled=false`; aggregate
status is computed from enabled phases only. At least one test must remain
enabled. Background baseline loops also consume the composed activation state.

## Per-test configuration

Current test-owned files:

```text
validation-tests/storage/test_config.toml
validation-tests/nccl/test_config.toml
validation-tests/dltest/test_config.toml
```

Each descriptor uses `schema_version = "cval.test.v1"` and declares:

- Test identity, display name, deterministic order, entrypoint, setup, and timeout.
- Minimum shared CPU, memory, GPU, RDMA, and `/dev/shm` requirements.
- An arbitrary test-owned `[settings]` table.
- Validation-root-relative result and health database paths.
- Test-owned health policy version, combination factors, metrics, directions,
  tolerances, sample trigger controls, and activation preference.

Every enabled `[health]` table requires a versioned `policy_version`, for
example `storage.health.v1`. The loaded plugin must export the identical
`health_policy_version`. Bump both when an observation's meaning, expansion,
sample identity, or custom aggregation becomes incompatible; the descriptor
digest then prevents an old active candidate from being evaluated as current.

`cval.test.v1` rejects `health.auto_activate=true`; all built-ins remain false.
U9 builds only candidates and never silently ignores an automatic-activation
request. Activation is a separate named-candidate operation and still requires
the evaluator write gate and exact confirmation.

The loader validates that global shared job resources cover every enabled test.
The current storage/NCCL/DL consumers receive compatibility dataclasses derived
from these descriptors, so workload values remain unchanged while fixed test
assumptions are removed incrementally.

The complete field contract is in
[Modular Validation Contract](modular-validation-contract.md).

## Rendered runtime context

The job manifest contains only shared runtime fields plus
`CVAL_RUNTIME_ENV_B64`. The payload decodes to shell-quoted exports generated
from typed configuration, including:

- `CVAL_CONFIG_PATH` and a stable `CVAL_CONFIG_DIGEST`.
- Ordered `CVAL_ENABLED_TESTS` and `CVAL_TEST_REGISTRY_JSON`.
- Test config paths and activation states.
- Temporary v1 `RUN_*`/`CVAL_*` compatibility values.

The manifest also assigns `CVAL_RUN_ID=<node>-<timestamp>`. The generic runner
uses this context for arbitrary registered tests; fixed exports remain only for
compatibility ingestion and pinned readers. Operators cannot inject arbitrary
shell source through TOML: names are fixed and values are shell-quoted before
base64 encoding.

`job_template` values are Kubernetes reservations for the whole pod. They must
cover the minimum requirements declared by every enabled test; config loading
fails before rendering when they do not. `gpu_resource_name` and
`rdma_resource_name` must be distinct. The renderer semantically rejects YAML
key collisions before and after substitution.

## Storage write gates

`run_history_enabled` and `per_test_ingestion_enabled` are strict TOML Booleans
and independent production-write gates. Both default to `false`.

- `run_history_enabled=true` allows finalized v2 envelopes to populate
  `metadata/node-run-history.db`.
- `per_test_ingestion_enabled=true` enables canonical common rows and declared
  storage/NCCL/DL adapter metrics under
  `validation_tests/<test-id>/<test-id>_results.db`.

The in-pod script compares these values with the immutable config snapshot.
Environment text cannot override a false snapshot value. Enabling either gate,
creating a live target, or migrating historical data requires a separate
backup/dry-run/activation approval.

## Health evaluator write gate

U9 has an independent strict section:

```toml
[health_evaluator]
write_enabled = false
lock_timeout_seconds = 30
max_classifications_per_test = 250
validation_root_mode = "0700"
```

- `write_enabled` must be a TOML Boolean. It does not inherit from or enable
  either U6/U7 ingestion gate.
- `lock_timeout_seconds` and `max_classifications_per_test` must be positive integers.
  `validation_root_mode` is an exact four-digit octal string, must grant owner
  read/search, and must not grant group/world write. U11 requires this exact
  mode on `runtime.validation_root` and exact `0700` on every existing
  descendant through each test-owned U7/U8 parent. Unknown keys are rejected.
- Dry-run creates no lock, migration, health DB, activation key, or history row.
- Apply additionally requires `--apply --confirm evaluate`; manual activation
  requires `--apply --confirm activate`.
- U11's one-shot service has a separate `--write-enabled` process flag for the
  deployment write gate; there is no duplicate write-enable environment
  variable. Shadow rejects a true gate; apply requires both a
  true gate and exact `--apply --confirm evaluate`. This projection changes only
  the in-memory service config and does not rewrite TOML.
- Release identity is not configuration: the image supplies an embedded commit
  marker and deployment supplies matching `CVAL_EXPECTED_COMMIT` plus a
  digest-pinned `CVAL_IMAGE_REF` exactly equal to the rendered container image.
  This verifies declarations, not the actual runtime image; admission/signature/
  provenance policy must provide that binding. All-zero placeholders fail closed.

The max bounds only the oldest pending classifications selected per test/cycle.
Candidate source catalogs always scan the complete cumulative current-config
passing set per environment combination, so baseline triggers cannot be made
impossible by this work bound. Reports expose selected, backlog, remaining, and
truncated counts. Result/history matching is performed in fixed-size keyset
pages using the `test_results` primary key and the unique
`classification_history(run_id, baseline_identity)` index; the configured
classification limit does not cause an unbounded history fetch. Apply holds one
owner-only `0600`, no-symlink lock beside each canonical result DB for a bounded
interval.

The `[runtime]` `dl_results_root_path` points at remapped DL rank JSON artifacts
(`dltest-<node>-<timestamp>/workdir/test_plans/<plan>/runs/*.json`). The
`[storage]` `dl_*_db_path` entries point at the four DL metric DBs rebuilt from
those JSON files. Global `[baseline]` values control the shared root, robust
statistics defaults, window, and loop cadence. Test-specific tolerances,
combination factors, and DL aggregation settings are test-owned. Existing
baseline code receives the same effective compatibility values (see
[Baselines and Node Classification](baselines.md)).

## Precedence

The human-readable image identity written into job names, result JSON, and
SQLite rows is derived from `job_template.container_image`.

1. CLI flags such as `--batch-size`, `--git-ref`, and `--namespace`.
2. Config file supplied by `--config`.
3. Config file supplied by `CVAL_CONFIG`.
4. Repository default `config/cval.toml`.
5. Built-in dataclass defaults if no config file is present.

## Validation

Show the effective config:

```bash
python -m cval.cli config
```

Inspect and validate the composed registry:

```bash
python -m cval.cli tests list
python -m cval.cli tests describe nccl
python -m cval.cli tests validate --output json
```

Dry-run a job to confirm configured defaults:

```bash
python -m cval.cli run \
  --free-nodes slc01-cl02-hgx-0001 \
  --timestamp 12345 \
  --git-ref <commit-or-tag> \
  --output json
```