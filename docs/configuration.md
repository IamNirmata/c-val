# Configuration

c-val uses TOML as its canonical operator configuration format.

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
- it avoids adding PyYAML or other config dependencies
- it maps cleanly to dataclasses in `cval.config`

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
timeout_seconds = 180
poll_interval_seconds = 60

[storage]
validation_db_path = "/data/continuous_validation/metadata/validation.db"
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
dl_results_root_path = "/data/continuous_validation/dltest"

[tests.storage]
enabled = true
install_fio = true

[tests.nccl]
enabled = true
gpu_count = 8
iterations = 20
data_size_gb = 8
ibbw_enabled = true
ibbw_start_device = 0
ibbw_end_device = 13
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"

[tests.dltest]
enabled = true
gpu_count = 8
test_plan = "80gb-example"
iterations = 100

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
nccl_peer_tolerance_pct = 5.0
storage_peer_tolerance_pct = 10.0
dl_compute_tolerance_pct = 3.0
dl_numerical_tolerance_pct = 0.1
dl_overlap_tolerance_pct = 20.0
robust_z_threshold = 3.5
min_samples = 8
window_days = 30
dl_degraded_metric_fraction = 0.02
dl_min_degraded_metrics = 10
dl_degraded_severity_pct = 10.0
build_interval_seconds = 86400
classify_interval_seconds = 300
```

## Test switches

Each phase can be independently enabled or disabled:

```toml
[tests.storage]
enabled = true

[tests.nccl]
enabled = true

[tests.dltest]
enabled = true
```

These values are rendered into the pod as `RUN_STORAGE`, `RUN_NCCL`, and
`RUN_DLTEST`. A disabled phase is not executed and does not write metric rows.
Its structured result is `status="incomplete", enabled=false`; the aggregate
result is computed from enabled phases only. At least one phase must remain
enabled. Background baseline build/classification loops also skip disabled test
families.

`job_template.gpu_count` is the Kubernetes GPU reservation for the whole pod;
keep it at least as large as the `gpu_count` of each enabled GPU test.

The `[runtime]` `dl_results_root_path` points at remapped DL rank JSON artifacts
(`dltest-<node>-<timestamp>/workdir/test_plans/<plan>/runs/*.json`). The
`[storage]` `dl_*_db_path` entries point at the four DL metric DBs rebuilt from
those JSON files. The `[baseline]` root path, tolerances, `window_days`, DL
aggregation thresholds, and loop intervals control dynamic baseline building and
node classification (see [Baselines and Node Classification](baselines.md)).

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

Dry-run a job to confirm configured defaults:

```bash
python -m cval.cli run \
  --free-nodes slc01-cl02-hgx-0001 \
  --timestamp 12345 \
  --git-ref <commit-or-tag> \
  --output json
```