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
image_name = "pytorch:26.05-py3"
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

[validation]
gpu_count = 8
nccl_iterations = 20
nccl_data_size_gb = 8
ibbw_start_device = 0
ibbw_end_device = 13
dl_test_plan = "80gb-example"
dl_baseline_test_id = "b200-pt2.8.0-cuda12.9"
dl_iterations = 20

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
classify_outliers = true
robust_z_threshold = 3.5
min_samples = 8
window_days = 30
build_interval_seconds = 86400
classify_interval_seconds = 300
```

The `[storage]` `dl_*_db_path` entries point at the four DL metric DBs; the
`[baseline]` root path, tolerances, `window_days`, and loop intervals control
dynamic baseline building and node classification (see
[Baselines and Node Classification](baselines.md)).

## Precedence

`job.image_name` is the human-readable image identity written into job names,
result JSON, and SQLite rows. It should normally match the trailing image and
tag from `job_template.container_image`; if omitted, c-val derives it from the
container image reference.

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