# Storage Validation Test

## Purpose

Runs six checked-in FIO profiles against the shared validation PVC to verify basic read/write correctness and collect IOPS and bandwidth evidence.

## Entrypoints

- `setup.sh` verifies `fio` and installs it with `apt-get` only when `install_fio` is enabled.
- `run-test.sh` is the canonical workload entrypoint.
- `storage.sh` is a compatibility wrapper for pinned jobs and older operator commands.

The framework calls setup and then the canonical runner. A zero runner exit code is raw `pass`; a non-zero exit is raw `fail`.

## Configuration

`test_config.toml` owns:

- `settings.install_fio`.
- Minimum resource requirements.
- Result and health database paths.
- Storage health combination factors and the low-is-bad tolerance.

The global config owns only `[tests.storage].enabled` and `config_path`.

## Workload and artifacts

FIO profiles live under `fio_jobs/`. Each profile writes JSON below the framework-provided `STORAGE_OUTPUT_DIR`. The runner writes a compact text summary to `STORAGE_SUMMARY_FILE` when `jq` is available; raw JSON remains authoritative for metric ingestion.

`plugin.py` implements `cval.plugin.v1` config, ingestion, and health
capabilities. For
a passing current run it validates the confined artifact tree, requires exactly
the six FIO JSON files, parses twelve finite non-negative IOPS/bandwidth fields,
and writes one immutable `storage_performance` row plus a durable receipt in the
framework-owned transaction. Failed/incomplete runs retain only their common
raw row. The canonical DB is `validation_tests/storage/storage_results.db`.

Canonical writes remain disabled while
`storage.per_test_ingestion_enabled=false`; the existing
`metadata/test-storage.db` compatibility writer remains the production surface.

## Health methodology

Storage health policy `storage.health.v1` expands the twelve persisted FIO
columns into independent metrics. Each result contributes exactly one stable
sample key per expanded metric. Throughput and IOPS are `low_bad`: higher values
are better. The tolerance floor is 10%; U8 reuses robust median/MAD statistics,
stratifies by image/CUDA/PyTorch, requires eight qualifying results and ten new
results for another candidate, and uses framework aggregation
`max_metric_class.v1`.

U8 candidates remain candidate-first and `auto_activate=false`. No live storage
health DB/evaluator is enabled; existing compatibility baselines remain the
operational classification surface until separately approved migration/U9 work.

## Troubleshooting

- Setup failure: verify `fio`, `apt-get`, and image permissions.
- Missing profiles: verify `CVAL_VALIDATION_TESTS_DIR` points at the pinned checkout.
- Missing summary with valid raw JSON: install `jq`; raw metrics can still be inspected.
- FIO failure: inspect the per-profile JSON and `STORAGE_LOG_FILE`.
