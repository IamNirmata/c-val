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
- The summary filename and baseline/export plugin capabilities.

The global config owns only `[tests.storage].enabled` and `config_path`.

## Workload and artifacts

FIO profiles live under `fio_jobs/`. Each profile writes JSON below the framework-provided `STORAGE_OUTPUT_DIR`. The runner writes a compact text summary to `STORAGE_SUMMARY_FILE` when `jq` is available; raw JSON remains authoritative for metric ingestion.

`db-update.sh` validates the six FIO JSON files and writes the current
`metadata/test-storage.db` `storage_performance` row. `plugin.py` supplies
test-owned configuration, baseline, and export hooks; it does not create a
second framework database.

## Health methodology

`cval.baselines` expands the twelve persisted FIO columns into independent
low-is-bad metrics. It builds robust median/MAD candidates, uses a 10% tolerance
floor, and classifies nodes as normal, degraded, or improved.

## Troubleshooting

- Setup failure: verify `fio`, `apt-get`, and image permissions.
- Missing profiles: verify `CVAL_VALIDATION_TESTS_DIR` points at the pinned checkout.
- Missing summary with valid raw JSON: install `jq`; raw metrics can still be inspected.
- FIO failure: inspect the per-profile JSON and `STORAGE_LOG_FILE`.
