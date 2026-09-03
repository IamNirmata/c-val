# Storage validation test

Runs six checked-in FIO profiles against the shared validation PVC. Setup
verifies `fio`; `run-test.sh` executes the workload and retains each JSON result
under `STORAGE_OUTPUT_DIR`.

For a passing phase, `db-update.sh` validates all six artifacts and writes one
raw `metadata/test-storage.db` row containing IOPS and bandwidth measurements.
The plugin validates `install_fio` and provides read-only raw export rows.

Troubleshooting:

- verify the pinned image can provide `fio`;
- verify `CVAL_VALIDATION_TESTS_DIR` points at the exact checkout;
- inspect per-profile JSON and `STORAGE_LOG_FILE` for workload failures.