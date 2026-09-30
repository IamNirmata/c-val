# Storage validation test

Runs six FIO profiles from [test_config.toml](test_config.toml) against the shared validation PVC. Setup
verifies `fio`; [run-test.sh](run-test.sh) executes the workload and retains each JSON result
under `STORAGE_OUTPUT_DIR`. Each FIO child enters that reserved directory before
execution and uses relative data/output names. Its launcher may close inherited
descriptors, so child arguments must not depend on `/proc/self/fd/...` aliases.
The scratch `--directory=fio-data` option precedes filename options, keeping
workload data under the run-private directory removed by the existing exit trap.
JSON artifacts remain alongside the summary. This does not delete data left by
historical runs.

[summary.py](summary.py) creates the JSON metric summary without `jq`.
[logger.py](logger.py) owns the test's consolidated logging hook and reuses the
shared safe append implementation; it also formats setup/workload stream lines. New runs keep `job.log`, `summary.json` and
the six FIO JSON files together under
`/data/continuous_validation/logs/storage/<node>/<run-id>/`.
No separate per-test result or stream/event files are generated.

For a passing phase, the registered [ingest.py](ingest.py) validates all six FIO
artifacts against the declared summary and writes one authorized raw
`metadata/test-storage.db` row containing IOPS and bandwidth measurements.
[plugin.py](plugin.py) owns setup/FIO validation, runtime/evidence environment
projection, raw store declaration, ingestion dispatch and read-only export.
The shared coordinator validates the returned receipt before publishing status.

Troubleshooting:

- verify the pinned image can provide `fio`;
- verify `CVAL_VALIDATION_TESTS_DIR` points at the exact checkout;
- inspect per-profile JSON and `STORAGE_LOG_FILE` for workload failures.