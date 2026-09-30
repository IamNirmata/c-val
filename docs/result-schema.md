# Result and raw database schema

## Output and Ingestion Map

New-source Jobs keep **one job log, one final job result, and one directory per
test**. This is the compact source contract; activation still requires separate
exact-commit cluster acceptance. Historical PVC files are not moved or deleted.

The paths below are inside the PVC-mounted pod, not the local checkout.
`<run-id>` is `<node>-<epoch-seconds>`; `<plan>` defaults to `80gb-example`.
Every run uses its frozen configuration, which can override these defaults.

### Files and Database Writes

| Belongs to | Data | Full PVC path (default) | Writer and timing |
| --- | --- | --- | --- |
| Job | Combined child output and `CVAL_EVENT` notifications | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/job.log` | [supervisor.py](../cval/validation/supervisor.py) captures merged stdout/stderr throughout the run. |
| Job | Final execution, log, file and database completion record | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/result.json` | Supervisor publishes it once, **after** ingestion acknowledgement and final log/file checks. |
| Storage | Consolidated setup/workload output and test events | `/data/continuous_validation/logs/storage/<node>/<run-id>/job.log` | Test-owned [logger.py](../validation-tests/storage/logger.py), through the registry logging hook. |
| Storage | JSON summary of all 12 FIO measurements | `/data/continuous_validation/logs/storage/<node>/<run-id>/summary.json` | Test-owned [summary.py](../validation-tests/storage/summary.py); no `jq` dependency. |
| Storage | Six raw FIO JSON artifacts | `/data/continuous_validation/logs/storage/<node>/<run-id>/randread.json`<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/randwrite.json`<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/iodepth_read_1file.json`<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/iodepth_write_1file.json`<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/numjobs_read_nfiles.json`<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/numjobs_write_nfiles.json` | FIO launched by [run-test.sh](../validation-tests/storage/run-test.sh). Required by the existing raw writer and exact verifier. |
| Storage | One raw metric row | `/data/continuous_validation/metadata/test-storage.db`, table `storage_performance` | [ingest.py](../cval/storage/ingest.py), `add_storage_result`; passing storage only. |
| NCCL | Consolidated setup/workload output and test events | `/data/continuous_validation/logs/nccl/<node>/<run-id>/job.log` | Test-owned [logger.py](../validation-tests/nccl/logger.py). |
| NCCL | Validated AllReduce and HCA summary | `/data/continuous_validation/logs/nccl/<node>/<run-id>/summary.json` | Test-owned [summary.py](../validation-tests/nccl/summary.py), followed by secure publication. |
| NCCL | Retained InfiniBand samples | `/data/continuous_validation/logs/nccl/<node>/<run-id>/ibbw-<run-id>.log` | [ibbw.sh](../validation-tests/nccl/ibbw.sh), when sampling is enabled. This is a raw measurement artifact, not a duplicate stream log. |
| NCCL | One consolidated raw row | `/data/continuous_validation/metadata/test-nccl.db`, table `IB_HEALTH` | [ingest.py](../cval/storage/ingest.py), `add_nccl_health_from_summary`; passing NCCL only. |
| DL | Consolidated setup/workload output and test events | `/data/continuous_validation/logs/dltest/<node>/<run-id>/job.log` | Test-owned [logger.py](../validation-tests/dltest/logger.py). |
| DL | Rank coverage and task-completion summary | `/data/continuous_validation/logs/dltest/<node>/<run-id>/summary.json` | Test-owned [summary.py](../validation-tests/dltest/summary.py). |
| DL | Run-local source, selected plan and optional baseline | `/data/continuous_validation/logs/dltest/<node>/<run-id>/workdir/` | [run-test.sh](../validation-tests/dltest/run-test.sh) copies and checks the configured external PVC source. |
| DL | Original rank measurements | `/data/continuous_validation/logs/dltest/<node>/<run-id>/workdir/test_plans/<plan>/runs/*.json` | External `dl_unit_test` package, launched with `torchrun`. |
| DL numerical | Raw numerical rows | `/data/continuous_validation/metadata/dltest_numerical_correctness.db`, table `numerical_correctness` | [dltest_ingest.py](../cval/storage/dltest_ingest.py), `ingest_dltest_run`; passing DL only. |
| DL compute | Raw compute measurements | `/data/continuous_validation/metadata/dltest_compute_performance.db`, table `compute_performance` | Same authorized DL ingestion. |
| DL collective | Raw collective measurements | `/data/continuous_validation/metadata/dltest_collective_performance.db`, table `collective_performance` | Same authorized DL ingestion. |
| DL overlap | Raw overlap measurements | `/data/continuous_validation/metadata/dltest_overlap_performance.db`, table `overlap_performance` | Same authorized DL ingestion. |
| DL ingestion | Per-run receipts and generation state | Each of the four DL DBs above: `cval_ingested_runs` and `cval_ingest_metadata` | Existing DL writer, under its shared persistent lock. |
| Job and built-in tests | Storage, NCCL, DL and aggregate `all` outcomes | `/data/continuous_validation/metadata/validation.db`, table `runs` | [run_ingest.py](../cval/storage/run_ingest.py) invokes `add_validation_run_results` after required metric writes. |
| Raw reporting | Latest-row SQL views | `/data/continuous_validation/metadata/validation.db`: `latest_status`<br/>`/data/continuous_validation/metadata/test-nccl.db`: `LATEST_NODE_STATUS` | Existing schema helpers; these are views, not extra output files. |

The test directories own `setup.sh`, `test_config.toml`, `run-test.sh`,
`logger.py` and `summary.py`. Logger hooks reuse the shared safe append primitive
and process supervision instead of duplicating that safety code. The seven raw
DB schemas and their writers remain unchanged apart from recognizing the new
artifact paths. A new database design and test-owned SQL adapters are separate work.

### Control Records and Temporary Files

- The job's `.run-active` marker still reserves its run identity. It is removed
  on normal completion; a leftover marker is evidence to investigate.
- The supervisor passes an anonymous Linux memory-file descriptor to the runner.
  It seals that execution result before starting ingestion, so each writer can
  revalidate immutable inputs without an intermediate PVC result or digest file.
- Storage removes only its run-local `fio-data/` scratch directory. Raw FIO JSON remains.
- NCCL removes its pod-local staging files after publishing the summary. Its
  retained HCA sample log remains available for exact raw verification.
- `/data/continuous_validation/metadata/.dl-metric-ingest.lock` remains a
  persistent shared writer lock and must not be deleted during operation.

### Shared Write Order

```text
Storage -> NCCL -> DL, retaining test logs, summaries and artifacts
  -> seal execution result in memory
  -> authorized storage metrics
  -> authorized NCCL metrics
  -> locked DL metrics and receipts
  -> built-in outcome rows in validation.db
  -> verify ingestion_finished acknowledgement
  -> check test files and finish/fsync job.log
  -> publish the final result.json once
```

The [supervisor](../cval/validation/supervisor.py) directly invokes the Python
runner and ingestion coordinator. The existing shell compatibility launchers do
not add production stages. Standalone runner execution is test-only and does not
publish a final job receipt or perform ingestion.

Ordinary benchmark failure does not prevent later selected tests from running.
Passing tests require their metric writes; failed/disabled tests do not get
fabricated metrics. Failure evidence can be fully recorded without becoming a
passing run. There is no transaction across all seven stores: earlier writes can
remain after a later failure. No database migration or PVC cleanup is implicit.

### Host-Side Outputs Are Separate

The controller retains session intents, observations and cycle history under
`live.log_dir`. [status.py](../cval/storage/status.py) produces the two public
status CSVs under `live.status_dir`. Requested result exports are local files
written by [results_export.py](../cval/storage/results_export.py). None of these
outputs is an additional per-test raw database.

## Canonical result envelope

New runs use `cval.results.v3`. The top-level result retains node/run identity,
source and configuration digests, reported software versions, test outcomes,
errors and execution times. Each test has one `log`, `summary` and `artifacts`
location, plus its phase, selection, timings and exit code. The artifacts path
is the test directory itself; there is no separate per-test result file.

The supervisor adds a `completion` object:

| Field | Meaning |
| --- | --- |
| `finished_at`, `exit_code` | Final supervisor completion time and infrastructure exit code |
| `logs` | `retained` or `unconfirmed`; successful stream capture is required |
| `files` | Per-test `complete`, `incomplete`, `not_required` or `unconfirmed` file state |
| `ingestion` | `pass`, `fail`, `unconfirmed`, `not_started` or `not_requested` |
| `stores` | Existing per-store `pending`, `committed`, `not_required` or `unconfirmed` acknowledgements |
| `result_digest` | Digest of the sealed execution result, independent of this later completion object |

The public file is atomically published last, without replacement. During a run,
progress comes from `CVAL_EVENT` lines. A recoverable failure can still produce
a final JSON with non-success completion. An early crash, SIGKILL or invalid path
may prevent final publication; missing JSON is unconfirmed, never a pass.

`overall` remains the test outcome, not a combined ingestion verdict. A passing
test with failed ingestion is therefore distinguishable from a failed test with
successfully retained failure evidence. Exact acceptance also compares artifact
values with raw rows and checks the terminal Job/pod identity and zero restarts.

## Compact runtime events

Events retain the `cval.event.v1` schema and begin on their own line with
`CVAL_EVENT `. They are written into `job.log` and forwarded to the container
console. Test events also appear in that test's single log. Benchmark stdout and
stderr receive test/stage labels, rather than creating separate stream files.

| Event | Producer and meaning |
| --- | --- |
| `supervisor_started` | Supervisor: run/config identity and selected tests |
| `run_started`, test milestones, `run_finished` | Runner: execution state, not DB completion |
| `result_finalized` | Supervisor: the in-memory execution result is sealed; the public JSON is not yet written |
| `ingestion_started`, `ingestion_store_finished` | Coordinator: current per-store write state |
| `ingestion_finished` | Coordinator: result/config-bound acknowledgement of required writes |
| `supervisor_stage_finished` | Supervisor: child exit and elapsed time |
| `supervisor_finished` | Supervisor: terminal tests, capture and ingestion state, before final JSON publication |

An exit code or a printed completion message is not an ingestion receipt. The
supervisor requires matching run/result/config identity and the expected store
outcomes. Exact verification reads these events from the consolidated log and
checks their order against the final receipt and raw evidence.

### Timing Interpretation

Test `duration_ms` includes setup, workload and runner overhead. Setup-to-start
and start-to-finish timestamps provide approximate wall-clock intervals.
`ingestion_finished.duration_ms` measures coordinator elapsed time, while
`supervisor_stage_finished.duration_ms` includes capture and cleanup. Nested
timings are not additive. Missing or interrupted intervals are not zero-duration
successes.

## Raw databases

The seven authoritative files remain under `/data/continuous_validation/metadata/`.
The coordinator and writers retain identity/path authorization, status-last
publication, SQLite retry bounds and DL generation checks. The normalized DB and
evaluation design are not activated by this logging change.

## Historical compatibility

Existing `cval.results`, `cval.results.v1` and `cval.results.v2` readers remain.
Historical stdout/stderr logs, JSON-lines events, environment results, per-test
results, text summaries and `.ingestion-result-digest` files stay on the PVC.
They may still be read when verifying an older run, but new Jobs do not generate
them. Older `logs/validation_tests/...` paths and documented relocation rules
remain supported for historical reads; no files are moved automatically.

Old source modules that import NCCL summary helpers and the former DL summary
entrypoint have small compatibility shims to the test-owned implementations.
Already pinned sessions keep their old source/configuration until an explicitly
approved turnover. Publication is not cluster acceptance or activation.