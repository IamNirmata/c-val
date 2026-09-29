# Result and raw database schema

## Output and Ingestion Map

This table covers the built-in validation Job: storage, NCCL and DL. Here,
**file output** means writing logs or results to the PVC; **DB ingestion** means
reading those files and saving selected values in SQLite. The test scripts do
not directly write the SQLite databases.

The paths below use the default validation root. A run uses the paths in its
frozen configuration, not necessarily these defaults.

| Path name used below | Default PVC location |
| --- | --- |
| `ROOT` | `/data/continuous_validation` |
| `JOB` | `ROOT/logs/job_logs/<node>/<run-id>` |
| `TEST_LOG` | `ROOT/logs/<test>/<node>/<run-id>` |
| `TEST_RUN` | `ROOT/logs/validation_tests/<test>/runs/<node>/<run-id>` |
| `DB` | `ROOT/metadata` |

`<test>` is `storage`, `nccl` or `dltest`. `<run-id>` is
`<node>-<epoch-seconds>`. Node/run identity connects the files to the DB rows.

### Files and Database Writes

These are absolute paths **inside a pod that mounts the PVC at `/data`**, such as
the PVC access pod. They are not paths in the local Git checkout. Replace
`<node>`, `<run-id>` and `<plan>` with the selected run's values; the default DL
plan is `80gb-example`. SQLite table and view names identify objects inside the
listed DB file, not additional filesystem paths. This lists the current layout;
it does not assert that every artifact exists for a failed or historical run.

| Belongs to | Data type and contents | Full PVC path (default) | Code responsible for writing it | When it is written / what consumes it |
| --- | --- | --- | --- | --- |
| Job | Combined text log: runner and ingestion stdout/stderr, plus supervisor messages | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/job.log` | [supervisor.py](../cval/validation/supervisor.py#L846), `_run_child`; `_emit_supervisor_event` adds supervisor events | Streamed throughout execution and ingestion. Diagnostic evidence, not a DB row. |
| Job | Separate text logs: stdout and stderr | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/stdout.log`<br/>`/data/continuous_validation/logs/job_logs/<node>/<run-id>/stderr.log` | [supervisor.py](../cval/validation/supervisor.py#L846), `_run_child` | Same child streams as the combined log, kept separately; also forwarded to the container console. |
| Job | JSON-lines progress: run/test lifecycle, finalization, ingestion/store outcomes and supervisor completion | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/events.jsonl` | [execution.py](../cval/validation/execution.py#L68), `RunLogger.emit`, called by runner and ingestion; [supervisor.py](../cval/validation/supervisor.py#L379), `_emit_supervisor_event` | Appended at milestones. `CVAL_EVENT` copies also reach stdout. Used by tracking and exact verification. |
| Job | Canonical JSON: identity, source/config checksums, software versions, test states, timings, paths and overall outcome | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/result.json` | [runner.py](../cval/validation/runner.py#L674), `_write_state` | Replaced at state transitions and finalized before ingestion. Main structured input to write authorization and verification. |
| Job | Compatibility environment text: built-in outcome projection | `/data/continuous_validation/logs/job_logs/<node>/<run-id>/result.env` | [runner.py](../cval/validation/runner.py#L717), `_write_legacy_env` | Updated alongside the canonical JSON. Compatibility output, not sufficient DB-write authorization. |
| Every enabled test | Text logs: setup/workload stdout and stderr; combined workload stream with stream labels | Directories:<br/>`/data/continuous_validation/logs/storage/<node>/<run-id>/`<br/>`/data/continuous_validation/logs/nccl/<node>/<run-id>/`<br/>`/data/continuous_validation/logs/dltest/<node>/<run-id>/`<br/>Each contains `stdout.log`, `stderr.log` and `workload.log`. | [execution.py](../cval/validation/execution.py#L254), `RunLogger._pump`; `emit`/`message` also add per-test records | Written while setup and the workload run. Child output also reaches the job-level logs. |
| Every enabled test | JSON-lines test milestones | `/data/continuous_validation/logs/storage/<node>/<run-id>/events.jsonl`<br/>`/data/continuous_validation/logs/nccl/<node>/<run-id>/events.jsonl`<br/>`/data/continuous_validation/logs/dltest/<node>/<run-id>/events.jsonl` | [execution.py](../cval/validation/execution.py#L68), `RunLogger.emit`, called by [runner.py](../cval/validation/runner.py) | Setup, start and terminal test events. Used to explain failure, timing and completion. |
| Every executed test | Per-test JSON: phase, pass/fail/incomplete, exit code, duration, summary and artifact paths | `/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/result.json`<br/>`/data/continuous_validation/logs/validation_tests/nccl/runs/<node>/<run-id>/result.json`<br/>`/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/result.json` | [runner.py](../cval/validation/runner.py#L700), `_write_test_result` | On terminal test state, including setup failure or timeout. Must agree with the whole-run JSON. Disabled tests remain not-selected entries in the whole-run result instead. |
| Storage | Raw FIO JSON from six workloads: random read/write, queued single-file read/write and parallel multi-file read/write | `/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/randread.json`<br/>`/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/randwrite.json`<br/>`/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/iodepth_read_1file.json`<br/>`/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/iodepth_write_1file.json`<br/>`/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/numjobs_read_nfiles.json`<br/>`/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/artifacts/numjobs_write_nfiles.json` | FIO launched by [storage/run-test.sh](../validation-tests/storage/run-test.sh#L1) | Created per workload. These JSON files, not the readable summary, supply the storage DB metrics. |
| Storage | Human-readable bandwidth/IOPS summary | `/data/continuous_validation/logs/validation_tests/storage/runs/<node>/<run-id>/summary.txt` | [storage/run-test.sh](../validation-tests/storage/run-test.sh#L1), using `jq` and formatting commands | After FIO execution. Required as passing canonical evidence by exact verification; current generation is skipped if `jq` is missing. |
| Storage | SQLite metrics: one row with node/run/image and 12 bandwidth/IOPS values | `/data/continuous_validation/metadata/test-storage.db`<br/>Table: `storage_performance` | [run_ingest.py](../cval/storage/run_ingest.py#L110) calls [ingest.py](../cval/storage/ingest.py#L798), `add_storage_result` | Only for a passing storage outcome; parses the six FIO JSON files and validates authorized targets. |
| NCCL | Text samples of InfiniBand transmit counters/bandwidth | `/data/continuous_validation/logs/validation_tests/nccl/runs/<node>/<run-id>/artifacts/ibbw-<run-id>.log` | [nccl/ibbw.sh](../validation-tests/nccl/ibbw.sh), started/stopped by [nccl/run-test.sh](../validation-tests/nccl/run-test.sh#L1) | Sampled alongside AllReduce when bandwidth tracking is enabled. Retained samples are also printed into the workload output. |
| NCCL | JSON summary: AllReduce bandwidth/latency, run settings and HCA-port measurements | `/data/continuous_validation/logs/validation_tests/nccl/runs/<node>/<run-id>/summary.json` | [nccl/run-test.sh](../validation-tests/nccl/run-test.sh#L1) calls [nccl_summary.py](../cval/validation/nccl_summary.py); [secure_fs.py](../cval/validation/secure_fs.py) publishes the staged summary | After AllReduce completes and summary checks pass. This and the required HCA samples supply the NCCL DB write. |
| NCCL | SQLite metrics: one consolidated row with node/run/software context, bandwidth, latency and port measurements | `/data/continuous_validation/metadata/test-nccl.db`<br/>Table: `IB_HEALTH` | [run_ingest.py](../cval/storage/run_ingest.py#L110) calls [ingest.py](../cval/storage/ingest.py#L362), `add_nccl_health_from_summary` | Only for a passing NCCL outcome. Checks summary values and required HCA samples. The table name does not imply a health classification. |
| DL | Retained execution inputs: copied external package, selected plan and optional baseline | Package: `/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/artifacts/workdir/src/`<br/>Plan: `/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/artifacts/workdir/test_plans/<plan>/test_plan.json`<br/>Optional baseline: `/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/artifacts/workdir/test_plans/<plan>/baseline/` | [dltest/run-test.sh](../validation-tests/dltest/run-test.sh#L1), `prepare_workdir` | Copied from the configured PVC DL source before execution and compared with it. Inputs/provenance, not ingested metric rows. |
| DL | Raw rank JSON: numerical, compute, collective and overlap measurements | `/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/artifacts/workdir/test_plans/<plan>/runs/*.json` | External `dl_unit_test` package invoked by [dltest/run-test.sh](../validation-tests/dltest/run-test.sh#L1) with `torchrun` | Produced by the selected plan, one rank output per expected rank. Supplies the DL summary and all four metric stores. |
| DL | JSON summary: rank coverage, task completion, plan/settings, result paths and outcome | `/data/continuous_validation/logs/validation_tests/dltest/runs/<node>/<run-id>/summary.json` | [dltest/summarize_results.py](../validation-tests/dltest/summarize_results.py#L148), called by the test entrypoint | Created for success and attempted for failure. Validates expected ranks, matching plan and completed tasks; does not replace rank metrics. |
| DL numerical | SQLite raw numerical observations | `/data/continuous_validation/metadata/dltest_numerical_correctness.db`<br/>Table: `numerical_correctness` | [run_ingest.py](../cval/storage/run_ingest.py#L110) calls [dltest_ingest.py](../cval/storage/dltest_ingest.py#L1082), `ingest_dltest_run` then `write_standard_db` | Passing DL only. Reads/classifies rank JSON; row count depends on the plan and ranks. |
| DL compute | SQLite compute timings/measurements | `/data/continuous_validation/metadata/dltest_compute_performance.db`<br/>Table: `compute_performance` | [dltest_ingest.py](../cval/storage/dltest_ingest.py#L1082), `ingest_dltest_run` then `write_standard_db` | Same coordinated passing-DL write; not a separate test launch. |
| DL collective | SQLite collective-operation measurements | `/data/continuous_validation/metadata/dltest_collective_performance.db`<br/>Table: `collective_performance` | [dltest_ingest.py](../cval/storage/dltest_ingest.py#L1082), `ingest_dltest_run` then `write_standard_db` | Same coordinated passing-DL write. |
| DL overlap | SQLite compute/communication-overlap measurements | `/data/continuous_validation/metadata/dltest_overlap_performance.db`<br/>Table: `overlap_performance` | [dltest_ingest.py](../cval/storage/dltest_ingest.py#L1082), `ingest_dltest_run` then `write_overlap_db` | Same coordinated passing-DL write. |
| DL ingestion | SQLite receipts and publication generation, not benchmark metrics | `/data/continuous_validation/metadata/dltest_numerical_correctness.db`<br/>`/data/continuous_validation/metadata/dltest_compute_performance.db`<br/>`/data/continuous_validation/metadata/dltest_collective_performance.db`<br/>`/data/continuous_validation/metadata/dltest_overlap_performance.db`<br/>Inside each DB: `cval_ingested_runs` and `cval_ingest_metadata` | [dltest_ingest.py](../cval/storage/dltest_ingest.py), receipt helpers and `_write_ingest_generation` / `_finalize_dl_generation` | Binds ingested run keys to artifact directories and records a shared complete generation. Private migration bookkeeping may also exist. |
| Job and all built-in tests | SQLite outcomes: storage, NCCL, DL and aggregate `all`, with node/time/software context | `/data/continuous_validation/metadata/validation.db`<br/>Table: `runs` | [run_ingest.py](../cval/storage/run_ingest.py#L110) calls [ingest.py](../cval/storage/ingest.py#L673), `add_validation_run_results` | Atomic built-in status set written **last**, after every required metric stage succeeds. Failed/disabled test outcomes remain visible; they do not require invented metrics. |
| Job history / NCCL history | SQL views selecting latest records | `/data/continuous_validation/metadata/validation.db`<br/>View: `latest_status`<br/>`/data/continuous_validation/metadata/test-nccl.db`<br/>View: `LATEST_NODE_STATUS` | Schema helpers in [ingest.py](../cval/storage/ingest.py) | Views over stored rows, not separate ingestion jobs or copied result files. |

### Control Records and Temporary Files

These are part of the flow but are not additional test measurements.

| Scope | Record or temporary data | Owner | Meaning / lifetime |
| --- | --- | --- | --- |
| Job | `JOB/.run-active` | [supervisor.py](../cval/validation/supervisor.py), secure run reservation and `release_marker` | Protects run-path ownership; released on normal completion. A leftover marker needs investigation, not automatic deletion. |
| Job ingestion | `JOB/.ingestion-result-digest` | [write_provenance.py](../cval/storage/write_provenance.py#L188), `bind_ingestion_digest`, with [supervisor.py](../cval/validation/supervisor.py#L167), `bind_result` | Binds the exact finalized result. Its existence alone does not prove that any or all DB writes completed. |
| DL ingestion | `DB/.dl-metric-ingest.lock` | [run_ingest.py](../cval/storage/run_ingest.py#L110) using [dl-metric-lock.py](../scripts/dl-metric-lock.py) | Serializes live DL writers; preserve the persistent lock file. |
| Storage | `TEST_RUN/artifacts/fio-data/` | [storage/run-test.sh](../validation-tests/storage/run-test.sh#L1) | Scratch files used to exercise storage; removed on exit while raw FIO outputs remain. |
| NCCL | Pod-local temporary metrics/summary staging directory | [nccl/run-test.sh](../validation-tests/nccl/run-test.sh#L1); [single-node-allreduce.py](../validation-tests/nccl/single-node-allreduce.py) produces staged metrics | Intermediate files, cleaned up by the entrypoint. The published summary and HCA sample log are the retained artifacts. |

### Shared Write Order

The [supervisor](../cval/validation/supervisor.py#L639) starts the Python runner
and, after valid result finalization, directly starts
`python -m cval.storage.run_ingest`. The
[DB shell launcher](../validation-tests/db-update.sh) is a compatibility entry,
not another required production ingestion stage.

```text
Storage test -> NCCL test -> DL test -> finalized whole-run result
  -> authorize result/configuration and bind digest
  -> passing storage metrics
  -> passing NCCL metrics
  -> passing DL metrics + receipts/generation, under the shared DL lock
  -> built-in outcome rows in validation.db
  -> ingestion_finished acknowledgement
  -> supervisor completion and retained logs
```

Disabled tests are skipped. Ordinary test failures are retained and later enabled
tests still run; an infrastructure failure or interruption can prevent normal
finalization/ingestion. A failed selected test can therefore have **successfully
recorded failure evidence** without being a passing run.

There is no transaction spanning all seven DBs. Earlier metric writes can remain
when a later write fails. A `committed` acknowledgement can mean an identical row
already existed, not necessarily that a new row was inserted. Acceptance requires
the correct Job/pod, passing selected tests, readable canonical evidence and
matching exact-run raw records; console text or DB-file existence is insufficient.

### Host-Side Outputs Are Separate

These are not additional writes performed by the validation pod into raw DBs.

| Belongs to | Data/output | Responsible code | Location and trigger |
| --- | --- | --- | --- |
| Controller session | Durable intents/receipts, Job observations and session projections | [manager.py](../cval/jobs/manager.py), [cval-live-state.py](../scripts/cval-live-state.py), [cval-live.sh](../scripts/cval-live.sh) | Configured host `live.log_dir`; updated during planning, submission and tracking. |
| Reporting | Paired `node-status.csv` and `cluster-status.csv` | [status.py](../cval/storage/status.py), `refresh_status_csvs` / `project_status_csvs` | Configured host `live.status_dir`; scheduled refresh and session-event projection. These are derived views, not ingestion authority. |
| Operator export | Requested raw-result CSV snapshot | [cli.py](../cval/cli.py), `handle_results`, [results_export.py](../cval/storage/results_export.py) and test export plugins | Operator-selected local output directory, only when `cval results` is requested. Reads the databases; does not update them. |

This map documents the current source-level flow, not a new live audit. It does
not authorize deletion of apparently overlapping logs, summaries or artifacts:
they serve different diagnostic, compatibility and verification purposes.

## Canonical result envelope

New runs write `logs/job_logs/<node>/<run-id>/result.json`.

The `cval.results` object contains run identity, timestamps, image/framework
versions, exact Git ref, effective configuration digest, aggregate outcome,
errors, and a dynamic test map. Each test records selection state, order,
phase, `pass`/`fail`/`incomplete`, timing, exit code, descriptor identity, logs,
summary, artifacts, and message.

The runner atomically replaces the envelope at each transition. Readers still
accept historical `cval.results.v1` and `cval.results.v2` files without
rewriting them.

## Compact runtime events

Phase 2.1 adds milestones to the existing `cval.event.v1`/`CVAL_EVENT` stream.
Every record remains one JSON line, flushed to container stdout and retained in
`events.jsonl`; supervisor-owned events also use the reserved global log descriptors.
No new result schema, database or tracking service is introduced.

| Event | Producer and Meaning |
| --- | --- |
| `supervisor_started` | Supervisor: node/run, source/config identity, selected tests and canonical evidence paths. |
| `run_started`, test milestones, `run_finished` | Runner: test execution only; producer/node fields and elapsed times augment existing events. |
| `result_finalized` | Supervisor: canonical result is bound to its immutable digest before ingestion. |
| `ingestion_started` | Ingestion coordinator: required stores are pending; nonpassing/disabled metric stores are not required. |
| `ingestion_store_finished` | Ingestion coordinator: the writer returned successfully, or the store was not required. Includes destination and known row/rank/generation details. |
| `ingestion_finished` | Ingestion coordinator: final ingestion outcome, result/config digests, per-store state and elapsed time. |
| `supervisor_stage_finished` | Supervisor: runner/ingestion child exit and elapsed time, not a test verdict. |
| `supervisor_finished` | Supervisor: one final summary with test outcomes, result/log retention, ingestion outcome, store states and process exit. |

Store outcomes are `pending`, `committed`, `not_required` and `unconfirmed`.
`committed` means the authorized writer returned (possibly after matching an
existing identical row), not that a new row was necessarily inserted. Four DL
stores are acknowledged only after the writer completes generation finalization.
A failed DL write can have partial commits; its unconfirmed stores are not labeled
absent or rolled back. The large DL summary dump is replaced by compact milestones;
legacy completion messages and raw workload output remain available.

The supervisor requires the ingestion producer's acknowledgement for the same
node/run/result/config and the expected required stores. Exit zero alone does not
confirm ingestion. A final `status=pass` requires passing tests, no result errors,
successful infrastructure and confirmed ingestion. Test failure with successful
failure-evidence ingestion remains `status=fail`, `tests_outcome=fail`,
`ingestion_outcome=pass`; existing process-exit semantics are unchanged in this task.
Skipped ingestion is `not_requested`, never pass. Interrupted or failed log capture
is `logs_state=unconfirmed`. Missing final output after OOM/SIGKILL, failed reservation
or broken logging is unconfirmed and must be interpreted with Kubernetes status.

The supervisor forwards flushed child output without waiting for a 64 KiB buffered
read or EOF, and drains streams before normal finalization. Successful summary
publication follows canonical log fsync. A logging failure still fails the process;
no synthetic successful summary is supplied when evidence cannot be verified.

These are source changes pending separately approved exact-commit pod acceptance.
Historical event/result readers remain compatible, but old undigested ingestion
acknowledgements cannot authorize a new supervisor's success.

### Timing Interpretation

Use the existing event stream for the exact run, not a combined log tail:

| Measurement | Existing Evidence | Interpretation |
| --- | --- | --- |
| Test total | Result/test-finished `duration_ms` | Monotonic elapsed time across setup, workload and runner overhead. |
| Setup interval | `test_setup_started` to `test_started` timestamps | Wall-clock approximation including transition/logging overhead, not pure setup process time. |
| Workload interval | `test_started` to the matching terminal test event | Wall-clock approximation including summary/result/logging overhead, not kernel-only time. |
| Ingestion | `ingestion_finished.duration_ms` when present | Coordinator elapsed time; historical start/finish timestamps provide only a wall-clock approximation. |
| Outer stages | `supervisor_stage_finished.duration_ms` | Child lifetime plus capture/cleanup; not additive to the nested test/ingestion timings. |

Require matching run/test identities and ordered milestones. Missing, repeated,
out-of-order or interrupted boundaries are unconfirmed; do not fill missing phases
with zero or infer a pass from elapsed time. Timestamp subtraction can be affected
by wall-clock changes; prefer emitted monotonic totals when comparing totals.
Retain setup failure, timeout and test outcome separately from timing.

Image/framework values, selected test descriptors, iterations and plan identify
the experiment inputs already recorded by c-val. Different values are valid test
dimensions, not errors to normalize away. Compare environment experiments explicitly;
do not attribute differences to a framework refactor across different nodes, inputs
or unaccepted source revisions. Historical evidence cannot establish new-source
acceptance or a speedup. No new timing DB, fields or instrumentation are required
for this stage-level inspection.

## Raw databases

- `metadata/validation.db`: `runs` and `latest_status`;
- `metadata/test-storage.db`: one validated storage metric row per passing run;
- `metadata/test-nccl.db`: one consolidated `IB_HEALTH` metric row per passing
  NCCL run;
- four `metadata/dltest_*.db` files: raw component metrics from passing rank
  evidence.

The supervisor directly starts [cval.storage.run_ingest](../cval/storage/run_ingest.py);
[validation-tests/db-update.sh](../validation-tests/db-update.sh) remains a
compatibility launcher for the same coordinator. The coordinator authorizes structured
result/config provenance before calling test-specific writers; the writers
independently revalidate each write. A current run's ingestion digest remains
bound to its canonical directory through the supervisor's retained descriptor.
Required metric writes precede the atomic built-in status set. DL writes and
their retries share the persistent metadata-directory POSIX record lock.

The coordinator emits `ingestion_started`, per-store milestones and a terminal
`ingestion_finished` event through the existing logger. A metric-stage failure cannot publish final
status. Historical v1/v2 JSON remains readable, but environment-only result
files are not accepted as write authorization. Existing result.env outputs,
raw rows, receipts and evidence files are not rewritten by this refactor.

These schemas contain test outcomes and measurements only. They do not contain
framework-generated health classes, comparative verdicts, scores, or rankings.