# Result and raw database schema

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