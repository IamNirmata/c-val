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

## Raw databases

- `metadata/validation.db`: `runs` and `latest_status`;
- `metadata/test-storage.db`: one validated storage metric row per passing run;
- `metadata/test-nccl.db`: one consolidated `IB_HEALTH` metric row per passing
  NCCL run;
- four `metadata/dltest_*.db` files: raw component metrics from passing rank
  evidence.

[validation-tests/db-update.sh](../validation-tests/db-update.sh) launches
[cval.storage.run_ingest](../cval/storage/run_ingest.py). It authorizes structured
result/config provenance before calling test-specific writers; the writers
independently revalidate each write. A current run's ingestion digest remains
bound to its canonical directory through the supervisor's retained descriptor.
Required metric writes precede the atomic built-in status set. DL writes and
their retries share the persistent metadata-directory POSIX record lock.

The coordinator emits `ingestion_started` and a terminal `ingestion_finished`
event through the existing logger. A metric-stage failure cannot publish final
status. Historical v1/v2 JSON remains readable, but environment-only result
files are not accepted as write authorization. Existing result.env outputs,
raw rows, receipts and evidence files are not rewritten by this refactor.

These schemas contain test outcomes and measurements only. They do not contain
framework-generated health classes, comparative verdicts, scores, or rankings.