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

`validation-tests/db-update.sh` validates result/config provenance and exact
database targets before writes. Required metric writes happen before the
atomic built-in status set. DL writes are serialized by the metadata-directory
lock.

These schemas contain test outcomes and measurements only. They do not contain
framework-generated health classes, comparative verdicts, scores, or rankings.