# Result and database schema

## Canonical result envelope

New validation runs write one `cval.results` JSON object under:

`logs/job_logs/<node>/<run-id>/result.json`

The envelope contains run identity, timestamps, image/framework versions,
checked-out Git ref, effective configuration digest, aggregate status, errors,
and a dynamic test map. Each test records enabled/selected state, order, phase,
status, times, duration, exit code, descriptor identity, logs, summary, result,
artifacts, and message.

The runner validates and atomically replaces the envelope at each transition.
The emitted shape is the proven dynamic schema previously called v2; only the
public schema name changed. Readers continue to accept historical
`cval.results.v2` and fixed `cval.results.v1` artifacts. Historical files are
never rewritten or deleted.

## Current raw databases

- `metadata/validation.db` — `runs` plus `latest_status`, authoritative pass/fail;
- `metadata/test-storage.db` — storage metrics;
- `metadata/test-nccl.db` — consolidated `IB_HEALTH` metrics and current views;
- four `metadata/dltest_*.db` files — component metrics rebuilt from canonical
  rank JSON.

`validation-tests/db-update.sh` validates result/config provenance and writes
only these current surfaces.

## Baseline and classification databases

Baseline DBs store immutable baseline JSON plus lifecycle/provenance columns.
Classification rows store timestamp, node, target, baseline identity, verdict,
metric counts/fraction/worst deviation, and detailed metric JSON.

Classifications are stored in separate target files under `baselines/`; readers
merge the latest row for each `(node, test_type)`. See `baselines.md` for exact
filenames and split preparation.
