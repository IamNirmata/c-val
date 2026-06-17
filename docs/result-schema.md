# Result Schema

c-val 2.0 writes one structured result JSON per node validation run.

Path:

```text
/data/continuous_validation/results/<node>/cval-results-<node>-<timestamp>.json
```

Schema version:

```text
cval.results.v1
```

## Example

```json
{
  "schema_version": "cval.results.v1",
  "node": "slc01-cl02-hgx-0204",
  "image_name": "pytorch:26.05-py3",
  "timestamp": "1781134840",
  "generated_at": "2026-06-10T23:47:42.103216Z",
  "overall": "pass",
  "tests": {
    "storage": {
      "status": "pass",
      "log": "/data/continuous_validation/storage/.../storage.log",
      "summary": "/data/continuous_validation/storage/.../storage-summary.txt"
    },
    "nccl": {
      "status": "pass",
      "log": "/data/continuous_validation/nccl/.../nccl.log",
      "summary": "/data/continuous_validation/nccl/.../nccl-summary.json"
    },
    "dltest": {
      "status": "pass",
      "log": "/data/continuous_validation/dltest/.../dltest.log",
      "summary": "/data/continuous_validation/dltest/.../dltest-summary.json"
    }
  }
}
```

## Rules

- Valid statuses: `pass`, `fail`, `incomplete`.
- `image_name` records the validation image identity used for the run.
- `overall` is `pass` only when all test statuses are `pass`.
- `db-update.sh` prefers JSON and falls back to the legacy env file if JSON is missing.
- DB writes use package-native `cval db-add-*` commands inside the validation pod and store `image_name` with validation, storage, and NCCL rows.
- `cval.validation.results` validates schema version, required tests, valid statuses, and aggregate consistency.

## DB Rows

Each run should write four latest-status rows:

```text
<node> storage <timestamp> <status> <image_name>
<node> nccl    <timestamp> <status> <image_name>
<node> dltest  <timestamp> <status> <image_name>
<node> all     <timestamp> <overall> <image_name>
```