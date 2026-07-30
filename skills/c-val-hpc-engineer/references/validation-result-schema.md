# Validation Result Schema

c-val writes structured validation results to:

```text
/data/continuous_validation/logs/job_logs/<node>/<run-id>/result.json
```

Schema version:

```json
"cval.results.v2"
```

Example:

```json
{
  "schema_version": "cval.results.v2",
  "run_id": "slc01-cl02-hgx-0001-12345",
  "node": "slc01-cl02-hgx-0001",
  "timestamp": 12345,
  "generated_at": "2026-06-10T00:00:00Z",
  "overall": "fail",
  "tests": {
    "storage": {"enabled": true, "selected": true, "phase": "finished", "status": "pass"},
    "nccl": {"enabled": true, "selected": true, "phase": "finished", "status": "fail"},
    "dltest": {"enabled": true, "selected": true, "phase": "finished", "status": "pass"}
  },
  "errors": []
}
```

Valid statuses:

- `pass`
- `fail`
- `incomplete`

The test map is dynamic. The aggregate `overall` is `pass` only when all
enabled and selected tests pass. Historical `cval.results.v1` remains readable.
Use this helper to inspect either version:

```bash
python -m cval.cli result --result-json <result.json>
```