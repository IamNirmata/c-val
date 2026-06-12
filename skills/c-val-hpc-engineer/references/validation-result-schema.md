# Validation Result Schema

c-val writes structured validation results to:

```text
/data/continuous_validation/results/<node>/cval-results-<node>-<timestamp>.json
```

Schema version:

```json
"cval.results.v1"
```

Example:

```json
{
  "schema_version": "cval.results.v1",
  "node": "slc01-cl02-hgx-0001",
  "timestamp": "12345",
  "generated_at": "2026-06-10T00:00:00Z",
  "overall": "fail",
  "tests": {
    "storage": {"status": "pass", "log": "...", "summary": "..."},
    "nccl": {"status": "fail", "log": "...", "summary": "..."},
    "dltest": {"status": "pass", "log": "...", "summary": "..."}
  }
}
```

Valid statuses:

- `pass`
- `fail`
- `incomplete`

The aggregate `overall` is `pass` only when all tests pass. Use this helper to inspect result JSON:

```bash
python -m cval.cli result --result-json <result.json>
```