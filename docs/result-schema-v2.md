# Structured Result Schema v2

**Schema:** `cval.results.v2`  
**Implementation status:** Emitted by the U5 generic runner; consumed by the locally validated U6 run-history writer and U7 registry-driven per-test ingestion dispatcher  

The JSON, logs, summaries, and artifacts described here remain shared evidence
under `runtime.validation_root`. Canonical U7/U8 SQLite state is resolved
separately under `health_evaluator.state_root`; no result path is rewritten to
the evaluator-owned tree.
**Legacy schema:** `cval.results.v1` remains readable and is documented in [Result Schema](result-schema.md).

`cval.results.v2` represents one c-val run with an arbitrary explicit registry of validation tests. It removes fixed storage/NCCL/DL result slots while preserving deterministic raw status and atomic partial-run recovery.

## Canonical location

```text
/data/continuous_validation/logs/job_logs/<node>/<run-id>/result.json
```

A compatibility copy or pointer may temporarily remain under the current `results/<node>/` path during migration.

The in-pod supervisor may hand the runner `/proc/self/fd/<fd>` paths backed by
retained directory descriptors for all physical writes. Those implementation
paths are never serialized. Every path in this schema remains the canonical
validation-root path shown below, preserving readers and exact compatibility
projections while eliminating pathname-reopen TOCTOU for run evidence.

## Example

```json
{
  "schema_version": "cval.results.v2",
  "run_id": "slc01-cl02-hgx-0204-1785273600",
  "node": "slc01-cl02-hgx-0204",
  "timestamp": 1785273600,
  "timestamp_la": "2026-07-28T09:00:00-07:00",
  "generated_at": "2026-07-28T16:08:31.123456Z",
  "completed_at": "2026-07-28T16:08:31.100000Z",
  "overall": "fail",
  "image_name": "pytorch:26.05-py3",
  "pytorch_version": "2.8.0a0+gitabc123",
  "cuda_version": "12.9",
  "git_ref": "0123456789abcdef0123456789abcdef01234567",
  "global_config_digest": "sha256:9a71...",
  "tests": {
    "storage": {
      "display_name": "Storage FIO",
      "enabled": true,
      "selected": true,
      "order": 10,
      "status": "pass",
      "phase": "finished",
      "started_at": "2026-07-28T16:00:10.000000Z",
      "completed_at": "2026-07-28T16:01:40.000000Z",
      "duration_ms": 90000,
      "exit_code": 0,
      "config_path": "validation-tests/storage/test_config.toml",
      "config_digest": "sha256:8e30...",
      "stdout": "/data/continuous_validation/logs/storage/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/stdout.log",
      "stderr": "/data/continuous_validation/logs/storage/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/stderr.log",
      "log": "/data/continuous_validation/logs/storage/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/events.jsonl",
      "summary": "/data/continuous_validation/validation_tests/storage/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/summary.json",
      "result": "/data/continuous_validation/validation_tests/storage/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/result.json",
      "artifacts": "/data/continuous_validation/validation_tests/storage/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/artifacts",
      "message": ""
    },
    "nccl": {
      "display_name": "NCCL and HCA health",
      "enabled": true,
      "selected": true,
      "order": 20,
      "status": "fail",
      "phase": "finished",
      "started_at": "2026-07-28T16:01:40.100000Z",
      "completed_at": "2026-07-28T16:03:00.100000Z",
      "duration_ms": 80000,
      "exit_code": 1,
      "config_path": "validation-tests/nccl/test_config.toml",
      "config_digest": "sha256:a642...",
      "stdout": "/data/continuous_validation/logs/nccl/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/stdout.log",
      "stderr": "/data/continuous_validation/logs/nccl/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/stderr.log",
      "log": "/data/continuous_validation/logs/nccl/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/events.jsonl",
      "summary": "/data/continuous_validation/validation_tests/nccl/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/summary.json",
      "result": "/data/continuous_validation/validation_tests/nccl/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/result.json",
      "artifacts": "/data/continuous_validation/validation_tests/nccl/runs/slc01-cl02-hgx-0204/slc01-cl02-hgx-0204-1785273600/artifacts",
      "message": "all-reduce process exited with code 1"
    },
    "dltest": {
      "display_name": "Deep-learning unit test",
      "enabled": true,
      "selected": false,
      "order": 30,
      "status": "incomplete",
      "phase": "not_selected",
      "started_at": null,
      "completed_at": null,
      "duration_ms": null,
      "exit_code": null,
      "config_path": "validation-tests/dltest/test_config.toml",
      "config_digest": "sha256:d20b...",
      "stdout": "",
      "stderr": "",
      "log": "",
      "summary": "",
      "result": "",
      "artifacts": "",
      "message": "not selected for this run"
    }
  },
  "errors": []
}
```

## Top-level fields

| Field | JSON type | Required | Rules |
| --- | --- | --- | --- |
| `schema_version` | String | Yes | Exactly `cval.results.v2`. |
| `run_id` | String | Yes | Non-empty stable run identifier; matches run-history key. |
| `node` | String | Yes | Non-empty target node. |
| `timestamp` | Integer | Yes | UTC epoch at run creation. |
| `timestamp_la` | String | Yes | ISO-8601 America/Los_Angeles representation of `timestamp`. |
| `generated_at` | String | Yes | UTC RFC 3339 timestamp for the most recent atomic write. |
| `completed_at` | String or null | Yes | UTC RFC 3339 when all selected tests reached terminal state; otherwise null. |
| `overall` | String | Yes | `pass`, `fail`, or `incomplete`; computed by the framework. |
| `image_name` | String | Yes | Validation container identity; may be empty only for imported historical data. |
| `pytorch_version` | String | Yes | Best-effort detected version; empty is allowed. |
| `cuda_version` | String | Yes | Best-effort detected version; empty is allowed. |
| `git_ref` | String | Yes | Requested or resolved runtime Git ref. Production writers should record the resolved commit. |
| `global_config_digest` | String | Yes | `sha256:<hex>` digest of canonical effective global config. |
| `tests` | Object | Yes | Map keyed by registered test ID. Must not be empty. |
| `errors` | Array of objects | Yes | Framework-level errors, empty when none. |

Unknown top-level fields are rejected by the v2 reader unless a later minor-version policy explicitly permits extension fields under a dedicated `extensions` object.

## Per-test fields

| Field | JSON type | Required | Rules |
| --- | --- | --- | --- |
| `display_name` | String | Yes | Non-empty operator-facing name from validated config. |
| `enabled` | Boolean | Yes | Global activation state at run creation. |
| `selected` | Boolean | Yes | Whether this enabled test was selected for this run. Disabled implies not selected. |
| `order` | Integer | Yes | Non-negative execution order from validated config. |
| `status` | String | Yes | `pass`, `fail`, or `incomplete`. |
| `phase` | String | Yes | One of the phases below. |
| `started_at` | String or null | Yes | UTC RFC 3339; null before execution. |
| `completed_at` | String or null | Yes | UTC RFC 3339; null before terminal state. |
| `duration_ms` | Integer or null | Yes | Non-negative; present only after start and completion. |
| `exit_code` | Integer or null | Yes | Process exit code when an entrypoint was launched. |
| `config_path` | String | Yes | Repository-relative validated path. |
| `config_digest` | String | Yes | `sha256:<hex>` digest of the test config used. |
| `stdout` | String | Yes | Assigned stdout path or empty before selection/execution. |
| `stderr` | String | Yes | Assigned stderr path or empty before selection/execution. |
| `log` | String | Yes | Assigned structured/combined log path or empty. |
| `summary` | String | Yes | Assigned summary path or empty when none exists. |
| `result` | String | Yes | Assigned test-specific result path or empty. |
| `artifacts` | String | Yes | Assigned artifacts directory or empty. |
| `message` | String | Yes | Concise failure/skip explanation; empty on ordinary success. |

The test key must satisfy the test ID regex and equal the associated registry ID.

## Test phases

| Phase | Terminal | Required status semantics |
| --- | --- | --- |
| `not_selected` | Yes | `incomplete`; `selected=false`. |
| `pending` | No | `incomplete`; selected but setup has not started. |
| `setup` | No | `incomplete`; setup process active. |
| `running` | No | `incomplete`; workload active. |
| `finished` | Yes | `pass` for exit 0, otherwise `fail`, subject to valid summary checks. |
| `setup_failed` | Yes | `fail`; setup returned non-zero. |
| `timed_out` | Yes | `fail`; process exceeded configured timeout. |
| `interrupted` | Yes | `incomplete` unless the test had already produced a validated terminal result. |
| `framework_error` | Yes | `fail` when selected execution could not be completed. |

A disabled test is represented as `enabled=false`, `selected=false`, `status=incomplete`, and `phase=not_selected` when the writer includes all registered tests. Writers must include every test from the effective registry so the run configuration is recoverable.

## Aggregate status

Define the participating tests as those where both `enabled` and `selected` are true.

- No participating tests: `overall="incomplete"`.
- Every participating test has terminal `status="pass"`: `overall="pass"`.
- Any participating test has `status="fail"`: `overall="fail"`.
- Otherwise: `overall="incomplete"`.

A writer cannot claim `pass` while a participating test is non-terminal, failed, timed out, or in framework error.

Tests that are enabled but intentionally not selected do not affect aggregate status. The run-history record still preserves that selection.

## Framework error objects

Each error object contains:

| Field | Type | Rules |
| --- | --- | --- |
| `code` | String | Stable lowercase identifier. |
| `message` | String | Concise operator-safe text. |
| `test_id` | String or null | Test attribution when applicable. |
| `timestamp` | String | UTC RFC 3339. |
| `detail_path` | String | Path to detailed logs, or empty. |

Secrets, full environment dumps, tokens, and kubeconfig data are prohibited.

## Atomic write contract

The framework writes the result after:

1. Run initialization.
2. Every test phase transition.
3. Every test completion/failure/timeout.
4. Framework-level error capture.
5. Final aggregate computation.

Write sequence:

1. Serialize complete JSON to a temporary file in the same directory.
2. Flush and close the temporary file.
3. Atomically replace `result.json`.
4. Do not update run-history state beyond what the durable result file represents.

A reader either sees the previous complete document or the next complete document, never a partial overwrite.

## Canonical digest rules

Configuration digests use SHA-256 over canonical JSON generated from parsed effective TOML data:

- UTF-8.
- Object keys sorted lexicographically.
- Compact separators.
- No comments or source formatting.
- Repository-relative paths remain relative in the digest input.
- No secrets are allowed in config.

Digest format is lowercase `sha256:<64 hexadecimal characters>`.

## Framework per-test result and test-owned summary

After a test becomes terminal, the framework writes `cval.test-result.v1` at
the assigned result path:

```json
{
  "schema_version": "cval.test-result.v1",
  "test_id": "nccl",
  "status": "pass",
  "phase": "finished",
  "started_at": "2026-07-28T16:01:40.100000Z",
  "completed_at": "2026-07-28T16:03:00.100000Z",
  "duration_ms": 80000,
  "exit_code": 0,
  "summary": "/data/continuous_validation/validation_tests/nccl/runs/node/run/summary.json",
  "artifacts": "/data/continuous_validation/validation_tests/nccl/runs/node/run/artifacts",
  "message": ""
}
```

Rules:

- `test_id` must match the running test.
- Status must agree with exit behavior; a non-zero process cannot claim pass.
- Test workloads write only the assigned summary and artifacts; they do not
  overwrite the framework result file.
- Test-owned summaries and framework results do not contain derived health classes.
- Missing summary does not automatically fail a pass/fail-only test; tests declaring ingestion or health capabilities may require one.

## Structured progress event schema

Each framework event is one JSON object prefixed by `CVAL_EVENT ` in the combined log and also written as JSONL without the prefix to `events.jsonl`.

Common fields:

```json
{
  "schema_version": "cval.event.v1",
  "event": "test_finished",
  "run_id": "slc01-cl02-hgx-0204-1785273600",
  "test": "nccl",
  "timestamp": "2026-07-28T16:03:00.100000Z",
  "status": "fail",
  "message": "all-reduce process exited with code 1"
}
```

Required event types:

- `run_started`
- `test_setup_started`
- `test_started`
- `test_finished`
- `test_skipped`
- `test_timed_out`
- `ingestion_started`
- `ingestion_finished`
- `run_finished`

The targeted validator derives progress from events, not test-specific English log messages.

## Validation invariants

The reader must reject:

- Unsupported schema version.
- Empty tests object.
- Invalid test IDs, statuses, phases, timestamps, or digests.
- Disabled but selected tests.
- Non-selected tests with pass/fail status.
- A pass with a non-zero exit code.
- A finished terminal test without completion time.
- Negative duration.
- Completion before start.
- Duplicate execution order among selected tests.
- Path fields containing NUL.
- Aggregate status inconsistent with participating tests.

The reader does not require referenced files to still exist. Historical artifact inspection must distinguish schema validity from artifact availability.

## v1 compatibility

Existing `cval.results.v1` remains accepted by a dedicated v1 parser.

Mapping for compatibility views:

| v1 | v2 compatibility value |
| --- | --- |
| `node` | `node` |
| `timestamp` string | Parsed integer `timestamp` where valid. |
| Top-level image/PyTorch/CUDA fields | Same top-level fields. |
| Fixed `tests.storage/nccl/dltest` | Dynamic map entries with those IDs. |
| `enabled` | Same. |
| No `selected` | Equal to `enabled`. |
| No phase | `finished` for pass/fail; `not_selected` for disabled incomplete; otherwise `interrupted`. |
| `log` and `summary` | Same available fields. |
| No run ID | Deterministically synthesize `<node>-<timestamp>` for read-only compatibility. |
| No digests/Git ref | Empty compatibility values; never presented as verified. |

Compatibility parsing does not rewrite historical files. The v2 JSON envelope
never contains `GCRRESULT1`, `GCRRESULT2`, or `GCRRESULT3`. The generic v2
runner still emits those exact values in the separate `result.env`
compatibility projection, and `cval result --output env` projects them for
`db-update.sh` and pinned consumers during the compatibility window. Dynamic
tests have no fixed `GCRRESULT*` slot; their authoritative status remains the
v2 `tests` map and structured events.

## Database ingestion boundary

- Run-history ingestion reads the complete v2 envelope.
- Per-test adapters receive only their own validated test result and assigned artifacts.
- Raw status is stored before or independently of metric ingestion.
- Ingestion failure is recorded as an ingestion error; it does not alter a valid raw execution status.
- Derived health class fields are absent from this raw result schema.
- The caller supplies the exact lowercase SHA-256 digest of the complete parsed
  envelope plus the immutable effective-config snapshot. Envelope-only changes
  therefore invalidate a retry even when one test object is unchanged.
- The dispatcher requires the result's test set, descriptor path/digest,
  enabled/selected state, order, global config digest, identity, and canonical
  evidence paths to match the snapshot before opening a writable database.
- A common `test_results` row is immutable by `run_id`. Passing tests with an
  `ingest` capability then run their adapter in an isolated spawn worker using
  a parent-owned SQLite transaction and durable receipt. Failed/interrupted
  tests retain raw status and do not ingest metrics.
- `storage.per_test_ingestion_enabled=false` is the production default. The
  preflight remains read-only, but canonical per-test DB creation/writes are
  skipped until separately approved. The U6 run-history gate is independent.
