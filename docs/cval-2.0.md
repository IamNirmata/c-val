# c-val 2.0 Implementation Notes

This document tracks the first package-oriented refactor slice. The goal is to move c-val from a notebook-first prototype toward a dry-run-first Python package and CLI that Hermes can safely operate.

## Current Slice

Implemented package modules:

- `cval.k8s.discovery`: pure parsing for Kubernetes pod/node JSON and GPU free-node discovery.
- `cval.scheduler.priority`: stale-node priority queue construction.
- `cval.jobs.renderer`: validation job manifest rendering from the existing Volcano YAML template.
- `cval.orchestrator.workflow`: dry-run workflow planning from free nodes, validation history, and job template rendering.
- `cval.storage.ingest`: in-pod SQLite write helpers for result and metric ingestion.
- `cval.cli`: dry-run-first command surface.

The old notebook-first and `utils/functions.py` helper paths have been removed
from the active repository. Operators and Hermes should use `python -m cval.cli`
for orchestration and package-native DB ingestion.

Validation job scripts now persist per-test results through `CVAL_RESULT_JSON_FILE` with schema version `cval.results.v1`. `run-test.sh` writes storage, NCCL, and DL statuses after each phase, and `db-update.sh` records `storage`, `nccl`, `dltest`, and aggregate `all` rows based on those actual results instead of writing unconditional `all/pass`. `CVAL_RESULT_ENV_FILE` remains as a compatibility fallback.

## Commands

Public operator commands are intentionally lean:

```text
cval config
cval status
cval nodes
cval run
cval jobs
cval result
```

Read-only node discovery:

```bash
cval nodes --output table
cval nodes --output json
```

Read latest validation status without creating or mutating DB tables:

```bash
cval status --output table
cval status --output json
cval status --output tsv
```

Build a dry-run workflow plan from known free nodes:

```bash
cval run \
  --free-nodes slc01-cl02-hgx-0001,slc01-cl02-hgx-0002 \
  --threshold-days 4 \
  --batch-size 2 \
  --timestamp 12345 \
  --output json
```

Or read validation history directly from the PVC access pod in read-only mode:

```bash
cval run \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --output json
```

If `--free-nodes` is omitted, `cval run` performs read-only live discovery and plans against fully free nodes.

Dry-run is the default. This does not submit Kubernetes resources:

```bash
cval run \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --git-ref main \
  --output json
```

Real submission is policy-gated and requires an explicit confirmation phrase:

```bash
cval run \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --git-ref <branch-tag-or-commit> \
  --submit \
  --confirm submit
```

Read Volcano job phases without mutating resources:

```bash
cval jobs --jobs cval-slc01-cl02-hgx-0064-pytorch-26-05-py3-12345
```

Poll Volcano job phases until terminal or timeout without deleting or cancelling jobs:

```bash
cval jobs \
  --jobs cval-slc01-cl02-hgx-0064-pytorch-26-05-py3-12345 \
  --watch \
  --timeout-seconds 180 \
  --poll-interval-seconds 30 \
  --output json
```

Inspect a structured validation result in shell-friendly form:

```bash
cval result --result-json /data/continuous_validation/results/<node>/cval-results-<node>-<timestamp>.json
```

In-pod DB ingestion commands used by `validation-tests/db-update.sh`:

```bash
cval db-add-result <node> <test> <pass|fail|incomplete> <timestamp> --db-path <validation.db>
cval db-add-storage-result <node> <timestamp> <storage-result-dir> --db-path <test-storage.db>
cval db-add-nccl-result <node> <timestamp> <busbw> <latency> --db-path <test-nccl.db>
```

The DB ingestion commands remain hidden compatibility commands for in-pod scripts.

## Safety Boundary

The new CLI slice does not delete Kubernetes resources. `nodes` and `jobs` perform read-only `kubectl get` calls. `status` performs a read-only SQLite query through the PVC access pod using `mode=ro`. `run --free-nodes ...` operates locally unless `--live-status` is passed. `run` also talks to Kubernetes when `--free-nodes` is omitted for live discovery. `run` is dry-run by default; real submission requires `--submit --confirm submit` and namespace/batch policy checks.

Submit/cleanup commands should be added only after policy gates exist for namespace allowlists, max batch size, node allow/deny lists, and explicit `--submit --confirm` operation mode.

## Next Refactor Slice

1. Package or symlink `skills/c-val-hpc-engineer` into the Hermes skill directory.
2. Replace runtime Git checkout with a prebuilt image once the validation image pipeline exists.
3. Add richer peer/baseline outlier classification for storage and NCCL metrics.