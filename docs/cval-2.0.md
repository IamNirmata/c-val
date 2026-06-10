# c-val 2.0 Implementation Notes

This document tracks the first package-oriented refactor slice. The goal is to move c-val from a notebook-first prototype toward a dry-run-first Python package and CLI that Hermes can safely operate.

## Current Slice

Implemented package modules:

- `cval.k8s.discovery`: pure parsing for Kubernetes pod/node JSON and GPU free-node discovery.
- `cval.scheduler.priority`: stale-node priority queue construction.
- `cval.jobs.renderer`: validation job manifest rendering from the existing Volcano YAML template.
- `cval.orchestrator.workflow`: dry-run workflow planning from free nodes, validation history, and job template rendering.
- `cval.cli`: dry-run-first command surface.

`job-runner.ipynb` now acts as a thin, dry-run-oriented notebook over the package APIs. `utils/functions.py` remains in place for legacy compatibility while DB writes and helper commands are migrated.

Validation job scripts now persist per-test results through `CVAL_RESULT_JSON_FILE` with schema version `cval.results.v1`. `run-test.sh` writes storage, NCCL, and DL statuses after each phase, and `db-update.sh` records `storage`, `nccl`, `dltest`, and aggregate `all` rows based on those actual results instead of writing unconditional `all/pass`. `CVAL_RESULT_ENV_FILE` remains as a compatibility fallback.

## Commands

Read-only discovery:

```bash
cval discover-free-nodes --output table
cval discover-free-nodes --output json
```

Read latest validation status without creating or mutating DB tables:

```bash
cval status --output table
cval status --output json
cval status --output tsv
```

Priority queue from known free nodes and optional DB status JSON:

```bash
cval prioritize \
  --free-nodes slc01-cl02-hgx-0001,slc01-cl02-hgx-0002 \
  --db-status-json latest-status.json \
  --threshold-days 4
```

The same command can consume the tab-separated output shape produced by the existing DB status path:

```bash
cval prioritize \
  --free-nodes slc01-cl02-hgx-0001,slc01-cl02-hgx-0002 \
  --db-status-tsv latest-status.tsv \
  --threshold-days 4
```

Render one job manifest without submitting it:

```bash
cval render-job \
  --node slc01-cl02-hgx-0001 \
  --timestamp 12345 \
  --template ymls/specific-node-job.yml \
  --git-ref main
```

Render a dry-run batch without submitting any Kubernetes jobs:

```bash
cval run-batch \
  --nodes slc01-cl02-hgx-0001,slc01-cl02-hgx-0002 \
  --batch-size 2 \
  --timestamp 12345
```

Build a full dry-run workflow plan from known free nodes:

```bash
cval plan \
  --free-nodes slc01-cl02-hgx-0001,slc01-cl02-hgx-0002 \
  --db-status-tsv latest-status.tsv \
  --threshold-days 4 \
  --batch-size 2 \
  --timestamp 12345 \
  --output json
```

Or read validation history directly from the PVC access pod in read-only mode:

```bash
cval plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --output json
```

To use an explicit TSV file, create it first:

```bash
cval status --output tsv > latest-status.tsv
```

If `--free-nodes` is omitted, `cval plan` performs read-only live discovery and plans against fully free nodes.

Dry-run a submit plan. This does not submit Kubernetes resources:

```bash
cval submit-plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --git-ref main \
  --output json
```

Real submission is policy-gated and requires an explicit confirmation phrase:

```bash
cval submit-plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --git-ref <branch-tag-or-commit> \
  --submit \
  --confirm submit
```

Read Volcano job phases without mutating resources:

```bash
cval job-status --jobs hari-gcr-ceval-slc01-cl02-hgx-0064-12345
```

Poll Volcano job phases until terminal or timeout without deleting or cancelling jobs:

```bash
cval monitor-jobs \
  --jobs hari-gcr-ceval-slc01-cl02-hgx-0064-12345 \
  --timeout-seconds 180 \
  --poll-interval-seconds 30 \
  --output json
```

Inspect a structured validation result in shell-friendly form:

```bash
cval result-env --result-json /data/continuous_validation/results/<node>/cval-results-<node>-<timestamp>.json
```

## Safety Boundary

The new CLI slice does not delete Kubernetes resources. `discover-free-nodes`, `job-status`, and `monitor-jobs` perform read-only `kubectl get` calls. `status` performs a read-only SQLite query through the PVC access pod using `mode=ro`. `render-job`, `prioritize`, `run-batch`, and `plan --free-nodes ...` operate locally unless `--live-status` is passed. `plan` also talks to Kubernetes when `--free-nodes` is omitted for live discovery. `submit-plan` is dry-run by default; real submission requires `--submit --confirm submit` and namespace/batch policy checks.

Submit/cleanup commands should be added only after policy gates exist for namespace allowlists, max batch size, node allow/deny lists, and explicit `--submit --confirm` operation mode.

## Next Refactor Slice

1. Add a controlled end-to-end one-node submit/monitor/ingest validation run after explicit approval.
2. Package or symlink `skills/c-val-hpc-engineer` into the Hermes skill directory.
3. Replace runtime Git checkout with a prebuilt image once the validation image pipeline exists.