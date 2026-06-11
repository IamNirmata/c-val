# Design Decisions

## Dry-Run First

c-val operates GPU clusters. The safest default is to render intent without creating resources. `submit-plan` therefore defaults to dry-run and reports `submitted=false` until `--submit --confirm submit` is provided.

## Keep Deterministic Validation in Jobs

The orchestrator should not infer GPU health from arbitrary logs. It submits deterministic storage, NCCL, and DL validation jobs and consumes their structured outputs.

## Use the CLI as the Agent Contract

Hermes and human operators use the same `cval.cli` commands. This avoids a separate hidden agent API and makes every action reproducible from a shell.

## Read-Only Status Access

`cval status` opens SQLite with `mode=ro` through the PVC access pod. It must not create tables or mutate metadata.

## Explicit Runtime Code Ref

Validation jobs accept `CVAL_GIT_REF` so operators can pin a commit or tag. This prevents accidental validation against a moving `main` branch.

## Structured Result JSON Before DB Ingestion

`run-test.sh` writes `cval.results.v1` JSON after each phase. `db-update.sh` reads that JSON to decide per-test and aggregate DB rows. This makes failures visible and prevents unconditional `all/pass` writes.

## No Automatic Deletion

`monitor-jobs` reports timeout but does not delete or cancel jobs. Cleanup needs explicit operator approval.