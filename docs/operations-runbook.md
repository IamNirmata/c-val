# Operations runbook

## Read-only checks

```text
cval nodes --output json
cval status --output json
cval results --test overall --type csv
cval results --test storage --type csv
cval results --test nccl --type csv
```

These commands do not mutate Kubernetes or SQLite.

## Continuous validation loop

Every operational `scripts/cval-live.sh` cycle is submit-capable and requires
exact `CVAL_LIVE_CONFIRM=submit`. Each submission also passes the CLI's
independent `--submit --confirm submit` gate with an exact published commit.

Use the script's `start`, `status`, and `stop` commands for the persistent tmux
`run-once` and `run-loop` require the same environment confirmation. Stopping
the session does not delete Kubernetes jobs, and pending-job pruning requires
the separate exact `CVAL_PRUNE_CONFIRM=delete-pending` gate.

## Exact-commit validation

The validation command requires an eligible node, exact published commit,
explicit submit flag, and exact confirmation. The validation pod checks out
that commit, runs enabled storage, NCCL, and DL tests, validates canonical
evidence, and writes raw SQLite databases. No evaluation or node-health
assignment follows ingestion.

The continuous loop uses these same exact-commit submission gates for every
job.

## Raw DL reconciliation

`db-rebuild-dltest-metrics` can append missing raw DL metric evidence from
retained rank JSON. It does not calculate verdicts or delete rows.

## Whole-root backup

`scripts/cval-backup.sh` defaults to a nonwriting capacity preview. Apply is a
separate live write requiring an independent destination, stopped source
writers, exact backup confirmation, and exact quiescence confirmation.

Verification is read-only and validates the published manifest, checksums,
hardlinks, metadata, and SQLite integrity. Backup never restores or deletes
source data.
