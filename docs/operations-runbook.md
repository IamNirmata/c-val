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

## Audit loop

Run `scripts/cval-live.sh` in audit mode with an exact published Git commit.
Audit mode reads inventory and latest status, plans all discovered candidates
by default, and checks them in priority order. It submits and prunes nothing.

Use the script's `start`, `status`, and `stop` commands for the persistent tmux
session. `start` fetches the current branch from `origin`, rejects a stale
explicit ref, and pins the session to that exact latest published commit.
Stopping the session does not delete Kubernetes jobs.

## Exact-commit validation

The validation command requires an eligible node, exact published commit,
explicit submit flag, and exact confirmation. The validation pod checks out
that commit, runs enabled storage, NCCL, and DL tests, validates canonical
evidence, and writes raw SQLite databases. No evaluation or node-health
assignment follows ingestion.

The continuous submit loop additionally requires submit mode and its exact
environment confirmation. Pending-job pruning has a separate exact gate.

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
