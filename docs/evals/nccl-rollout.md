# NCCL PostgreSQL production rollout and rollback

This is a reviewed, phased procedure—not an authorization to mutate the live
cluster. The existing SQLite raw databases remain authoritative until the
cutover is separately approved. Validation jobs never receive PostgreSQL
credentials: they write immutable native JSON to
`/data/continuous_validation/nccl_eval/outbox/`; the credentialed NCCL process
inside the resident evaluator reads that directory without deleting files.

Source manifests fail closed: PostgreSQL and the evaluator have zero replicas,
and every `CVAL_GIT_REF` is an all-zero placeholder. NCCL Git, Python, and
PostgreSQL images are already pinned to reviewed immutable digests. No source
Secret is provided. There are no recurring evaluator CronJobs.

NCCL workloads use a pinned `alpine/git@sha256` repo-pull init container and a
pinned Python base; they install `requirements-postgresql.lock` with
`--require-hashes`, then install c-val editable with `--no-deps`. All Python
3.12 transitive/bootstrap dependencies, including setuptools 80.10.2, are
exactly versioned and hash-pinned. The clean bootstrap has passed in the pinned
Python image. Git and PyPI network access remain required during init, but
accepted content is commit/digest/hash pinned.

## Required Secret contract

Create two namespace-local Secrets out of band:

- `cval-postgres-admin`: `username`, `password`, and owner `admin-url`;
- `cval-postgres-runtime`: `runtime-url`, `runtime-username`, and
  `runtime-password` for the non-owner role provisioned by the evaluator init
  sequence.

Runtime workloads never receive the admin Secret. The StatefulSet receives
only the admin username/password, not either URL.

Do not place values in source control, command history, rendered review logs, or
chat. Secret creation/application requires separate live approval.

## Phase 0 — quiesce and whole-root backup

1. Stop the audit/evaluator writer loops and verify no current validation or
  baseline/classification writer is active. Discover any previously deployed
  evaluator CronJobs/controllers too: removing them from source does not prune
  live objects. Suspend or retire them only with an approved, bounded command
  list. Do not scale the resident evaluator while any old writer remains.
2. From the existing PVC reader, run the whole-root backup plan:

```text
bash scripts/cval-backup.sh \
  --source /data/continuous_validation \
  --destination-root /data/cval-backups
```

3. **APPROVAL REQUIRED — backup plus verified writer quiescence:**

```text
bash scripts/cval-backup.sh \
  --source /data/continuous_validation \
  --destination-root /data/cval-backups \
  --apply --confirm backup --quiesced --confirm-quiesced writers-stopped
```

4. Verify the exact generated directory and manifest before migration:

```text
bash scripts/cval-backup.sh --verify /data/cval-backups/<BACKUP_DIR>
```

Do not continue if the manifest is incomplete or any writer resumed.

## Phase 1 — storage semantics and fail-closed manifests

PostgreSQL uses a dedicated 100Gi RWO StatefulSet claim and never mounts the
shared validation PVC. Replace
`replace-with-reviewed-rwo-storage-class` only after storage-class review.
Run the preflight inside an approved disposable pod with that claim mounted:

```text
bash scripts/cval-nccl-postgres-preflight.sh \
  --mount-path /var/lib/postgresql/data \
  --pgdata-path /var/lib/postgresql/data/pgdata
```

The script reports filesystem type/options, capacity, and target absence but
returns `postgresql_storage_supported=UNDETERMINED`. Disposable writes require
separate storage approval:

```text
# APPROVAL REQUIRED — disposable probe only; no authoritative data
bash scripts/cval-nccl-postgres-preflight.sh \
  --mount-path /var/lib/postgresql/data \
  --pgdata-path /var/lib/postgresql/data/pgdata \
  --apply --confirm storage-preflight
```

This probe is not PostgreSQL certification. Before selecting the storage
class, render and separately approve a disposable namespace-scoped Job using
the same pinned PostgreSQL image and a fresh claim to run `initdb`, start,
create/commit/checkpoint, stop, restart, verify the committed row, and remove
only the disposable Job/claim. Keep its bounded logs as rollout evidence; do
not reuse that disposable PGDATA for production.

Review the source render; it is non-runnable:

```text
kubectl kustomize deploy/cval-evaluator > /tmp/cval-nccl-source.yaml
```

Applying any base or overlay remains separately approval-gated. The DB overlay
only sets PostgreSQL to one replica. Render it after Secret, image/commit/hash,
storage-class, preflight, and PGDATA-absence review:

```text
kubectl kustomize deploy/cval-evaluator/overlays/db \
  > /tmp/cval-nccl-db-phase.yaml
```

## Phase 2 — resident evaluator initialization

After an approved DB-phase apply, wait for PostgreSQL readiness with a bounded
read-only status command. Stop any old storage/DL evaluator loops before the
resident pod is enabled. Render the evaluator overlay; it sets exactly one
`cval-evaluator` replica. Its init sequence waits for PostgreSQL, applies
idempotent migrations with `admin-url`, and provisions the least-privilege
runtime role. The running pod then starts a root SQLite sidecar for the existing
root-owned storage/DL files and a non-root NCCL sidecar for outbox ingestion,
baseline building, recovery, and evaluation:

```text
kubectl kustomize deploy/cval-evaluator/overlays/evaluate \
  > /tmp/cval-evaluator-phase.yaml
```

The equivalent local plans/commands are:

```text
python -m cval.cli nccl-eval schema
python -m cval.cli nccl-eval schema --apply --confirm schema
python -m cval.cli nccl-eval grant-runtime --apply --confirm grant-runtime
```

Applying the evaluator overlay requires separate approval. Verify one pod,
four completed init containers, both running sidecars, `Recreate` strategy,
and readiness before enabling NCCL outbox generation.

Before apply, retain read-only evidence that no old evaluator Job/CronJob/tmux
loop is active. If an old CronJob exists, suspend it first; deletion is a
separate optional cleanup approval and is not required for rollback safety.

## Phase 3 — copied legacy calibration import

Use only a copied SQLite file covered by the verified backup manifest. First
supply reviewed profile metadata and inspect without writes:

```text
python -m cval.cli nccl-eval migrate-legacy \
  --sqlite /data/cval-backups/<BACKUP_DIR>/metadata/test-nccl.db \
  --profile-metadata /secure/cval/nccl-legacy-profile.json
```

**APPROVAL REQUIRED — copied source import:**

```text
python -m cval.cli nccl-eval migrate-legacy \
  --sqlite /data/cval-backups/<BACKUP_DIR>/metadata/test-nccl.db \
  --profile-metadata /secure/cval/nccl-legacy-profile.json \
  --apply --confirm migrate-legacy
```

Legacy and native rows have no calibration decision and are excluded. Submit
exact result IDs through `nccl-eval calibration plan --input decisions.json`,
then append only after review with `calibration apply --apply --confirm
calibration`. Revocation appends `REVOKE`; raw rows are never mutated.
The first event for a result must be version 1 `APPROVE`; later versions are
contiguous and alternate action. Runtime callers have no direct INSERT on the
ledger and invoke a security-definer function. Revoking a sample included in
the active baseline atomically marks that version `FAILED`, records the reason,
clears the profile pointer, and returns ready/retry jobs to
`WAITING_FOR_BASELINE`. Historical lineage and evaluations remain immutable.
Runtime-role reuse is accepted only after attestation finds no memberships,
unsafe role attributes, database-local ownership, default privileges, or
direct ACLs outside the exact runtime allowlist.

## Phase 4 — outbox activation

Keep `evaluation_enabled=false` in the validation descriptor until PostgreSQL,
schema, backup, and nonwriting scanner evidence are accepted. Inspect the scanner
without credentials or database access:

```text
python -m cval.cli nccl-eval ingest-outbox \
  --outbox-root /data/continuous_validation/nccl_eval/outbox --limit 5000
```
The resident pod runs the NCCL process as UID/GID 65532. Pending/committed
directories are exactly `0755` and immutable files `0644`; never `chown` the
shared validation root. Verify one bounded read before enabling outbox
generation in validation jobs:

```text
kubectl -n gcr-admin exec <EXACT_EVALUATOR_POD> -c nccl-evaluator -- \
  /bin/sh -ec 'test "$(id -u)" = 65532; stat -c "%a %F" /data/continuous_validation/nccl_eval/outbox /data/continuous_validation/nccl_eval/outbox/pending /data/continuous_validation/nccl_eval/outbox/committed; test -r /data/continuous_validation/nccl_eval/outbox/committed'
```
Set `evaluation_enabled=true` only in a
reviewed validation-job release pinned to an exact source commit and image
digest. Ingestion creates immutable `pending/<run>.json` before any SQLite
write, performs all compatibility writes, then creates
`committed/<run>.json` binding the pending and result digests. The scanner
ignores pending files without markers. If marker creation fails after SQLite,
retry `nccl-eval commit-outbox` with the same pending file and result digest.
If setup fails before runtime evidence can be collected, raw compatibility
status is still recorded and no incomplete PostgreSQL outbox is emitted.
Passing results and failures with runtime evidence remain fail-closed on outbox
validation. Outbox files and PostgreSQL receipts are retained permanently.

## Phase 5 — baseline activation and acceptance

Inspect eligibility and queue state:

```text
python -m cval.cli nccl-eval build-baselines --output json
python -m cval.cli nccl-eval evaluate --output json
python -m cval.cli nccl-eval status --output json
```

The resident evaluator automatically builds the first baseline when an approved
profile reaches 40 eligible results, then refines every additional 10. No new
workload is unsuspended. Do not claim cutover complete until repeated
`nccl-eval status` receipts show ingestion, baseline, queue, and resident health
and storage/DL SQLite comparison evidence has been reviewed.

## Rollback

Rollback never deletes PVC files, outbox JSON, PostgreSQL rows, old SQLite,
baselines, classifications, logs, or backup evidence.

1. Set `evaluation_enabled=false` in the next reviewed validation release.
2. **APPROVAL REQUIRED:** scale `cval-evaluator` to zero with an exact bounded
  patch command. This stops all recurring evaluator DB work.
3. **APPROVAL REQUIRED:** scale `cval-postgres` to zero after all PostgreSQL
   clients are stopped.
4. Continue current SQLite ingestion/read/export paths. They remain the
   authoritative fallback and were never removed or rewritten.
5. Preserve PGDATA for investigation or later recovery; deletion is not part
   of rollback.

## Current live blockers

No live phase is approved by this document. Before any apply, resolve and
review all of the following:

- replace every all-zero `CVAL_GIT_REF` with one reviewed 40-hex commit;
- replace `replace-with-reviewed-rwo-storage-class` after dedicated RWO
  semantics and disposable PostgreSQL restart evidence are accepted;
- create the split admin/runtime Secrets out of band and verify the reused
  runtime role attestation is clean;
- retain verified whole-root backup, quiescence, preflight, render, and
  rollback evidence for the exact release.
