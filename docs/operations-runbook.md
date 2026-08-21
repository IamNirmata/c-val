# Operations runbook

## Read-only checks

```text
cval status --output json
cval overview --output json
cval classifications --test all --type csv
```

These commands do not mutate Kubernetes or SQLite. `status`, `results`, and
scheduling continue to use `metadata/validation.db`.

## Cluster-first development validation

```text
cval nodes --output json
cval validate --node <eligible-node> --git-ref <40-hex-commit> \
  --submit --confirm submit
```

Publish the candidate commit first because the job fetches only that immutable
commit. The command performs the real storage/NCCL/DL workload and canonical
raw ingestion. A passing DL phase writes that run's rank metrics into all four
DL metric DBs under the shared DL refresh lock before its pass status is
committed. It does not persist baselines or classifications; those remain
independently operated by the resident evaluator. A degraded verdict never
triggers cluster mutation.

Use `cval plan --live-status --git-ref <40-hex-commit>` only to inspect the
rolling scheduler queue. It is
not a prerequisite for targeted development validation.

## Evaluator loops

Where `/data/continuous_validation` is mounted:

```text
bash scripts/cval-baseline-build.sh run-once
bash scripts/cval-baseline-classify.sh run-once
```

The scripts retain manual `run-once` and `run-loop` modes for diagnosis. In
production, the single `cval-evaluator` Deployment supervises both loops and
runs all recurring NCCL PostgreSQL work. Do not start separate tmux loops while
the Deployment is active. Before scaling it to one, discover and stop old tmux
loops and suspend any evaluator CronJobs left by an earlier deployment;
removing their source manifests does not prune live objects.

Evaluator DL rebuilds reconcile retained historical rank evidence and refresh
derived baselines. They are not required for current validation-run ingestion.

## Whole-root backup preparation

Preview only (the default destination is the excluded `backups/` subtree):

```text
bash scripts/cval-backup.sh
bash scripts/cval-backup.sh --destination-root /independent/storage/cval-backups
```

The capacity inspection creates nothing. It reports source file count, apparent unique bytes
(hardlinks counted once), destination filesystem free bytes, the configurable
safety margin (default 10%), and capacity sufficiency. A same-PVC backup is
useful for cutover rollback but is **not independent disaster recovery**;
prefer an external destination on separate storage.

Apply is a separate live write and must not be run without approval. First stop
the evaluator/baseline loops and ensure all validation ingestion is stopped.
Only then make the exact quiescence declaration:

```text
bash scripts/cval-backup.sh \
  --destination-root /independent/storage/cval-backups \
  --apply --confirm backup \
  --quiesced --confirm-quiesced writers-stopped
```

Apply fails before writes when capacity is insufficient. It excludes the source
`backups/` subtree; rejects symlinks, devices, FIFOs, incomplete hardlink sets,
and SQLite WAL/SHM/journal sidecars; inventories path/type/device/inode/size/
times/link count before and after; uses SQLite online backup plus `quick_check`;
copies regular files no-follow; preserves hardlinks and metadata; and publishes
a staged timestamped directory atomically without overwrite. Failure removes
only its private staging directory. It never deletes source data or an existing
backup.

Verify the published backup read-only before cutover:

```text
bash scripts/cval-backup.sh --verify \
  /independent/storage/cval-backups/cval-backup-YYYYMMDDTHHMMSSZ
```

Verification validates the manifest, exact file/directory set, sizes, modes,
mtimes, SHA-256 checksums, hardlink groups, and SQLite `quick_check`, then
reports `restore_ready: true`. This helper deliberately does not restore data.

## Classification cutover preparation

Preview a copied or explicitly selected former global DB:

```text
scripts/cval-split-classifications.py \
  --source <root>/baselines/classification-results.db
```

Apply requires exact `--apply --confirm split-classifications` plus
`--backup-manifest <path>`. The manifest must authenticate the source and its
verified SQLite backup. Exact existing targets and targets containing the exact
source subset plus newer rows are accepted idempotently; conflicting rows or
schemas fail closed. The source DB is retained.

## Evaluator deployment preparation

- `deploy/cval-evaluator/base/evaluator-deployment.yaml` is the sole recurring
  evaluator workload.
- The pod runs storage/DL SQLite loops and all recurring NCCL PostgreSQL tasks;
  there are no evaluator CronJobs.
- The Deployment clones/fetches `CVAL_GIT_REPO` and `CVAL_GIT_REF`, detaches at the
  resolved commit, verify exact checkout, install editable c-val, and validate
  the registry.
- It has one replica, `Recreate` strategy, no Kubernetes token, no `kubectl`,
  and no GPU/RDMA requests.
- Existing evaluator CronJobs/controllers must be discovered and suspended
  before activation; their later deletion requires separate approval.

Do not apply, scale, rename/delete the live pod, run backup/split against the
live PVC, or restart tmux until the remaining actions in `todo/cval-update.md`
are explicitly approved.
