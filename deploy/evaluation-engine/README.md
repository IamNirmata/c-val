# Evaluation Engine

Independent CPU evaluator. Raw c-val validation still has no evaluation step.
Implementation: [evaluation_engine](../../evaluation_engine); test-owned
`baseline.py` / `classification.py` under each validation test directory.

## Configuration

- Global [config/cval.toml](../../config/cval.toml): `[evaluation]` cadence,
  limits, output path; `[sqlite_retry]` attempts, fixed delay and read timeout.
- Each `test_config.toml`: `[evaluation]` scripts, sample minimum, windows,
  tolerances, expected ranks, explicit metric lists. Excluded from raw snapshots.
- Default: 60-second wake; 20 runs/test/pass; hourly baseline check; 14-day
  refresh, 28-day training window, 24-hour held-out window; minimum 30 nodes.
- One latest passing run per node/cohort; rank-specific thresholds. Numerical
  rules use independent cross-node consensus with 1% relative/1e-6 absolute
  tolerance. This is not certified numerical correctness. Timing classes are
  relative bands with a 5% minimum separation from the median.

```bash
python -m evaluation_engine --config config/cval.toml --check-config
python -m evaluation_engine --config config/cval.toml --check-sources
```

The second command only opens raw sources read-only. To write derived results,
the worker requires `--confirm evaluate --revision <exact-published-40-hex-SHA>`;
`--once` performs one bounded pass. Otherwise it wakes continuously. Never point
its output at a raw database. Test with temporary DBs before approving live use.

Test-owned scripts are also standalone (installed package or `PYTHONPATH=.`):
`python validation-tests/dltest/baseline.py --config config/cval.toml ...`
or `classification.py`, with the same confirmation/revision arguments. They run
one test's bounded baseline-only/classification-only pass under the same lock.

## Storage and Retry

SQLite BUSY/LOCKED: wait and retry a complete operation, up to five attempts.
Raw `db-*` CLI commands retry after connection cleanup; existing identity,
digest and idempotency checks remain. Non-lock errors are not blindly retried.
Derived writes use short transactions and DELETE journaling; raw connections
use `mode=ro`, `query_only=ON`, query deadlines and row limits.

SQLite [does not support cross-host WAL](https://www.sqlite.org/wal.html).
`shared_raw_storage=true` keeps rejecting WAL sources. Current DL writers use
DELETE journaling and FULL durability, and refuse to change an existing WAL DB
implicitly. Before rollout, pause the submission loop, drain/cancel old writers,
and use [cval-sqlite-journal.py](../../scripts/cval-sqlite-journal.py) with explicit
migration/quiescence confirmation. It holds the stable ingestion record lock, backs
up each DB through SQLite, converts only the four DL journal modes, and retains
a verification manifest. Tables, receipts and sampled metrics must not change.

The [SQLite backup API](https://www.sqlite.org/backup.html) stages each snapshot
in a private temporary directory under `--staging-directory` (default `/tmp`).
Choose node-local storage with capacity for the largest DB plus a 20% margin.
The tool streams the closed snapshot to a new PVC backup file in 1 MiB chunks,
fsyncs it, and verifies its full SHA-256 before permitting source conversion.
It retains that checksum in the manifest and removes only its private temporary
staging directory. An incomplete PVC backup is retained, never overwritten.
This avoids page-sized synchronous backup writes on NFS. Metric evidence uses
six bounded rowid-tail samples plus all receipts, schema and generation markers.
Progress includes stages, SQLite status, pages and elapsed time; the per-DB
backup deadline also covers publication and checksum verification.
`--check-backup-source` is a read-only first-batch diagnostic using memory only.

The selected deployment uses the existing NFSv4.1 PVC with remote locks enabled,
DELETE journaling, and one derived writer. Run [check_locking.py](check_locking.py)
between the reader and a second CPU pod before use. This verifies SQLite and
POSIX record-lock exclusion/release; it is not a guarantee against storage outages.
Any failed lock test blocks rollout. A dedicated block PVC remains preferable
when provisioning permission is available; [pvc.yaml](pvc.yaml) is an optional
template, not required or applied by this deployment.

The live preflight disproved directory `flock` exclusion across CPU nodes.
Raw ingestion, migration and the evaluator now use `fcntl.lockf` on persistent
regular files. Never unlink/replace a lock file while any participant may run.

Missing provenance, references, rank coverage, or receipt/generation mismatch
remain `unclassified`; they never inherit a global baseline. Current DL/NCCL
hardware verification reads the recorded DL summary. NCCL-only historical runs
without that provenance remain unclassified. Storage cohorts use source root,
test-config digest and environment; cross-storage-target pooling is not enabled.

## Pod

[pod.yaml](pod.yaml) defines the exact fixed name `cval-evaluation-engine`,
Always restart, 2 CPU, 2Gi request/4Gi limit; no GPU or RDMA, API token, or writes
to `/data`. CPU node `slc01-cl02-ccpu-008` matches the inspected PVC reader.
UID 0 is needed for existing root-owned private result summaries; capabilities
are dropped and root/raw filesystems read-only. `/evaluation` is a writable
subPath `continuous_validation/evaluation-engine` on the existing claim. Raw
`/data` remains read-only. Provision that isolated directory explicitly first.
Use one PVC volume with two container mounts; enforce `/data` read-only at the
mount rather than presenting the same CSI volume twice with conflicting modes.

The default deployment uses a digest-pinned public Python image and immutable
ConfigMaps containing a Git archive of the exact commit and an offline PyYAML
wheel. [render.py](render.py) creates the two ConfigMaps and Pod manifest;
[bootstrap.py](bootstrap.py) verifies their SHA-256 digests before extraction.
No private registry credentials or dependency network access are needed at
startup. An exact-commit custom image remains an alternative:

Build from a clean checkout of the exact published revision:

```bash
docker build -f deploy/evaluation-engine/Dockerfile \
  --build-arg CVAL_COMMIT=<published-SHA> \
  -t <registry>/cval-evaluation-engine:<published-SHA> .
```

Before separately approved apply: verify quiescence, backups, journal modes,
cross-node locks, source digests, CPU capacity, and read-only source preflight.
The ConfigMap renderer accepts only a complete Git SHA and refuses overwrite
of its output manifests. Never run a second pod against the same output; the
worker holds an exclusive process lock. Stop/replacement/delete are gated.
This fixed-name Pod restarts containers, not a deleted Pod; node-loss recovery
requires operator replacement. A Deployment would generate a suffixed pod name.

Rollback: stop the evaluator and keep its output/ConfigMaps; do not revert the
compatible raw writer after migration. Migration backups can be restored only
while all readers/writers are quiesced and before subsequent raw writes, under
separate approval. Restart cval-live only at the latest compatible remote tip.

## Data and Recovery

Output tables: `baseline_version`, `threshold`, `evaluation`,
`metric_classification`, `test_classification`, `worker_state`;
view `latest_test_class`. DL's four categories retain their identities in
metric keys; the DL test summary aggregates their required comparisons.
Comparison rows, summary and evaluation receipt commit together. The scan
cursor commits afterward; a crash in between replays idempotently. Repeated
observations refresh `evaluated_at`, not immutable comparison payloads.

The small raw status catalog is scanned in rowid order; a persisted rotating
cursor evaluates bounded batches and revisits old identities for late arrivals
and repairs. This is bounded reconciliation, not timestamp-only discovery.
Outputs can lag by a sweep; do not infer raw freshness from an evaluation alone.
Baseline training is capped at `max_training_runs=100`, reported when truncated.
Large metric DBs are read only by exact node/timestamp, never fully loaded.
No automatic pruning: monitor output disk growth and retain referenced baselines.

Worker JSON reports and `worker_state` expose progress/errors. New tests add
their own two scripts and `[evaluation]` policy; no central dispatch map change.
No health class causes Kubernetes actions. Threshold calibration, certified
correctness references, production sizing and live acceptance remain required.