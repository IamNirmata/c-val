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

**Deployment blocker:** raw DL files currently use WAL on shared NFS. SQLite
[does not support cross-host WAL](https://www.sqlite.org/wal.html).
`shared_raw_storage=true` rejects these sources rather than pretending retries
fix the filesystem contract. Use an approved quiesced journal-mode migration
with all writers configured compatibly, or a coherent local snapshot service.
This implementation neither changes raw journal modes nor copies live DB files.
Set `shared_raw_storage=false` only for a supported local/single-host source.

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
are dropped and root/raw filesystems read-only. Output uses a separate PVC.

Build from a clean checkout of the exact published revision:

```bash
docker build -f deploy/evaluation-engine/Dockerfile \
  --build-arg CVAL_COMMIT=<published-SHA> \
  -t <registry>/cval-evaluation-engine:<published-SHA> .
```

Before separately approved apply: resolve raw WAL storage, choose a real block
StorageClass in [pvc.yaml](pvc.yaml), publish the CPU image, pin its digest in
the Pod, verify node capacity and read-only source preflight. No manifest has
been applied. Never run a second pod against the same output; the worker also
holds an exclusive process lock. Stop/replacement/delete are separately gated.
This fixed-name Pod restarts containers, not a deleted Pod; node-loss recovery
requires operator replacement. A Deployment would generate a suffixed pod name.

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