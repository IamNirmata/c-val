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

[ymls/cval-evaluation-engine.yaml](../../ymls/cval-evaluation-engine.yaml) is the
canonical template defining the exact fixed name `cval-evaluation-engine`,
Always restart, 2 CPU, 2Gi request/4Gi limit; no GPU or RDMA, API token, or writes
outside `/data/continuous_validation` within `/data`. CPU node
`slc01-cl02-ccpu-008` matches the inspected PVC reader.
UID 0 is needed for existing root-owned private result summaries; capabilities
are dropped and the root filesystem remains read-only. `/evaluation` is a
writable subPath `continuous_validation/evaluation-engine` on the existing claim.
The entire `/data/continuous_validation` tree is writable through a second
subPath mount; the rest of `/data` remains read-only. Existing file permissions
still apply; no recursive chmod/chown is performed. Raw SQLite connections in
the evaluator remain read-only, but filesystem protection no longer prevents
interactive tools from changing raw evidence. Preserve all raw evidence.
Use one PVC volume with three container mounts rather than presenting the same
CSI volume twice with conflicting modes. Provision subPath directories first.

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

### VS Code attachment

The template keeps `readOnlyRootFilesystem: true` and mounts a dedicated,
disk-backed `emptyDir` at `/root/.vscode-server` with a 2Gi size limit. The
container requests 2Gi of ephemeral storage and limits it to 4Gi, allowing
headroom for `/tmp`, application files and logs. This is node-local storage,
not a new PVC and not part of the raw or derived databases.

Attach to the `evaluator` container using VS Code Dev Containers after the
updated pod is deployed. Allow VS Code to install its matching server and
extensions in that directory; do not hard-code a different server build into
the Python bootstrap. Installation requires download access or a matching
server supplied through an approved offline transfer. Kubernetes exec/attach
permissions are also required. This mount addresses installation writes, not
authentication, network, or extension-compatibility failures.

The volume survives container restarts but is lost on pod deletion/recreation.
Server builds and extensions must then be installed again. Increase its size
if multiple builds/extensions exceed 2Gi. `/app` stays read-only, but
`/data/continuous_validation` is writable, including raw evidence and evaluation
output. VS Code extensions run as UID 0 and can modify that entire subtree;
attach only trusted clients/extensions and avoid heavy interactive work while
the evaluator is processing.

### Recreate or update

Keep template edits under `ymls/` and retain the bootstrap, renderer and offline
dependency tooling here. Publish the reviewed commit first, then render from
that exact SHA (which must contain the new template path):

```bash
python3 deploy/evaluation-engine/render.py \
  --revision <exact-published-40-hex-SHA> \
  --wheel <reviewed-PyYAML-wheel> \
  --output-dir <new-release-directory>
```

The renderer emits `source-configmap.yaml`, `dependency-configmap.yaml` and
`pod.yaml`. Do not apply the template directly: its ConfigMap names are
intentional placeholders. Existing immutable ConfigMaps remain available for
rollback; keep the old rendered Pod manifest too.

Adding a volume/mount to an existing standalone Pod requires replacement, not
an in-place apply. Separately approve the exact ConfigMap apply, graceful Pod
deletion and rendered Pod creation commands. Wait for the old pod to terminate
before creating its replacement; retain the PVC and never run two evaluators
against the same output. Verify source annotation, init completion, worker
logs, mount permissions and actual VS Code attachment after replacement.
Rollback recreates the old rendered Pod only after the replacement terminates;
it does not delete or restore database contents automatically.

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

The raw status catalog is queried in bounded pages of complete `(node,timestamp)`
identities ordered by their first rowid. The per-metric row cap is not a cap on
the whole catalog. A persisted rotating cursor revisits old identities for late
arrivals and repairs. This is bounded reconciliation, not timestamp-only discovery.
Outputs can lag by a sweep; do not infer raw freshness from an evaluation alone.
Baseline discovery separately fetches at most `max_training_runs + 1` identities
inside the training window, then trains on at most `max_training_runs=100`.
`bounded=true` means more candidates exist; the reported candidate count is the
bounded number fetched, not a full catalog count.
Large metric DBs are read only by exact node/timestamp, never fully loaded.
No automatic pruning: monitor output disk growth and retain referenced baselines.

DL metric coverage is checked against the retained plan and run-summary plan/
iteration identity, independently of observed DB rows or baseline thresholds.
The test-owned contract expands selected numerical and timing metrics per task,
overlap layer/collective pair, and rank. Supported layer constructors are parsed
without executing the layer expression; optional bias/affine parameters are
respected. Unknown layer contracts stay unclassified until explicitly supported.
Missing or extra selected metric identities produce
`dl_required_metric_coverage_mismatch`, exclude training evidence, and cannot
produce a healthy test class. The required-key digest is part of the DL cohort,
so older baselines without this coverage contract cannot be reused silently.

New artifacts live under `logs/validation_tests`. Historical paths in retained
JSON and SQLite receipts are not rewritten; read-only evaluation resolves the
explicit legacy-to-logs mapping after the approved PVC migration. See the
[operations runbook](../../docs/operations-runbook.md#approved-consolidation-workflow).

Worker JSON reports and `worker_state` expose progress/errors. New tests add
their own two scripts and `[evaluation]` policy; no central dispatch map change.
No health class causes Kubernetes actions. Threshold calibration, certified
correctness references, production sizing and live acceptance remain required.