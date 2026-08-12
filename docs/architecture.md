# c-val architecture

## Purpose

c-val continuously validates GPU-cluster nodes without allowing health verdicts
to mutate the cluster. The code path is deterministic and testable:

1. list all matching GPU nodes without scanning cluster-wide pods;
2. prioritize never-tested or stale nodes from `metadata/validation.db`, after
  applying the node submission cooldown;
3. check prioritized nodes one at a time until the first currently available
  node is found;
4. render Volcano jobs from an exact published commit;
5. run development validation directly on the cluster through the existing
  double confirmation gate;
6. run registered storage, NCCL, and DL checks;
7. ingest current raw pass/fail and metric databases;
8. build median/MAD baselines and classify nodes;
9. export `normal`, `degraded`, and `improved` verdicts.

## Canonical data model

Raw execution state remains authoritative in:

- `metadata/validation.db` — built-in pass/fail rows and `latest_status`;
- `metadata/test-storage.db` — FIO metrics;
- `metadata/test-nccl.db` — consolidated NCCL/HCA metrics;
- `metadata/dltest_numerical_correctness.db`;
- `metadata/dltest_compute_performance.db`;
- `metadata/dltest_collective_performance.db`;
- `metadata/dltest_overlap_performance.db`.

Baselines remain immutable records with the lifecycle
`candidate → active → superseded`. Storage and each DL component use a
separate baseline DB under `baselines/`.

Classification is also per evaluator target:

- `storage-classifications.db`;
- `dltest-classifications.db`;
- `dltest-{numerical,compute,collective,overlap}-classifications.db`.

Readers enumerate these files and merge the latest `(node, test_type)` rows.
The old global `classification-results.db` is not migrated automatically.

## Validation runtime

The test registry is defined by `[tests.<id>]` entries in the operator config
and repository-local `cval.test.v1` descriptors. A descriptor declares test
metadata, resources, settings, artifact summary filename, and optional plugin.
It does not require a framework-owned per-test result or health database.

The descriptor-anchored supervisor creates canonical evidence under retained
file descriptors and invokes the generic runner. New runs write the canonical
`cval.results` envelope. Historical `cval.results.v1` and `cval.results.v2`
artifacts remain readable and are never rewritten.

`validation-tests/db-update.sh` writes only the current raw databases. It has no
normalized run-history or alternate per-test dual-write branches.

The optional NCCL PostgreSQL path is an asynchronous outbox, not a validation
job dual-write. The GPU pod records strict runtime evidence and, only when the
descriptor flag is enabled, writes immutable pending JSON before authoritative
raw SQLite writes and a digest-bound committed marker after those writes are
durable. It receives no PostgreSQL credentials. A suspended
credentialed process in the resident evaluator scans the shared PVC
non-recursively and ingests each file with a durable PostgreSQL receipt. Outbox
files are retained.

## Canonical evaluator

`cval.baselines` is the sole storage/DL evaluator. It computes robust median/MAD bands,
keeps engineering-tolerance floors, and aggregates DL severity using count and
fraction thresholds. Plugins may declare baseline and export hooks, including
unique test-owned source/DB behavior. c-val does not create a generic common DB
for a new test.

The NCCL PostgreSQL evaluator is the single explicit exception, governed by
`docs/evals/nccl-eval-process.md`. It does not change storage or DL evaluation.

## Evaluator workload

The source-controlled `cval-evaluator` Deployment uses
`python:3.12-slim`, clone/fetch an operator-configured `CVAL_GIT_REPO` and
`CVAL_GIT_REF`, verify the exact checkout, install c-val, validate the registry,
and mount `/data`. They carry no Kubernetes token, GPU, RDMA, host path, port,
or embedded credential.

One root SQLite sidecar supervises the storage/DL build and classification
loops because the existing live files and directories are root-owned. One
non-root NCCL sidecar reads the shared PVC outbox and owns all recurring
PostgreSQL tasks. Idempotent schema migration and runtime-role provisioning run
as init containers. The Deployment uses `Recreate` and one replica, preventing
two storage/DL writers during rollout. PostgreSQL uses its dedicated RWO claim.
Base replicas are zero; Git refs and the RWO storage class remain fail-closed
placeholders. The DB and evaluator overlays are review inputs, not automatic
authorization.

## Safety boundaries

- Discovery and reporting commands are read-only.
- Development validation has no preview/local-test mode: `validate` and `run`
  require explicit submission plus exact confirmation. `plan` is read-only
  queue inspection.
- Health verdicts never delete, cordon, taint, reboot, or restart nodes.
- `scripts/cval-backup.sh` provides a nonwriting capacity check; apply requires exact
  `--apply --confirm backup` and never deletes.
- `scripts/cval-split-classifications.py` provides nonwriting inspection; apply requires
  exact confirmation and a backup manifest containing the source DB.
- No manifest in this repository is applied or scaled automatically.
