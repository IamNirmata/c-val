# c-val current status and remaining live actions

Updated: 2026-08-03

## Local implementation complete

- The working scheduler, renderer, generic runner, descriptor-anchored
  supervisor, current raw DB writers, DL metric rebuild, and baseline engine are
  retained.
- Normalized U6 history and its CLI/config/script/docs/tests are removed.
- Alternate U7 per-test persistence/plugin ingestion is removed.
- Alternate U8/U9 health classes, keys, candidate chain, and CLI/config are
  removed.
- U11 custom image, wheelhouse, parity/preflight/service/local-copy stack, and
  old deployment overlays are removed.
- U12 compatibility inventory/audit and no-removal-period machinery are removed.
- Required current built-in constants moved to `cval.validation.builtins`.
- New results emit `cval.results`; historical v1/v2 readers remain.
- `cval.baselines` is the sole storage/DL evaluator with median/MAD and
  normal/degraded/improved outputs.
- Classification persistence is per target, and readers merge those files.
- One source-pulling always-on CPU `cval-evaluator` Deployment is prepared
  locally. It owns all recurring SQLite and PostgreSQL evaluator tasks; base
  replicas remain zero.
- Whole-root nonwriting capacity inspection and global-classification split preparation
  scripts are implemented and locally tested.

## Canonical DBs

Raw:

- `metadata/validation.db`
- `metadata/test-storage.db`
- `metadata/test-nccl.db`
- four `metadata/dltest_*.db` component files

Derived:

- one SQLite baseline DB per storage/DL component
- one SQLite classification DB per operational storage/DL target
- optional NCCL PostgreSQL baseline versions and evaluations under the three
  NCCL schemas; raw SQLite NCCL evidence remains retained

The former global `baselines/classification-results.db` is retained live until a
separately approved backup and split/cutover.

## Remaining live actions — not performed

1. Review this local diff and select/push an immutable Git commit.
2. Replace all-zero NCCL Git refs, select the dedicated RWO storage class, and
  retain disposable PostgreSQL restart evidence. The complete Python 3.12
  hash lock and clean pinned-image bootstrap are already verified.
3. Read-only verify the live namespace, PVC claim, current pod/controller,
   process manager availability, storage capacity, and raw/global DB sizes.
4. Approve and execute a whole `continuous_validation` backup with
   `scripts/cval-backup.sh --apply --confirm backup`; retain and verify its
   manifest/checksums and restore plan.
5. Inspect the classification split against an explicit copy and compare row
   counts/latest exports.
6. Approve and execute the split with exact confirmation and backup evidence.
7. Discover old evaluator tmux loops and any previously deployed evaluator
  CronJobs/controllers. Stop or suspend them with separately approved exact
  commands before scaling the resident Deployment; ordinary apply does not
  prune removed manifests.
8. Deploy `cval-evaluator` at one replica after PostgreSQL is ready and old
  writers are stopped; verify checkout, package install, schema/role init, PVC
  visibility, readiness, and read-only exports.
9. Run one manual storage/DL baseline build/classify comparison through the
  resident pod. Do not start separate evaluator loops alongside it.
10. Update readers/config to the renamed live workload in the same controlled
   cutover; do not delete historical artifacts or the old global DB.

Every write/apply/restart/delete step above requires separate explicit operator
approval. No live Kubernetes, PVC, network, backup, split, apply, restart,
commit, or push was performed by this local simplification.
