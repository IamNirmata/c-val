---
name: c-val-hpc-engineer
description: Operate direct-cluster c-val validation, current raw databases, robust baselines, classifications, and the audit live loop.
---

# c-val operator contract

## Current pipeline

1. Discover fully free schedulable GPU nodes.
2. Read freshness from `metadata/validation.db` → `latest_status`.
3. Prioritize never-tested, then oldest stale nodes.
4. Run exact-commit Volcano validation jobs; submission remains double-confirmed.
5. Run registry-ordered storage, NCCL, and DL tests.
6. Ingest current raw databases under `metadata/`.
7. Build storage/DL median/MAD baselines and classify
  normal/degraded/improved; evaluate NCCL only through its PostgreSQL worker.

There is no normalized run-history DB, alternate per-test U7 store, U8/U9
health-class engine, evaluator key/state root, or shadow-parity service.

## Canonical databases

Raw:

- `metadata/validation.db`
- `metadata/test-storage.db`
- `metadata/test-nccl.db`
- four `metadata/dltest_*.db` component DBs

Derived:

- one baseline DB per storage/DL component under `baselines/`
- one classification DB per operational storage/DL target
- NCCL baseline versions and evaluations in the optional PostgreSQL schemas
- retained `baselines/classification-results.db` is read-only fallback until a
  separately approved backed-up split is complete

## Safety

- `cval-live` defaults to audit mode and performs zero cluster mutations.
- Submit requires `CVAL_LIVE_MODE=submit` and exact
  `CVAL_LIVE_CONFIRM=submit`; pruning separately requires
  `CVAL_PRUNE_CONFIRM=delete-pending`.
- Health verdicts never mutate nodes.
- Whole-root backup starts with a nonwriting capacity check. Apply requires exact backup and quiescence
  confirmations; verify the produced manifest before migration.
- Classification split provides nonwriting inspection and requires authenticated backup
  evidence. Never delete the retained global DB or historical artifacts.
- The source-controlled resident evaluator Deployment remains at zero replicas
  and has fail-closed commit/storage placeholders; reviewed NCCL images and
  wheels are pinned.

## Common commands

```bash
python -m cval.cli status --output json
python -m cval.cli nodes --output table
python -m cval.cli validate --node <node> --git-ref <40-hex-commit> --submit --confirm submit
python -m cval.cli baseline build --test-type storage
python -m cval.cli baseline classify --test-type storage --store-results
python -m cval.cli classifications --test all --type csv
bash scripts/cval-live.sh status
bash scripts/cval-backup.sh --help
```

Use one exact-commit cluster validation job as development acceptance; local
unit/compile/registry/render suites are not a pre-publish gate. Kubernetes apply,
unsuspension, PVC backup/migration, pod replacement, and destructive cleanup
require separate explicit approval.
