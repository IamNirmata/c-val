# C9: Recovery and Maintenance

Status: proposed redesign. Baseline/counting: [overview](major-components-overview.md#measurement-method).

## Overview

Protect retained evidence during backup, journaling changes, layout changes and
recovery. These are operator-invoked maintenance tools, never automatic phases
of validation or cleanup side effects of a code release.

## Details and Purpose

Read-only inspection consumes filesystem/receipt metadata and reports capacity,
coverage and constraints. A mutation additionally needs a reviewed exact plan,
quiescence, verified backup, explicit action approval and a rollback procedure.
Output is verified preserved evidence and a durable operation manifest.

## Current Process Flow

```mermaid
flowchart LR
    Inspect[Bounded read-only inventory] --> Plan[Review identities capacity and references]
    Plan --> Approval[Exact approval and quiescence]
    Approval --> Backup[Create and verify backup]
    Backup --> Change[Approved migration or move]
    Change --> Verify[Verify and retain manifest]
    Verify -. separately approved .-> Restore[Recovery or rollback]
```

## Codebase Size

| Kind | Files | Physical Lines |
| --- | ---: | ---: |
| Code | 4 | 2,389 |
| Configuration | 1 | 80 |
| Tests | 3 | 1,146 |

Counted: [backup](../../scripts/cval-backup.py) and its
[launcher](../../scripts/cval-backup.sh), [PVC layout](../../scripts/cval-pvc-layout.py),
[SQLite journal](../../scripts/cval-sqlite-journal.py),
[PVC access template](../../ymls/gcr-admin-pvc-access.yml), and the
[backup](../../tests/test_backup.py), [layout](../../tests/test_pvc_layout.py),
[journal](../../tests/test_sqlite_journal.py) suites. Shared filesystem/SQLite
utilities are counted once under C5/C7.

## Functionality Assessment

| Category | Functionality | Decision |
| --- | --- | --- |
| Critical | Capacity/quiescence gates, verified backups, identity checks, rollback manifests | Preserve despite infrequent use. |
| Good | Bounded inventory, receipt-reference checks, explicit partial-failure reports | Keep read-only by default. |
| Not important | Cosmetic archive organization | Defer; moving files does not reclaim storage. |
| Useless | Unused recovery safeguards | None proven. Lack of per-cycle use is not evidence of dead code. |

## Current Issues

1. Tools have different plan/manifest structures. Reconstructing an end-to-end
   restore from isolated successes requires operator knowledge.
2. The old layout note described September 9 paths as current even after code
   adopted consolidated log paths. Dated inventory must not guide new mutations.
3. Quiescence includes every consumer; a stopped loop or absent pod reference
   alone does not prove no process holds a file open.
4. Whole-tree inventory/hashing can be costly. Partial counts must be labeled
   lower bounds, never evidence that a directory is empty or unused.

## Proposed Design

Keep the separate tools and approval gates. Standardize only shared identity,
plan-hash and verification fields that reduce real duplication; do not create
a universal destructive-operation engine. Add offline end-to-end backup,
journal migration and restore fixtures alongside the current tests.

Give every operation an explicit staged state and durable receipt. Resume or
rollback must validate the recorded identities, backup manifest and destination
collisions before touching data. No auto-prune, automatic restore or metadata
rewrite belongs in the controller.

## Retained Paths

Configured/current code targets at the design baseline, not a new live PVC audit:

| Path Relative to Validation Root | Responsibility |
| --- | --- |
| `metadata/` | Seven authoritative stores and persistent ingestion lock. |
| `logs/job_logs/<node>/<run-id>/` | Canonical results, digests and global logs/events. |
| `logs/<test>/<node>/<run-id>/` | Per-test logs and events. |
| `logs/validation_tests/<test>/runs/<node>/<run-id>/` | Test summaries and metric artifacts. |
| `deep-learning-unit-test-main/` | External benchmark source dependency. |
| `backups/`, `archives/` | Retained recovery snapshots, moves and manifests. |
| Historical `validation_tests/`, `dltest/`, `nccl/`, `storage/`, `results/` | Retain referenced provenance; use reviewed relocation-aware readers. |
| Historical downstream data, including `evaluation-engine/` | Preserve despite removal of its source; no supported service is implied. |

Absolute paths may be embedded in immutable results and receipts. No in-place
rename, symlink alias, search-and-replace or deletion is authorized by this design.
The former September 9 note recorded 1,604 historical DL run receipts per DB
referencing `dltest/`; that count is historical, not a current inventory.

## Archive Allowlist

Carried forward from the old layout note and checked against the current tool:

| Source Entry | Destination Name |
| --- | --- |
| `baselines/` | `retired_baselines_and_classifications/` |
| `deeplearning_unit_test/` | `legacy_dl_source_and_results/` |
| `dltest.tar.gz` | `legacy_dl_results.tar.gz` |
| `old-files/` | `legacy_bundles/` |
| `test1/` | `legacy_scratch_tests/` |

The current tool uses `archives/<timestamp>/`, not the former note's singular
directory. Allowlisting is not approval: references, identities, mount boundaries,
collisions and source-idle conditions still gate every move.

## Tool and Gates

Follow the [operations runbook](../operations-runbook.md) for commands. Layout
inspection and `--receipts` are bounded, nonwriting reads. Archive application
requires the reviewed `plan_sha256`, exact archive confirmation and source-idle
confirmation. Rollback requires its own approval and restore confirmation.

Persistent POSIX locks and no-overwrite renames protect supported operations;
never unlink a lock while participants may run. Archive identity checks are not
full descendant-content verification. Whole-root backup additionally requires
capacity, quiescence and manifest verification. Journal migration verifies the
four-DB generation and backups before changing journal mode. DB audits use
`mode=ro` and `query_only=ON`.

## Acceptance and Risk

Verify restored raw rows, receipts, artifacts, metadata and identities. Exercise
insufficient capacity, active writers, tampered backups, partial moves, missing
sources and occupied rollback destinations. Record bounded/partial coverage.
Priority P2; low risk for offline test additions, high risk for any live mutation.
