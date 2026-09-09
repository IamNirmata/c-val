# PVC Layout and Legacy Archive

Root: `/data/continuous_validation`. Inspected September 9, 2026.
Canonical evidence paths remain stable; this is an archive-only cleanup, not a
raw database migration. The first live move requires separate approval.

## Retained Paths

| Directory | Role | Why retained |
|---|---|---|
| `metadata/` | Seven authoritative raw SQLite DBs and stable ingestion lock | Configured writer/read paths |
| `validation_tests/` | Current per-test results, summaries and metric artifacts | Absolute paths in canonical results and DL receipts |
| `logs/` | Canonical run results, digest markers, events and workload logs | Evidence for raw writes and independent evaluation |
| `evaluation-engine/` | Derived evaluation DB and persistent worker lock | Active CPU evaluator; pod alias `/evaluation` |
| `deep-learning-unit-test-main/` | Current DL benchmark source dependency | Configured even though its directory timestamp is old |
| `backups/` | Recovery snapshots and journal-migration manifests | Retained rollback evidence; not scanned in full |
| `dltest/` | Historical DL artifacts | 1,604 stored run receipts per DL DB still reference it |
| `nccl/`, `storage/`, `results/` | Historical test artifacts/results | Legacy evidence/recovery paths remain supported |

Do not rename these to `raw_test_results`, `evaluations`, etc. in place:
absolute paths are embedded in immutable results and DB provenance. Symlink
aliases are rejected by canonical-path checks. Renaming active storage needs
a separately designed evidence migration, not search-and-replace in config.
See [raw-database-design.md](raw-database-design.md) and
[evaluation-database-design.md](evaluation-database-design.md).

## Archive Allowlist

Only these root entries may move into `archive/YYYYMMDD_HHMMSS_PDT/` (or PST):

| Old name | Archived name | Meaning |
|---|---|---|
| `baselines/` | `retired_baselines_and_classifications/` | Superseded baseline/classification stores and snapshots |
| `deeplearning_unit_test/` | `legacy_dl_source_and_results/` | Old source copy and early result files |
| `dltest.tar.gz` | `legacy_dl_results.tar.gz` | Old DL bundle |
| `old-files/` | `legacy_bundles/` | Previously retired source/result bundles |
| `test1/` | `legacy_scratch_tests/` | January scratch-test material |

Current thresholds and classifications are tables inside the derived
`evaluation-engine/evaluation.db`, not the retired `baselines/` tree.
Moving directories preserves their contents and does not reclaim PVC capacity.

## Tool and Gates

[scripts/cval-pvc-layout.py](../../scripts/cval-pvc-layout.py) defaults to a
nonwriting, bounded metadata inventory. `--receipts` reads only the four small
DL receipt tables with `mode=ro` and `query_only=ON`; no large DB scan/hash.

```bash
python3 scripts/cval-pvc-layout.py --root /data/continuous_validation --receipts
python3 scripts/cval-pvc-layout.py --root /data/continuous_validation \
  --plan-archive --archive-name YYYYMMDD_HHMMSS_PDT
```

Execute inside the PVC reader when the path is not locally mounted. Review
the plan and record its `plan_sha256`. Apply requires that exact digest plus
`--apply-archive --confirm archive-legacy --confirm-sources-idle idle`.
The idle confirmation covers all consumers, not just cval-live; pod-spec
reference checks cannot establish absence of open files or hidden callers.

The tool refuses referenced sources, unknown receipt coverage, symlinks,
mount boundaries, changed top-level identities and destination collisions.
One persistent POSIX archive lock serializes operations. Linux no-overwrite
renames preserve inodes; `manifest.json` records each successful move.
It does not inspect every descendant or prove full-content immutability.
Partial inventory sizes are lower bounds, never proof that a tree is unused.

Rollback uses `--rollback-archive --archive-name <same-name>` with
`--confirm restore-legacy --confirm-sources-idle idle`, under separate approval.
It handles partially completed moves, refuses changed/missing sources or
occupied original names, and retains the manifest. Never remove archive or
ingestion lock files while participants can run.

No automatic pruning, row edits, raw-path aliases, or evaluator restart is
part of this operation. Publish code first; restart cval-live separately at the
latest approved main commit, retaining its configured Pending timeout/pruning.