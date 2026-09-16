# Operations runbook

## Read-only checks

```text
cval nodes --output json
cval status --output json
cval results --test overall --type csv
cval results --test storage --type csv
cval results --test nccl --type csv
```

These commands do not mutate Kubernetes or SQLite.

## Continuous validation loop

Every operational `scripts/cval-live.sh` cycle is submit-capable and requires
exact `CVAL_LIVE_CONFIRM=submit`. Each submission also passes the CLI's
independent `--submit --confirm submit` gate with an exact published commit.

Use the script's `start`, `status`, and `stop` commands for the persistent tmux
`run-once` and `run-loop` require the same environment confirmation. Stopping
the session does not delete Kubernetes jobs, and pending-job pruning requires
the separate exact `CVAL_PRUNE_CONFIRM=delete-pending` gate.

## Exact-commit validation

The validation command requires an eligible node, exact published commit,
explicit submit flag, and exact confirmation. The validation pod checks out
that commit, runs enabled storage, NCCL, and DL tests, validates canonical
evidence, and writes raw SQLite databases. No evaluation or node-health
assignment follows ingestion.

The continuous loop uses these same exact-commit submission gates for every
job.

The configured source is the internal `msr-central/c-val` repository. Jobs without
repository credentials can use an approved immutable source ConfigMap rendered
from the published commit by `deploy/evaluation-engine/render.py`. In the
operator's effective `[job]` configuration, set `source_configmap` to that
ConfigMap name and `source_sha256` to the SHA-256 of its `source.tar.gz` bytes.
Both fields must be supplied together with the exact commit. The runtime checks
the archive hash and Git archive commit header before extracting into a fresh
checkout directory. It then runs the same descriptor-anchored supervisor.
Publishing the ConfigMap and submitting the Job remain separate gated operations.
Do not put personal Git credentials in a ConfigMap, Pod environment, or manifest.

## Raw DL reconciliation

`db-rebuild-dltest-metrics` can append missing raw DL metric evidence from
retained rank JSON. It does not calculate verdicts or delete rows.

## Whole-root backup

`scripts/cval-backup.sh` defaults to a nonwriting capacity preview. Apply is a
separate live write requiring a reviewed destination, stopped source
writers, exact backup confirmation, and exact quiescence confirmation.

An external destination such as `/data/cval-backup` includes the entire source
tree, including any existing `backups/` directory. Only the default destination
inside the source (`backups/`) excludes itself to avoid recursion. A sibling
directory on the same PVC is a rollback copy, not independent disaster recovery.
The tool creates the external destination during approved apply, not preview.

Use `--preserve-history-bytes` when retained `backups/`, `archive/`, or `archives/`
contains old journals or incomplete database snapshots. Those historical trees
are then copied and SHA-256 verified byte-for-byte, without SQLite recovery or
integrity claims. Current databases, including `metadata/`, `evaluation-engine/`
and `dl_unit_test_evaluation/`, still reject sidecars and require quiescence.
Hardlinks across current and historical trees are rejected. Version 2 manifests
record this policy; verification continues to accept earlier version 1 backups.

Verification is read-only and validates the published manifest, checksums,
hardlinks, metadata, and current SQLite integrity. Byte-preserved historical
snapshots are verified as retained evidence, not as recovered databases.
Backup never restores or deletes source data.

## PVC organization

Use [pvc-layout.md](components_design/pvc-layout.md) for the retained-path map,
bounded inventory, and earlier archive history. The current code writes new
detailed test evidence under `logs/validation_tests`; existing PVC contents are
not migrated automatically by a code update. No compatibility symlinks are used.

| Existing location | Consolidated location |
|---|---|
| `archive/` | `archives/` |
| `dltest/`, `nccl/`, `storage/` | Matching directories under `logs/` |
| `results/` | `logs/results/` |
| `validation_tests/` | `logs/validation_tests/` |

`logs/job_logs` contains canonical overall results and supervisor logs.
`logs/<test>` contains per-test stdout, stderr and events; legacy per-test trees
contain older run records. `logs/validation_tests/<test>/runs` contains detailed
summaries, artifacts, retained plans and rank JSON. These are different evidence
types, not interchangeable copies. They share one `logs/` root after migration.
`metadata/`, `evaluation-engine/`, and `dl_unit_test_evaluation/` are not moved.
The protected `dl_unit_test_evaluation/` tree is fingerprinted before and after
consolidation; its files are read for backup/audit but never edited by migration.

### Approved consolidation workflow

Use an exact published checkout for these operator templates. Resolve every
placeholder and separately approve the exact command and rollback before each
live mutation; these examples are not evidence that a migration has run.

1. Inventory with `scripts/cval-pvc-layout.py --receipts`. Identify all writers,
	including the live loop, validation jobs, evaluator, and interactive clients.
	A blocked loop or a Kubernetes Ready condition is not proof of quiescence.
2. Stop writers under separate approval. Retain the exact evaluator Pod spec
	and ConfigMaps for restart, removing scheduler-assigned `nodeName` from a
	recreate manifest. Do not force-delete an active writer or start a second one.
3. Inspect capacity, create the complete backup, then verify its manifest:

	```bash
	bash scripts/cval-backup.sh --source /data/continuous_validation --destination-root /data/cval-backup --preserve-history-bytes
	bash scripts/cval-backup.sh --source /data/continuous_validation --destination-root /data/cval-backup --preserve-history-bytes --apply --confirm backup --quiesced --confirm-quiesced writers-stopped
	bash scripts/cval-backup.sh --verify /data/cval-backup/<verified-backup>
	```

4. Generate and review the full consolidation plan. Differing file content,
	metadata or entry types are collisions and block apply. Preserve both copies
	and review a revised plan; do not overwrite either to make the check pass.

	```bash
	python3 scripts/cval-pvc-layout.py --root /data/continuous_validation --plan-consolidate
	python3 scripts/cval-pvc-layout.py --root /data/continuous_validation --apply-consolidate --backup-dir /data/cval-backup/<verified-backup> --plan-sha256 <reviewed-sha256> --confirm consolidate --confirm-sources-idle idle
	```

	Apply verifies the backup and unchanged source inventory again. Whole trees
	move without replacement and their identities/checksums are rechecked.
	Byte-and-metadata-identical duplicate files are retained in a private sibling
	`<verified-backup>-layout/duplicates/` directory, outside the cleaned root.
	Only empty original directories are removed. The original backup remains
	untouched and independently verifiable. A failed move retains its manifest
	for rollback; it does not authorize continuing with a partial layout.
5. Audit all planned destinations, unchanged raw DB evidence, and protected
	directory fingerprints. Historical receipts/result JSON retain original
	absolute paths; the explicit evidence resolver and artifact downloader
	recognize relocated paths without rewriting those records.
6. Restart only the reviewed exact-commit writers with separate approval. Follow
	[Exact-commit validation](#exact-commit-validation) on one eligible node, with
	a real published SHA and separate submission approval. Retain terminal job
	state, canonical result files, logs and exact-timestamp raw SQLite rows.
	Verify the evaluator's separate progress and coverage states; do not equate
	`unclassified` with a failed test.

Rollback, while all writers are stopped and before new evidence changes the
moved trees, restores only the recorded moves without overwriting newer data:

```bash
python3 scripts/cval-pvc-layout.py --root /data/continuous_validation --rollback-consolidate --transaction /data/cval-backup/<verified-backup>-layout --confirm restore-layout --confirm-sources-idle idle
```

Rollback does not restore database contents or prune backups. Restoring a database
snapshot is a different, separately approved operation. Preserve the persistent
lock files and both backup/migration manifests.
