# Operations runbook

## Read-only checks

```text
cval nodes --output json
cval status --output json
cval results --test overall
cval results --test storage
cval results --test nccl
```

These commands do not mutate Kubernetes or SQLite.
Use `--config <reviewed-file>` before the command to select namespace, PVC
reader and database paths. Result exports are always CSV; configuration output
is always JSON. Status supports table/JSON, not TSV.

## Candidate inspection

Plain `plan` uses the existing priority queue without rendering Jobs.
Its JSON has `batch_size`, `days_threshold`, `free_nodes_count`, `queue_count`,
`git_ref`, and `candidates`. Each candidate has `node`, `priority`, `reason`,
`last_tested_timestamp`, and `age_days`; there is no Job name or YAML.
`queue_count` covers all due candidates; `--batch-size` limits the returned list.
With explicit `--free-nodes`, the input list is not proof of current eligibility.

Planning requires an exact commit. It does not validate a Job template or
authorize submission. Fresh eligibility, template/bootstrap validation and
exact-source checks still run before creating the selected Job through the
confirmed `run` path.

`candidates` replaces the former `planned_jobs` output; there is no rendered-plan
inspection mode. See the [CLI migration inventory](components_design/cli-design.md#removed-settings-and-migration)
for retired flags and their replacements.

## Continuous validation loop

Every operational `scripts/cval-live.sh` cycle is submit-capable and requires
exact `CVAL_LIVE_CONFIRM=submit`. Each submission also passes the CLI's
independent `--submit --confirm submit` gate with an exact published commit.

Use the script's `start`, `status`, and `stop` commands for the persistent tmux
session.
`run-once` and `run-loop` require the same environment confirmation. Stopping
the session does not delete Kubernetes jobs, and pending-job pruning requires
the separate exact `CVAL_PRUNE_CONFIRM=delete-pending` gate.

Preflight and per-slot inspections use plain `plan`: inventory,
history and cooldowns produce an ordered candidate list without rendering Jobs.
The loop checks candidates in that order and renders one selected eligible node
through the existing confirmed `run` path. Input failures still stop the cycle;
an empty due queue is distinct from unavailable or malformed evidence.

Retained `preflight-plan.json` and `plan-slot-*.json` now contain `candidates`
and a top-level exact `git_ref`, not `planned_jobs`. The plan log field is
`candidate_count`, not `planned_count`. Historical files are not rewritten.

## Exact-commit validation

The validation command requires an eligible node, exact published commit,
explicit submit flag, and exact confirmation. The validation pod checks out
that commit, runs enabled storage, NCCL, and DL tests, validates canonical
evidence, and writes raw SQLite databases. No evaluation or node-health
assignment follows ingestion.

The continuous loop uses these same exact-commit submission gates for every
job.

Selected-node validation remains `cval validate --node <node>` with an exact
commit and explicit submission confirmation. Namespace, reader and evidence
paths come from the configuration. Optional ZIP downloading has been removed;
the command still reports results and verifies fresh status/ingestion, while
canonical logs, summaries, JSON and raw databases remain on the PVC. Existing
downloaded archives are not deleted. Older retained runbooks describe their
pinned revisions; do not replay their removed flags against the new CLI.

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

## Quick test workflow

Use this staged smoke test before deeper framework acceptance. It reuses existing
commands and the scheduler-selected template, without a new CLI mode or reduced
benchmark profile. "Quick" avoids pinning a busy node; it does not guarantee quick
scheduling. Volcano can still queue the Job for capacity or queue-policy reasons,
and the full configured storage, NCCL and DL tests still take their normal time.

| Stage | Required evidence | Mutation |
| --- | --- | --- |
| Discovery | Inventory and current resource discovery JSON | None |
| Preparation | Exact-source scheduler-selected manifest and run identity | Local files only |
| Validation | Assigned node, terminal success, canonical logs/results and exact-run raw rows | One separately approved Job and normal ingestion |
| Deeper acceptance | Targeted validation and first two fresh live-loop runs | Separate approvals |

### 1. Test discovery without submission

Run from the exact published checkout. Set `CVAL_CONFIG` to the reviewed operator
TOML and `CVAL_GIT_REF` to the published lowercase 40-hex SHA. Verify remote source
identity and effective config first. Retain a fresh local evidence directory:

```bash
set -euo pipefail
: "${CVAL_CONFIG:?Set the reviewed config path}"
: "${CVAL_GIT_REF:?Set the exact published commit}"
export PYTHONDONTWRITEBYTECODE=1
export CVAL_QUICK_TIMESTAMP="$(date +%s)"
export CVAL_QUICK_DIR="$PWD/run-logs/quick-test-${CVAL_QUICK_TIMESTAMP}"
mkdir -p "$PWD/run-logs"
mkdir "$CVAL_QUICK_DIR"
timeout --foreground 150s env CVAL_KUBECTL_TIMEOUT_SECONDS=120 python3 -m cval.cli --config "$CVAL_CONFIG" nodes --inventory-only --output json > "$CVAL_QUICK_DIR/inventory.json" 2> "$CVAL_QUICK_DIR/inventory.stderr"
timeout --foreground 390s env CVAL_KUBECTL_TIMEOUT_SECONDS=120 python3 -m cval.cli --config "$CVAL_CONFIG" nodes --output json > "$CVAL_QUICK_DIR/discovery.json" 2> "$CVAL_QUICK_DIR/discovery.stderr"
```

Both commands are read-only. Require successful exit codes and valid JSON before
continuing. Inventory alone does not test pod/resource discovery. Zero eligible
capacity is a valid observation, not a failed read or a capacity reservation.
Forbidden access, timeouts and malformed output leave discovery **unverified**;
retain stderr and stop the combined workflow. An independently approved
scheduler-only diagnostic must not be reported as a full quick-test pass.

### 2. Prepare and approve one Job

Use the [scheduler-selected rendering step](#scheduler-selected-test-job) below
to create `job.yaml` and `identity.json` locally. Keep the image, resources,
enabled tests and test settings identical to the intended live configuration.
Do not change the main loop's configured node-specific template.

Review the rendered namespace, queue, resources, mounts, source identity and lack
of node pinning. Require an unused Job name/timestamp and a responsive evidence
reader. If using a source ConfigMap, its creation and hash/embedded-commit
verification are separate prerequisites; rendering does not create it.

Inspect the existing loop and active Jobs before proposing live operations.
Record the literal manifest path, Job name, namespace and exact commit in a
release-specific approval document, with separate exact create and conditional
graceful-cancellation commands. Direct Kubernetes creation does not pass through
the CLI's submission policy checks, so explicitly review namespace/resource
policy too. Do not submit the unrendered YAML, delete another Job, or pause/restart
the loop based solely on this workflow. Approval of the workflow is not approval
of a particular cluster mutation.

### 3. Observe and verify the quick test

After the separately approved creation, use the existing `jobs` watcher on the
recorded Job name, with a bounded deadline. Inspect the Job and pod during the
watch and retain their state and server logs. Get the actual node from the pod's
`spec.nodeName`, not from the Job name, which contains `scheduled`. An unassigned
pod has no validated node identity yet. Do not invoke targeted validation on the
assigned node as an inspection step: that would create another Job.

The quick test passes only when **all** checks below pass:

- Inventory and resource discovery succeeded; inaccessible, empty and unavailable
	states remained distinct. This verifies reads, not the live scheduler's decisions.
- The Job is Completed/Succeeded and the server container exited 0. A watch exit
	code or `terminal=true` alone is insufficient: Failed, Aborted and Missing are
	terminal too. Pending, timeout and unknown state are not passes.
- Canonical `logs/job_logs/<node>/<node>-<timestamp>/result.json` has the exact
	node, timestamp, source SHA and effective config identity, with all enabled tests
	passing. Global/per-test logs, events, summaries and artifacts are readable.
	Require `ingestion_finished=pass` after test completion and a matching result
	digest; a passing test result before ingestion is not completion.
- Strict read-only audit connections (`mode=ro`, `PRAGMA query_only=ON`) confirm
	exact-run rows in all seven raw DBs for the full built-in profile: four passing
	status rows, storage/NCCL metrics, and complete DL metrics/ranks, receipts and
	consistent generations. Expected metric counts come from the reviewed profile;
	older latest-status rows or disabled tests cannot substitute for fresh evidence.

Retain the audit locally without modifying canonical evidence. On failure or
timeout, stop promotion and preserve logs/results. Watching stops tracking, not
the Job. Only the separately approved exact cancellation may remove that Job;
never force-delete, change finalizers or clean up raw evidence. Use a new timestamp
for a subsequent attempt.

### 4. Promote to deeper acceptance

A quick pass covers discovery reads and scheduler-selected execution/ingestion,
not priority selection, targeted eligibility, cooldowns, slots or resume behavior.
Keep the [targeted exact-commit acceptance](#exact-commit-validation) gate, then
separately approve live-loop start/restart at the current published branch tip.
If source or effective test settings change, do not reuse the previous acceptance.

Review the first two fresh loop-submitted runs: priority/eligibility decisions,
worker source, canonical logs/results and exact-run raw ingestion. Historical
resumed Jobs do not count. No eligible capacity leaves deeper run verification
open; do not weaken resource/placement checks. Nothing here automatically starts
or restarts cval-live.

## Scheduler-selected test job

[ymls/test-job.yml](../ymls/test-job.yml) exercises Volcano submission, validation
tests and raw ingestion without c-val node discovery or prioritization. It has
no node selector, node affinity or pod affinity, and does not tolerate cordoned
nodes. Volcano chooses a node with the requested GPU/CPU/memory/RDMA resources.
The actual node comes from the Downward API; Kubernetes expands it into the
canonical run ID. The job name uses `scheduled`, not a claimed target node.

Render explicitly with `node_name=None`; the ordinary node-specific workflow
does not accept this template. Both templates use the same exact-source
bootstrap, runtime configuration and descriptor-anchored supervisor. The renderer
replaces `runtime-bootstrap-placeholder` with the packaged
[bootstrap script](../cval/jobs/bootstrap.sh) before validating the manifest.
The generated YAML is self-contained; the raw template is not directly deployable.
Use the [quick test preparation](#quick-test-workflow) to set the exact published
commit, reviewed config, fresh timestamp and local evidence directory. Configure
`job.source_configmap` and `job.source_sha256` when using the internal repository
without in-pod credentials. This step only writes local files for review:

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path

import yaml
from cval.config import is_exact_commit, load_config
from cval.jobs.renderer import render_validation_job_from_file

revision = os.environ["CVAL_GIT_REF"]
if not is_exact_commit(revision):
	raise ValueError("An exact nonzero published commit is required")
directory = Path(os.environ["CVAL_QUICK_DIR"])
inventory = json.loads((directory / "inventory.json").read_text())
discovery = json.loads((directory / "discovery.json").read_text())
if not isinstance(inventory.get("nodes"), list) or not isinstance(discovery.get("nodes"), list) or not isinstance(discovery.get("totals"), dict):
	raise ValueError("Successful inventory and resource discovery evidence required")
config = load_config(os.environ["CVAL_CONFIG"])
job = render_validation_job_from_file(
	Path("ymls/test-job.yml"),
	node_name=None,
	timestamp=int(os.environ["CVAL_QUICK_TIMESTAMP"]),
	git_ref=revision,
	cval_config=config,
	job_template_config=config.job_template,
)
manifest = yaml.safe_load(job.yaml_text)
if manifest["metadata"]["namespace"] != config.cluster.namespace:
	raise ValueError("Rendered namespace differs from the reviewed cluster namespace")
identity = {
	"job": job.job_name,
	"pod": job.job_name + "-server-0",
	"namespace": config.cluster.namespace,
	"timestamp": job.timestamp,
	"git_ref": job.git_ref,
	"source_configmap": config.job.source_configmap,
	"source_sha256": config.job.source_sha256,
	"submitted": False,
}
with (directory / "job.yaml").open("x", encoding="utf-8") as handle:
	handle.write(job.yaml_text)
with (directory / "identity.json").open("x", encoding="utf-8") as handle:
	json.dump(identity, handle, indent=2)
print(json.dumps(identity, indent=2))
PY
```

Continue through the quick workflow's approval and evidence gates. Scheduler
selection does not replace targeted eligibility or prioritization in the main loop.

## PVC-access pod

[ymls/gcr-admin-pvc-access.yml](../ymls/gcr-admin-pvc-access.yml) is the canonical
CPU utility pod, with the existing claim mounted writable at `/data`. Its image
is digest-pinned to the observed Python 3.12 image. It starts without package
downloads or a Git clone; Python and standard-library SQLite are ready for c-val
read-only status/export queries. NumPy, pandas and a repository checkout are not
installed automatically. Install additional interactive tools explicitly if
needed; only `/data` is persistent.

The pod keeps the `pvcaccess` container name, requests 2 CPU/2 GiB, and limits
memory to 8 GiB. `/workspace`, `/tmp`, and `/root/.vscode-server` are disposable
volumes. The exec readiness probe checks the Python/SQLite runtime, not PVC
latency or node health. No liveness probe interrupts long interactive tasks.
No API token is mounted. The pod does not read or modify PVC contents at startup.

Replacement interrupts exec/VS Code sessions and discards pod-local tools and
workspace files, but retains the PVC. Save the original spec without status,
server-managed metadata or scheduler-assigned `nodeName` for rollback. Separately
approve graceful deletion, creation from the canonical manifest, and conditional
rollback. Do not force-delete an unresponsive pod or change/recreate the PVC.
Verify a bounded Python/SQLite exec and read-only c-val status query after Ready.

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

When `/data/continuous_validation/dl_unit_test_evaluation` must remain outside
the operation, pass `--exclude-dl-unit-test-evaluation` to both preview and apply.
Only that exact top-level directory is excluded, before any stat, traversal,
hashing, or copying. Its files may change independently; they are not covered by
this backup. Similarly named directories and historical copies elsewhere remain
included. Version 3 manifests record the exclusion; verification still accepts
version 1 and 2 backups. Hardlinks escaping the included tree remain rejected.

Use `--preserve-history-bytes` when retained `backups/`, `archive/`, or `archives/`
contains old journals or incomplete database snapshots. Those historical trees
are then copied and SHA-256 verified byte-for-byte, without SQLite recovery or
integrity claims. All included current databases, including `metadata/` and
`evaluation-engine/`, still reject sidecars and require quiescence. An excluded
`dl_unit_test_evaluation/` tree is not inspected. Hardlinks across current and
historical trees are rejected. The historical-byte policy is recorded in the
manifest separately from exclusions.

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
When the verified backup excludes `dl_unit_test_evaluation/`, consolidation skips
its content and fingerprint checks too. Rollback never touches that directory.
Without the explicit exclusion, the earlier fingerprint checks still apply.
Consolidation accepts no other omitted source directories, including `backups/`.

### Approved consolidation workflow

Use an exact published checkout for these operator templates. Resolve every
placeholder and separately approve the exact command and rollback before each
live mutation; these examples are not evidence that a migration has run.

1. Review `scripts/cval-pvc-layout.py --plan-consolidate` and backup preview with
	`--exclude-dl-unit-test-evaluation`; avoid whole-root inventory/content audits
	when the excluded directory must not be inspected. Identify all included writers,
	including the live loop, validation jobs, evaluator, and interactive clients.
	A blocked loop or a Kubernetes Ready condition is not proof of quiescence.
2. Stop writers under separate approval. Retain the exact evaluator Pod spec
	and ConfigMaps for restart, removing scheduler-assigned `nodeName` from a
	recreate manifest. Do not force-delete an active writer or start a second one.
3. Inspect capacity, create the complete backup, then verify its manifest:

	```bash
	bash scripts/cval-backup.sh --source /data/continuous_validation --destination-root /data/cval-backup --preserve-history-bytes --exclude-dl-unit-test-evaluation
	bash scripts/cval-backup.sh --source /data/continuous_validation --destination-root /data/cval-backup --preserve-history-bytes --exclude-dl-unit-test-evaluation --apply --confirm backup --quiesced --confirm-quiesced writers-stopped
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
5. Audit all planned destinations and unchanged included raw DB evidence. Do not
	inspect or fingerprint the excluded directory. Historical receipts/result JSON retain original
	absolute paths; the explicit evidence resolver recognizes relocated paths
	without rewriting those records.
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

### New evidence after backup

Consolidation rejects source changes before its lengthy backup recheck and again
under the migration lock. If the only change is a completely new top-level
directory outside the move plan and the excluded directory, back it up separately
with the existing backup tool and verify that supplemental backup. Both live
backup creation and the revised consolidation command need exact approval.

Pass the verified snapshot using `--supplemental-backup` with `--apply-consolidate`.
The helper requires the combined file/directory set to match the source exactly:
no removed, modified, overlapping, or unbacked entries are permitted. Only the
source root's directory size, timestamps and link count may differ to accommodate
the addition. Its identity, mode, and all descendant evidence still must match.
The supplement's source bytes are checked before moves, and its path and manifest
digest are retained in the migration transaction. The original backup is never
rewritten. Rollback reverses the original moves only, leaving the added directory
and both backups intact. Post-migration audits must account for both manifests.

The helper emits stage and move-count messages to stderr. Retain these with the
exit status in pod-local logs as well as local run logs; a disconnected terminal
or an empty local log is not evidence that a migration finished successfully.
