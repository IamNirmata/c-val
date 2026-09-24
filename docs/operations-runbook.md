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

### Node eligibility

`nodes` and `nodes --check-node <node_name>` use the same structured node/pod
calculation. Both require explicit Ready, positive GPU capacity, no active GPU
requests, sufficient configured GPU/CPU/memory/RDMA, and compatible scheduling
taints. Automatic checks reject cordons. Only explicit targeted `validate` may
permit a Ready drained cordon; it never bypasses other blocking taints or capacity
checks. NoExecute taints block because the templates tolerate NoSchedule only.

Bulk discovery reads pods and node JSON once each, without a separate capacity
table. Requests include regular containers, ordinary and restartable init
containers, and pod overhead. Completed pods do not reserve resources. Unsupported
pod-level or resize accounting and malformed per-node evidence are unverified:
bulk output omits that node and emits a diagnostic on stderr; a targeted check
fails explicitly. Missing/truncated inventories fail the whole read, not an empty
candidate list. Retain stderr as well as JSON; totals cover only reported nodes.
Missing or unknown Ready is ineligible. These reads are not an atomic reservation
and cannot authorize submission.

## Candidate inspection

Use `cval status --coverage --output json` for scheduling history. The response
keeps requested tests, provenance limitations, skipped incomplete/conflicting runs
and per-node coverage timestamps. `plan --live-status` reads the same history;
`--db-status-json` can consume a retained full coverage report. Plain node/timestamp
maps remain explicit offline inputs. An unreadable history stops planning rather
than labelling every node never-tested. `no-successful-coverage` means history was
read but no complete passing requested run qualified; it differs from `never-tested`.

The live loop collects this coverage report before each planning pass. The named
age policy is `successful_complete_run`: coherent historical passing status can still
count with explicit limitations; full dependency compatibility waits for its
producer. No source-only commit invalidates all prior workload coverage.
Failed/incomplete attempts remain visible but do not refresh successful coverage;
an older passing timestamp remains usable with normal age and cooldown checks.
Old `legacy_complete_run` reports are rejected rather than silently reinterpreted;
regenerate them with `status --coverage`. Raw failure rows are never suppressed.

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
inspection mode. See the [CLI migration inventory](components_design/operator-interface-major-component.md#removed-settings-and-migration)
for retired flags and their replacements.

## Continuous validation loop

On resume, the existing state helper reconciles durable intents before the loop
inspects any receipt files. Uncertain create outcomes are observed, never retried.
Derived jobs CSV can be rebuilt from intact v2 state. The shared manager enforces
cooldown for every submitter from confirmed creation evidence as well as the
retained cooldown table. Do not bypass an unresolved intent by editing projections.

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

### Shared Submission Admission

Every supported `run` or `validate` submission requires a compatible active session
under the configured absolute `live.log_dir`, even when explicit node names are
supplied. Session creation alone does not start the loop. Use the existing state
helper's `init --approval-record <reviewed-path>` only under a separately approved
initialization or turnover command; it refuses an existing `current-session`
pointer. The live launcher requires `CVAL_APPROVAL_RECORD` for a new session.
Preserve and reconcile the prior
session and outstanding Jobs before approving replacement. See
[shared admission settings](configuration.md#shared-admission-scope).

Under `.cval-admission.lock`, the manager verifies host/cluster, session/source/config
identity, the rendered request, existing node reservations and the shared inflight
budget. It then rechecks policy and fresh node eligibility immediately before each
create. An unavailable or incomplete Job read is not an empty queue. Unresolved
placement, missing local Job records or same-run reuse block admission. `Missing`
and `Pruned` local records do not silently release a reservation.

The lock is released before workload monitoring. It is separate from the loop-owner
lock and is also used by session updates. Contention fails the command; investigate
the holder, never delete the lock. No new queue, service or automatic host failover
is introduced. This only serializes cooperating new-source writers; stop/reconcile
old writers before activation. Live turnover/activation is deferred until Phase 1
is finished.

### Durable Submission Outcomes

V2 session state retains approved request identities and ordered intent/outcome
events before and after each create. `jobs.csv` remains an eight-column projection;
do not edit it to release uncertain work. `run --output json` preserves earlier
batch receipts when a later item fails and exits nonzero. Keep stdout, stderr and
the exit code. The shell already stops that cycle without a replacement submission.
Targeted `validate` returns a failed submission report on the same error path;
neither CLI discards a received create response just because state persistence failed.

Before further admission, the manager observes retained attempted intents by exact
name and verifies requested workload fields and any known Job UID. The existing
state helper's `job` operation also verifies a live observation instead of trusting
the supplied phase; it writes operational state and needs the normal approval.
No create is retried by reconciliation. Absent/unavailable observations keep the
node/slot reserved, and identity conflicts block admission. Preserve prepared-only
crash records for explicit reconciliation rather than assuming nothing happened.

A successful state write followed by a failed CSV write is still retained evidence;
the next append loads that on-disk state before rebuilding the projection. Missing
or corrupt authoritative state never becomes an empty session. Attempted
intents retain reservations until identity-bound terminal tracking permits release.
Tracking release is not evidence acceptance. Actual lost replies, concurrent live submission
and partial live batches remain unverified; this is not exactly-once delivery.

### Exact Evidence and Release

Ordinary active/resumed/targeted tracking shares one bounded Job/pod/log observer.
It verifies the submitted Job and owned pod/container, retains test/ingestion
progress and separates terminal execution from confirmed runtime completion.
A verified terminal Job with no live owned pod can release its execution slot even
when logs or ingestion remain unconfirmed. Missing Jobs, unavailable status/inventory
or conflicting identity cannot release it or authorize another create. A log read
failure is retained separately from Kubernetes execution state. Same-run reuse and
cooldown checks still apply. No routine raw-store scan is required.

Runtime completion additionally requires terminal success, zero container exit and
restarts, selected passing tests, retained logs/finalized result and matching
digest-bound ingestion/supervisor acknowledgements. Old/generic completion strings
are insufficient. See [shared tracking](components_design/job-control-major-component.md#phase-23-implementation).

Phase 2.4 batches CLI phase reads and pending-intent Job/pod inventories under
one remaining deadline. Phase 2.5 routes active/resumed shell monitoring through
one existing `update --reconcile` state-helper call and one report conversion.
Its optional `--output json --jobs <names>` report follows durable observations;
`--jobs` selects output only, never bypasses other unresolved session intents.
The shell consumes `tracking_complete`, not phase alone, to free slots. Failed
terminal work is retained and never restarted/recreated. The scalar `job` hook
remains for compatibility and separately authorized pruning, not ordinary polling.
Timestamped logs begin with the configured tail and then reuse an identity-bound
`log_cursor` with inclusive `--since-time` reads. Existing line/byte settings bound
reads; no config change is needed. Preserve the compact cursor/runtime fields in
intent history. `log_state=limited` means truncated output, not successful evidence
capture; unavailable reads do not advance the cursor. The boundary second is
replayed, not skipped. This reader is not a replacement for canonical retained logs.
See [batching and limits](components_design/job-control-major-component.md#phase-24-implementation).

Targeted validation retains a once-only read-only acceptance verifier after polling.
It uses the retained request and submitted snapshot, not the observed result, for
expected identity. It checks the owned terminal Job/pod, canonical files and digests,
event order, exact status rows and outcome-required metric values/receipts. Historical
or newer latest-status rows never substitute for the requested timestamp.

Reports include `verification.state`, `complete`, `accepted`, `release`, per-source
states and artifact hashes. Successful execution alone is insufficient. Complete
evidence of a failed test is still a failure. A bounded incomplete/conflicting
terminal evidence report may release legacy verification-based tracking without
acceptance. Inaccessible sources or changing generations block audit acceptance,
but do not undo a separately verified terminal tracking decision. Job/pod identity
conflicts remain blocking, never an accepted run.
No verification step deletes Jobs, edits raw rows or retries the workload.

The reader executes in memory in the configured PVC-access pod, opens SQLite with
`mode=ro` and `query_only=ON`, and creates no remote files. Both artifact limits are
explicit TOML settings. DL generation changes/in-progress publication use the
configured retry budget under the same deadline. Inspect unavailable reasons rather
than raising limits or changing identities without reviewing the request.

The new-source acceptance and live state-release paths are not yet exercised on a
fresh workload. Historical read verification passed; live-loop activation remains
deferred until all of Phase 1 is finished and exact rollout commands are approved.

## Exact-commit validation

### Exact-run exports

```bash
cval results --test overall --node slc01-cl02-hgx-0192 --timestamp 1790178532 --output-dir ./exports
cval results --test nccl --node slc01-cl02-hgx-0192 --timestamp 1790178532 --output-dir ./exports
```

These are read-only historical examples, not validation submissions. Both selectors
are required together. No matching exact status exits nonzero and writes no CSV.
The default without selectors still exports latest status. Existing CSV columns
are unchanged, including the legacy `latest_timestamp` name, which denotes the
selected status timestamp. Metric columns join only that exact node/timestamp.
Absent metrics are blank; inaccessible metrics fail the export, never silently
appear as blanks. DL's existing export remains status-only.

### Validation workflow

Newly rendered validation Jobs are single-attempt: the configured pod policy is
`Never` and failure/eviction/task-failure/Job-unknown events use `TerminateJob`.
The templates explicitly set Job/task retry limits to one; do not replace them
with zero, which upstream admission defaults to three. No task-level override or
automatic finished-Job TTL is permitted. See
[single-attempt settings](configuration.md#single-attempt-validation).

Expect terminal failures to be reported as Failed or Terminated, not success.
The policy preserves already-terminal pods under the inspected upstream semantics;
it may terminate live pods belonging to that Job, and cannot recover externally
deleted/evicted logs. Do not restart or recreate a failed run. Preserve what remains
and leave later testing to a new reviewed scheduler decision. This source change
does not modify existing Jobs or the active controller.

Before rollout, verify the installed Volcano version/schema and read back the
admitted Job's effective policies. The current agent was forbidden from reading
the controller Deployment and CRD, so their compatibility remains unverified.
Any server-side admission exercise, submission or deliberate failure/eviction
requires its own exact approval. A local render is not a single-attempt cluster test.

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
from the published commit by [scripts/cval-source.py](../scripts/cval-source.py):

```bash
python3 scripts/cval-source.py --revision "${CVAL_GIT_REF:?Set an exact published commit}" --namespace gcr-admin --output-dir "${CVAL_SOURCE_DIR:?Set a new local output directory}"
```

The tool writes only `source-configmap.yaml`, named `cval-source-<commit-prefix>`,
and reports its archive hash. It needs no dependency wheel or separate service
manifest. This is local preparation, not a cluster mutation. In the
operator's effective `[job]` configuration, set `source_configmap` to that
ConfigMap name and `source_sha256` to the SHA-256 of its `source.tar.gz` bytes.
Both fields must be supplied together with the exact commit. The runtime checks
the archive hash and Git archive commit header before extracting into a fresh
checkout directory. It then runs the same descriptor-anchored supervisor.
Publishing the ConfigMap and submitting the Job remain separate gated operations.
Do not put personal Git credentials in a ConfigMap, Pod environment, or manifest.

### Real-node development procedure

Phase 0 procedure; the templates below are not approval to execute a mutation.
Use existing commands only, from the exact published checkout. Retain one local
record containing source SHA, reviewed config/snapshot digest, node, unused run
timestamp, selected tests/settings, source/manifest identity, approval and rollback.
Expected successful category: `Fully recorded`, required tests `pass`.

| Step | Required Action / Evidence | Gate |
| --- | --- | --- |
| 1. Prepare | Verify published source, effective config, dependency identities and immutable source object if used. | Source creation needs its own exact approval; preparation is not activation. |
| 2. Observe | Inspect existing controller/Jobs and the selected node. Require Ready, all GPUs free, CPU/memory/RDMA sufficiency and known ownership. | A drained cordon may be explicitly targeted; unknown/busy state blocks submission. |
| 3. Submit | Approve the literal command, node, timestamp, inputs and rollback; recheck volatile conditions immediately before create. | One approved real Job, no fake workload or silent retry. |
| 4. Observe completion | Retain Job/pod UID and assigned node, phase, container exit, CLI output/error/exit status, logs and events. | A timeout or lost reply means reconcile the same identity, not submit again. |
| 5. Verify evidence | Canonical run/test identity and digests, selected-test outcomes, terminal ingestion and exact-run raw rows/receipts. | Successful terminal Job plus all required evidence; never latest-row substitution. |
| 6. Record outcome | Use the result categories, keeping test outcome and DB state separate; retain any incomplete/unavailable evidence. | Activation, cancellation and another run each retain separate approval gates. |

Read-only targeted check, using `cluster.kubectl_timeout_seconds` from the reviewed TOML:

```bash
python3 -m cval.cli \
	--config "${CVAL_CONFIG:?Set reviewed config}" nodes \
	--check-node "${CVAL_NODE:?Set reviewed node}" --output json
```

Only after exact submission approval:

```bash
python3 -m cval.cli --config "${CVAL_CONFIG:?Set reviewed config}" validate \
	--node "${CVAL_NODE:?Set reviewed node}" \
	--git-ref "${CVAL_GIT_REF:?Set exact published commit}" \
	--timestamp "${CVAL_RUN_TIMESTAMP:?Set unused approved timestamp}" \
	--submit --confirm submit --output json
```

`validate` already watches the Job. If observation is interrupted, use the existing
`jobs --jobs <recorded-name> --watch` command; do not invoke `validate` to inspect it.
Keep the successful terminal phase requirement even when the watch command exits zero.

In this workspace, reuse the existing read-only raw collector from the workspace
root (it is workspace tooling, not part of the nested c-val package):

```bash
python3 .github/skills/cval-node-health-audit/scripts/node-evidence.py \
	"${CVAL_NODE:?Set exact assigned node}" --timestamp "${CVAL_RUN_TIMESTAMP:?}" \
	--namespace "${CVAL_NAMESPACE:?Match reviewed config}" \
	--pvc-pod "${CVAL_PVC_POD:?Match reviewed reader}" \
	--output "${CVAL_AUDIT_OUTPUT:?Set new JSON path with existing parent}"
```

This collector checks exact raw rows with read-only/query-only SQLite; it does not
prove canonical files, result/config digests, event order or Job success. Complete
those checks using retained PVC artifacts and Job observations. For the full built-in
profile verify four status rows, storage/NCCL metrics, four DL stores, expected rank
coverage/receipts and stable completed generations. Expectations come from the
reviewed profile, not another run. Result-only or disabled tests have different
requirements as defined in the [result contract](components_design/Redesign.md#run-result-categories).

If the collector or evidence is unavailable, keep acceptance pending and retain the
reason. Do not replace it with a new audit script, fabricated rows or latest CSVs.
Do not damage raw data or interrupt unrelated workloads to manufacture a failure case.

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
timeout --foreground 150s python3 -m cval.cli --config "$CVAL_CONFIG" nodes --inventory-only --output json > "$CVAL_QUICK_DIR/inventory.json" 2> "$CVAL_QUICK_DIR/inventory.stderr"
timeout --foreground 390s python3 -m cval.cli --config "$CVAL_CONFIG" nodes --output json > "$CVAL_QUICK_DIR/discovery.json" 2> "$CVAL_QUICK_DIR/discovery.stderr"
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
	code or `terminal=true` alone is insufficient: Failed and Aborted are
	terminal failures; Missing retains uncertain tracking. Pending, timeout and unknown state are not passes.
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

Use the [recovery component](components_design/recovery-maintenance-major-component.md#retained-paths) for the retained-path map,
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
	including the live loop, validation jobs, interactive clients and any independently
	deployed legacy consumers. Removing repository code does not stop those processes.
	A blocked loop or a Kubernetes Ready condition is not proof of quiescence.
2. Stop writers under separate approval. Retain the exact manifests required for
	recovery, removing scheduler-assigned `nodeName` from any approved recreation.
	Do not force-delete an active writer or start a second one.
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
	Existing downstream processes are outside this repository's scope and must not
	be recreated automatically by this procedure.

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
