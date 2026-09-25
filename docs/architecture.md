# Architecture

Implemented source behavior; fresh final-commit acceptance and activation remain
pending under the [development plan](components_design/redesign-development-plan.md).

## Documentation Map

Keep current contracts in the following references; design notes explain
rationale and label deferred proposals rather than repeating operating steps.

| Reference | Responsibility |
| --- | --- |
| This architecture page | Runtime flow and ownership boundaries |
| [Configuration](configuration.md) | Effective settings and configuration sources |
| [Operations runbook](operations-runbook.md) | Planning contract and reviewed operational procedures |
| [Result schema](result-schema.md) | Canonical evidence and raw database contracts |
| [Operator interface](components_design/operator-interface-major-component.md#mode-inventory) | Current modes, retained compatibility and output limits |
| [Major components](components_design/major-components-overview.md) | Responsibility map, baseline measurements and component designs |
| [Redesign contracts](components_design/Redesign.md) | Implemented design decisions, evidence contracts and deferred work |
| [Cleanup review](cleanup-review.md) | Approved cleanup, recorded acceptance and remaining scope |

Code and effective configuration define behavior. Dated acceptance and rollout
records describe the observed revision, not an assertion of present live state.

## Runtime flow

1. Discover matching GPU nodes.
2. Read coherent successful-run coverage from raw status and canonical summaries;
   failed attempts remain evidence but do not refresh coverage. Legacy age-only
   compatibility limits are explicit.
3. Prioritize uncovered and stale nodes, applying submission cooldown, without
   rendering Jobs during candidate inspection.
4. Check prioritized nodes for current GPU, CPU, memory, RDMA, readiness, and
   scheduling eligibility.
5. Render exact-source Volcano Jobs, persist approved intents under shared admission,
   recheck eligibility and submit once. Preserve unknown outcomes; never blindly recreate.
6. Run enabled registry tests through the descriptor-anchored supervisor and
   generic runner.
7. Validate canonical artifacts, write required metrics, then publish raw status.
8. Retain result JSON, logs, events, and test artifacts.
9. Track bounded Job/pod status and compact logs. Run detailed exact-run raw
   verification for separate acceptance/diagnosis, not every polling cycle.

The framework ends at verified raw evidence. No downstream evaluator,
classification, scoring or remediation service is part of this repository.

## Repository Boundaries

The [live loop](../scripts/cval-live.sh) uses its
[session-state helper](../scripts/cval-live-state.py),
[cooldown helper](../scripts/cval-node-cooldown.py), CLI and configured
[pinned Job template](../ymls/specific-node-job.yml). That template launches the
supervisor, which runs the Python test runner and then directly starts
[the Python ingestion coordinator](../cval/storage/run_ingest.py). Both children
use the same interpreter; ingestion takes recorded software versions from the
finalized result and independently authorizes all writes. The
[environment script](../validation-tests/0-env.sh) and
[DB launcher](../validation-tests/db-update.sh) remain compatibility entry points,
not additional stages in the production supervisor path.

Both the pinned and scheduler-selected templates use one packaged
[bootstrap payload](../cval/jobs/bootstrap.sh). The
[renderer](../cval/jobs/renderer.py) embeds it before validating the executable
runtime contract. Each generated Job contains its full startup script; no
additional in-pod file or download is needed to obtain the bootstrap itself.

The [source renderer](../scripts/cval-source.py) produces only the immutable
code ConfigMap used by validation Jobs. Test-owned plugins provide configuration
and read-only raw exports. Backup, layout and SQLite journal tools belong to
evidence recovery, not post-test analysis. See the
[cleanup review](cleanup-review.md) for approval-only retirement candidates.

## Raw databases

- `metadata/validation.db`: built-in outcome rows and `latest_status`;
- `metadata/test-storage.db`: FIO metrics;
- `metadata/test-nccl.db`: consolidated `IB_HEALTH` metrics;
- `metadata/dltest_numerical_correctness.db`;
- `metadata/dltest_compute_performance.db`;
- `metadata/dltest_collective_performance.db`;
- `metadata/dltest_overlap_performance.db`.

`IB_HEALTH` is a retained raw table name. It contains measurements and does not
represent a framework health-class decision.

## Validation runtime

`[tests.<id>]` entries select repository-local `cval.test.v1` descriptors. A
descriptor owns execution metadata, minimum resources, test settings, artifact
summary name, and optional config/raw-export plugin methods.

The supervisor reserves canonical evidence paths through retained file
descriptors. The runner writes `cval.results` transitions and invokes tests in
registry order. The ingestion coordinator loads the immutable configuration,
authorizes the structured result, binds its ingestion digest and calls the
existing raw writers directly. Each writer still revalidates result identity,
configuration, evidence paths and targets at the write boundary.

Passing DL runs serialize raw metric ingestion through
`metadata/.dl-metric-ingest.lock`. Storage and NCCL metrics are written only
for passing phases. The aggregate `all` row is committed after required metric
writes succeed. The order remains storage, NCCL, locked DL, then the atomic
built-in status set. SQLite lock retries repeat a writer operation, not the
whole sequence. DL retries hold the same persistent POSIX lock; failures or
interruption release it without unlinking its file and emit a failure event.

## Safety boundaries

- Discovery and reporting are read-only.
- `plan` never submits.
- Validation creation requires an exact commit and explicit confirmation.
- Timeouts retain Kubernetes jobs and evidence.
- Backup apply requires separate capacity, quiescence, and confirmation gates.
- Existing historical non-raw databases are neither migrated nor deleted by
  normal operation.