# Framework Simplification Review

## September 30 Post-Acceptance Cleanup

Commit `ca79e54c1f9b9cffdb41aebcb3f92be125b583a4` completed exact-run
acceptance on `slc01-cl02-hgx-0149`, timestamp `1790786650`: Completed Job,
exit 0, zero restarts, all three tests passing, final v3 receipt, retained logs
and matching evidence in all seven raw stores. Its live loop remains pinned to
that accepted source; the cleanup below is prepared separately, not credited
with the earlier Job's acceptance.

| Candidate | Disposition |
| --- | --- |
| Old runtime boolean formatter and supervisor built-in import | Removed after finding no callers; test-owned runtime projections remain. |
| Private registry forwarding helper | Inlined its only call to `config.tests.registry`; no environment-based alternate registry remains. |
| Storage scratch data left beside JSON | Put `--directory=fio-data` before FIO filename options so the existing exit trap removes newly generated data. The regression creates scratch files and checks removal before its fixture directory is deleted. Existing PVC data is untouched. |
| Main/per-test settings | Retained the complete main TOML and test-owned descriptors. No workload, image, resource, retry, timeout or raw schema change. Old independent FIO configs are already absent. |
| Environment/DB launchers, DL/NCCL import shims | Retained: repository callers and historical/public compatibility obligations remain. |
| Backup, layout, journal, lock, source and cooldown tools | Retained: production, recovery and exact-source delivery still use these tools. |

No new framework feature, setting or source file is introduced. No historical
reader, database, failed run, log, session, ConfigMap or backup is removed.
Local shell/runner/safety checks do not substitute for deployment acceptance of
the new cleanup commit; its source installation and live activation need a new
exact-command approval.

## C2-C9 Source Cleanup

The 2026-09-26 follow-up starts at published C1 `f9837ab`. Source cleanup is
separate from exact-commit cluster acceptance and activation. It does not modify
the running checkout, canonical PVC evidence, historical snapshots or raw schemas.

| Component | Implemented / Retained |
| --- | --- |
| C2 | Retired the four `db-add-*` CLI commands and CLI-only writer wrappers. Automatic Python ingestion and the separately authorized DL rebuild command remain. |
| C3 | Retired GPU-only/text-table helper APIs; structured node/pod inventory and shared GPU/CPU/memory/RDMA eligibility remain. |
| C4 | Removed unused receipt/cooldown recovery and legacy log parsers. Plans now consume the original validated coverage JSON, without a duplicate map file. Shared batch observation, durable intents, cooldowns and pruning gates remain. |
| C5 | Retired the standalone path-only preflight and unused env-line formatter. Actual ID validation stays with descriptor-bound reservation. The supervisor/runner process boundaries, legacy result readers and reserved result/environment files remain. |
| C6 | Retired the three per-test shell aliases; descriptors still select their unchanged `run-test.sh`. Top-level runner/environment/ingestion compatibility launchers remain. |
| C7 | Node fallback, counts and snapshot hash are calculated once per projection. Both CSVs, their fields, atomic replacement, 300-second refresh, seven raw stores and exact-run verification remain. |
| C8 | Replaced the textual bootstrap scanner with semantic YAML checks and equality with the packaged startup program. Canonical inline startup is accepted; customized inline programs are not. Git/ConfigMap transports and both placement templates remain. |
| C9 | Reviewed and retained backup, layout and journal tools. Layout recovery dynamically loads the backup implementation; unknown rollback obligations are not evidence that these tools are disposable. |

Migration: use `get_job_phases([name])` instead of `get_job_phase`,
`discover_gpu_node_names` instead of the capacity-table parser, and
`describe_nodes_from_outputs` for scheduling/resource decisions. Legacy log strings
are not evidence APIs; consume the shared run-bound observer or canonical result.
Use the canonical per-test runners rather than the three retired aliases. The
path-only preflight command is gone; startup reservation still rejects unsafe paths.

This batch does not rewrite the live loop into another controller, remove the
independent status refresher, retire Git startup/pruning, or delete maintenance
recovery tools. Those larger choices remain separate from demonstrated redundancy.
The earlier reviews below are dated records, not current interface inventories.

## Phase 6 Closeout Review

Reviewed 2026-09-25 against published `62dcdeca982950dac9eb829765eba64f42ab1bf1`.
This bounded review found no additional runtime deletion justified. Public
interfaces remain supported under the updated
[caller inventory](components_design/operator-interface-major-component.md#phase-0-compatibility-inventory).
Source implementation and fresh runtime acceptance remain separate; the dated
records below do not describe current deployment.

| Candidate | Evidence and Disposition |
| --- | --- |
| Old observer, phase lister, receipt search and event-parser wrapper | Already absent: `observe_submission`, `list_job_phases`, `latest_cycle_dir_with_submits`, `_structured_progress_events`. Shared observation remains the owner; no second deletion credited. |
| Old standalone reservation helpers | `_run_paths`, `_preflight_registry_paths` and `_create_run_marker` are gone. Retain current reservation validation and descriptor projections; they do not recreate directories. |
| Shell aliases and ingestion launcher | Retain compatibility entry points and their existing tests. Production supervisor calls Python ingestion directly; unknown external usage is not proof of disuse. |
| Lock/cooldown scripts and path-preflight entry point | Lock and cooldown are dynamically loaded by production owners. Path preflight has a standalone CLI and safety tests; external usage is unknown. Retain them and the renderer's rejection of unsafe legacy bootstrap markers. |
| DL writer bodies | Both call `_write_metric_rows`; explicit schemas and independent authorization remain intentional, not another duplicate transaction implementation. |

No modules, settings, raw schemas, test environments or runtime entry points were
added, removed or moved. Documentation reconciliation removes stale proposals and
links to the existing owners. The measured source/doc delta and final acceptance
handoff belong to the [development plan](components_design/redesign-development-plan.md).

Retained limits: fresh ingestion/resume/single-attempt acceptance is pending;
local CSV writers can leave a partial destination on an interrupted write. Atomic
CSV publication, public retirement, speculative adapter caching and new dependency
provenance are deferred, not silently implemented. No cluster or PVC mutation was
performed. Workspace customization is outside this phase and repository; stale
skill wording about the compatibility ingestion launcher is not runtime authority.

## Phase 1–3 Retrospective

Reviewed 2026-09-24 at `ebe5cae33b495984aa013fe2f3041d805bbb7286` against
Phase 0 `b79a5e0`. **Not every addition was minimal.** Keep the core workflow:
discover/select, submit once, observe Job/pod/logs, run configured tests, ingest raw
results, and inspect exact evidence. Do not add another controller, report service,
database, environment-pinning policy or generic runtime framework.

### Findings and Decisions

1. **Acceptance, not more features, is the main gap.** Phases 1–3 are published
	source, not fresh cluster-accepted deployment. Local process/filesystem checks
	and historical reads cannot replace an approved exact-commit workload. Keep the
	loop paused and preserve the separate source/session/submission approval gates.
2. **Phase 1 safeguards are necessary but expensive.** Keep explicit TOML snapshots,
	fresh eligibility, one admission owner, durable no-recreate intents, successful
	coverage and exact-timestamp exports. Keep the detailed raw verifier for targeted
	acceptance/diagnosis, outside routine Job polling. No cross-host failover, generic
	workflow engine, broader compatibility database or classifier is justified.
3. **Phase 2 left dead implementations behind.** Removed the uncalled single-Job
	receipt observer, unused namespace phase lister, stale receipt-file recovery
	search and pass-through event parser. Shared batch observation, bounded logs,
	identity checks, cooldown and failure/unknown distinctions remain unchanged.
4. **Phase 3 retained a redundant ingestion handoff.** The supervisor now starts
	`cval.storage.run_ingest` directly with the same interpreter. It no longer starts
	an environment shell that can import PyTorch twice after the result is finalized.
	Ingestion still authorizes the result/snapshot and each writer revalidates it;
	image/CUDA/PyTorch values come from the result. Shell compatibility entry points
	remain available. Shared reservation, child cleanup and run-local DL inputs stay;
	extra timing instrumentation and custom images remain out of scope.
5. **Documentation grew faster than runtime code.** The phase series added 1,773
	net documentation lines, 1,163 runtime/config lines and 246 test lines in the
	reviewed paths. The roadmap repeated component-level implementation history and
	the job-control guide retained a superseded recovery rule. Trimmed the repetition,
	preserved all task IDs/linked anchors and corrected the terminal-release rule.
	Git and retained run records preserve the detailed history; current behavior
	should have one owning reference, not a new document for each iteration.

### Boundaries and Verification

The cleanup removes 76 net runtime lines without adding a module, setting, CLI
mode or raw schema. A small regression test covers direct-entry authorization
rejection. Four real-process lifecycle/authorization tests passed; current imports,
CLI construction, shell syntax, retained real-event parsing and identity/release
predicates passed. The customization validator passed; no customizations changed.
The session-store query had no indexed match, so the retrospective uses the current
conversation, Git history, source and retained evidence, not an archive reconstruction.

Do not equate this with all legacy tests passing: older synthetic/default-based
suites are not a demonstrated current regression gate. Fresh valid-result ingestion,
controller resume and installed Volcano single-attempt behavior remain unverified.
No cluster resource, live session, raw DB, existing log or source artifact was changed.
Further implementation should remove a demonstrated duplicate or fix a concrete
defect; retain separate approval for any live validation or activation.

## Earlier Review

Review date: 2026-09-22. Baseline:
`7464e4fc55ec88691d015afdb3ec08ffa246f6c1`.

**Scope revision, 2026-09-23:** the framework ends at successful tests, retained
PVC logs/results, and verified raw DB ingestion. Downstream evaluation code and
deployment functionality are removed; see finding 6. Historical operating records
below describe their original revisions, not the scope of the new release.

**Published and accepted on 2026-09-23:** batches A-B, planning finding 1,
ingestion finding 2, and the CLI settings cleanup shipped in
`1abde89ff3d615d0d064b33131542315dc4169c6` on internal `main`. One-node
acceptance passed before the live loop was restarted at that commit.

The 2026-09-23 settings cleanup also removes single-choice format switches,
legacy TSV, optional validation ZIP export and duplicate public CLI overrides
for config-owned targets, job settings and policy limits. The
[remaining control inventory](components_design/operator-interface-major-component.md#mode-inventory)
records replacements. Validation settings, raw writers and canonical evidence
are retained.

**Implemented follow-up:** retire batch C's all-files publish wrapper and
consolidate documentation (finding 5), followed by shared bootstrap extraction
(finding 3). The bootstrap refactor still requires its own exact-commit cluster
acceptance before deployment. Finding 4 and compatibility-alias retirement remain deferred. Absence
from the continuous loop alone is not proof of non-use.

## Maintainability Re-audit

The A-B removals reduce stale material, not active framework complexity. The
next goal was fewer places to understand or change one behavior, not merely fewer
files. At this earlier review, findings 1-2 were accepted, finding 3 was implemented,
and finding 4 was proposed; Phase 4 subsequently implemented it as recorded below.
The earlier retain inventory protected responsibilities and evidence; it did
not mean every current implementation or abstraction should remain unchanged.

### 1. Separate Candidate Selection From Job Rendering

**Published and accepted in `1abde89`.** Previously the loop rendered
every due candidate before selecting an available node, then rendered its Job
again at submission. Preflight repeated that rendering as well.

Plain `plan` now returns ordered candidates with an exact commit and counts,
using the existing pure priority function. Both
[live-loop](../scripts/cval-live.sh) inspection paths use this single default;
only confirmed submission invokes the [Job planner](../cval/orchestrator/workflow.py).
The unshipped `--candidates-only` flag and old `--include-yaml` mode are removed,
along with rendering-only plan arguments and the unused rendered-plan serializer.
No internal render toggle, new command, scheduler or configuration was added.

Local regressions verify zero renders during candidate inspection, identical
priority order, one render for selected-node submission, and failure before
creation for invalid templates. Existing preflight, fresh eligibility, cooldown,
resume, pruning and worktree/source guards remain. Malformed candidate JSON and
a returned commit differing from the requested commit fail closed. See the
[planning contract](operations-runbook.md#candidate-inspection) for the default
candidate JSON shape and the [CLI inventory](components_design/operator-interface-major-component.md#mode-inventory)
for retained controls. Callers must migrate from `planned_jobs`
to `candidates`; this is an intentional planning-interface change.

### 2. Give Ingestion One Orchestration Path

**Published and accepted in `1abde89`.** The former 537-line
[db-update.sh](../validation-tests/db-update.sh) is now a six-line launcher for
[one Python coordinator](../cval/storage/run_ingest.py). JSON-to-shell projection,
argument reconstruction and separate writer CLI processes are gone. The existing
raw writers and schemas are unchanged; no generic DB or new plugin layer was added.

The coordinator authorizes the result/config snapshot, binds the immutable
digest, and preserves storage -> NCCL -> locked DL -> atomic status ordering.
The provenance owner now owns digest binding; each low-level writer still
revalidates its authorization. The existing POSIX lock helper exposes its secure
acquisition checks for direct use, while its command wrapper retains child
cleanup behavior. Events and SQLite retries use the existing implementations.

Local tests verify seven actual temporary stores, unchanged receipts and rows
after a lock retry, suppressed status on missing evidence, failure/interruption
events, released locks and descriptor-bound supervisor ingestion. Historical
structured v1/v2 readers remain; environment-only results cannot authorize writes.
Individual CLI writer/rebuild hooks remain compatibility/maintenance interfaces,
not part of normal orchestration. Exact-commit cluster acceptance also verified
canonical evidence and matching rows in all seven raw databases; see below.

### 3. Maintain One Job Bootstrap Program

**Implemented; exact-commit cluster acceptance pending.** Both Job
templates now contain one `runtime-bootstrap-placeholder` in the server's Bash
args block. The [renderer](../cval/jobs/renderer.py) embeds the packaged
[shared shell payload](../cval/jobs/bootstrap.sh) before its existing semantic,
duplicate-key and executable-order validation. No template engine, CLI mode,
configuration switch or extra pod dependency was added.

The rendered Job remains self-contained: it does not need to fetch source before
obtaining its bootstrap. Node-specific placement and scheduler-selected placement
remain separate and unchanged, as do resource and environment fields. Valid
existing inline bootstrap templates still pass the same safety checks; malformed,
mixed or duplicate shared markers are rejected.

The 38 focused renderer/workflow/job-manager tests pass, including both source
transports, archive commit/hash rejection, shell syntax and hostile-template
regressions. A structured comparison with `1abde89` verified unchanged placement,
resources and environment, and identical executable bootstrap lines. This local
verification does not replace exact-commit cluster acceptance for the refactor.

### 4. Share DL Transaction Bookkeeping

**Phase 4 source implemented on 2026-09-25; real acceptance deferred.**
The two DL writers now share one private transaction helper, retaining explicit
schemas/columns, APIs and generation finalization. Source expansion is identical
to the original writer bodies. This removes a duplicate ownership path without
claiming fewer physical lines or adding a generic persistence framework.
See [verification and remaining gates](components_design/raw-evidence-major-component.md#phase-4-writer-consolidation).

### 5. Give Current Behavior One Documentation Home

**Completed.** The
[documentation map](architecture.md#documentation-map) assigns current runtime,
configuration, operations and evidence contracts to existing references.
No additional design document was created.

The later raw-only scope revision removes the evaluator's design/schema notes
and deployment procedures rather than retaining an unsupported downstream design.

The runbook owns the candidate JSON contract. The CLI note retains the mode
inventory and migration guidance, but replaces its stale local-only roadmap and
speculative line-count targets with explicitly unapproved interface decisions.
The published `1abde89` acceptance is recorded separately from these later
follow-ups. Dated rollout evidence is not a current-state
claim. Local link/reference checks and the customization validator cover this
documentation-only change; retained operational records are not rewritten.

### 6. Remove Downstream Evaluation

**Implemented; separate release acceptance required.** Removed the evaluator
package, deployment manifests/tools, per-test baseline/classification modules,
evaluation configuration and evaluator-only tests/docs. The raw registry no
longer accepts an evaluation policy in test descriptors.

The shared source-bundling capability is retained as
[scripts/cval-source.py](../scripts/cval-source.py), which produces one immutable
validation-source ConfigMap from an exact commit. It creates no evaluator Pod or
dependency ConfigMap. PVC-access coverage remains in the renderer tests.

Discovery, scheduling, supervisor execution, canonical results/logs, raw exports,
seven-DB ingestion, retries/locking and Job-success verification remain. No raw
schemas, historical result formats or PVC paths are migrated or deleted. Any
already deployed downstream workload/data is left untouched; decommissioning it
requires separate exact commands and approval.

## Simple Ownership Model

Keep the existing responsibilities; simplify the hand-offs, not the safety model:

| Owner | Responsibility |
| --- | --- |
| Live controller and existing scheduler/job APIs | Exact source, candidate priority, fresh eligibility, bounded slots, submission/resume state |
| Supervisor and registered test runner | Descriptor-owned paths, test setup/execution, deadlines, canonical results and events |
| Test-owned raw writers | Validated metrics first, final status last, unchanged raw schemas and history |
| Evidence inspection and maintenance | Read-only raw audit/export, backup and recovery outside the per-cycle path |

No new orchestration framework, generic persistence layer, or wholesale directory
reorganization is proposed. A refactor should remove a duplicate implementation
or conversion path, not just rename, relocate or compress it.

## File Cleanup Status

| Batch | Status | Baseline reduction |
| --- | --- | --- |
| A: retired evaluation prototypes | Published in `1abde89` | 3 files, 554 text lines, 23,924 bytes |
| B: historical planning bundle | Published in `1abde89` | 7 files, 427 text lines plus PNG, 147,067 bytes |
| C: all-files publish wrapper | Approved and removed in this follow-up | 1 file, 61 lines, 1,422 bytes |
| D: shell compatibility aliases | Three per-test aliases retired in C6 above; top-level runner retained | Historical candidate: 4 files, 26 lines total |

A-B total: 10 files, 981 text lines plus one PNG, 170,991 bytes. Gross source
removals exclude documentation edits. Exact removed paths remain recoverable at
the baseline commit; no duplicate archive was created:

```text
docs/evals/README.md
docs/evals/classify.py
docs/evals/threshold.py
docs/project_managemnt_docs/README.md
docs/project_managemnt_docs/cval-components.md
docs/project_managemnt_docs/cval-versions.md
docs/project_managemnt_docs/cval2.0-audited.md
docs/project_managemnt_docs/cval2.0-initial-scope.md
docs/project_managemnt_docs/cval3.0.md
docs/project_managemnt_docs/image.png
```

C retired the all-files publish wrapper: it staged everything and pushed the
current branch to origin, not an isolated reviewed change to an explicitly
chosen target. The workspace shipping skill now requires explicit reviewed
paths, staged-diff inspection and destination verification. No replacement
wrapper was added. C is safety cleanup, not the main runtime simplification.

The [top-level runner](../validation-tests/run-test.sh) remains. The retired
per-test aliases were `storage/storage.sh`, `nccl/run-nccl-allreduce.sh`, and
`dltest/dltest.sh` under the workload directory. Their canonical per-test runners
and descriptor selection are covered by the C6 checks above.

## Keep These Guarantees

- Retain exact-commit/mutation gates, cooldown/resume records, bounded commands,
	descriptor/path/symlink protections, immutable result/config checks, locking,
	status-last publication, and historical result/path readers. Reuse their code
	where justified; do not remove checks solely to lower line counts.
- Keep registered tests/plugins, their regression suites, and backup/layout/journal
	recovery. Their independent entry points or
	dynamic loading do not make them dead files. Splitting services/packages is
	a separate product decision, not necessary to obtain the reductions above.
- Protect raw DBs, PVC contents, backups, run logs, active source/runner worktrees,
	governance and user-owned untracked files. Conversation archives and the
	protected `dl_unit_test_evaluation` directory were not read.

## Verification and Remaining Scope

The runtime changes were accepted on `slc01-cl02-hgx-0250`, run timestamp
`1790132796`, using the published exact commit. The Job completed with exit 0
and no container restarts. Storage, NCCL and DL passed; ingestion finished with
`pass` in 37.395 seconds. The audit verified 38 canonical evidence files,
result/config digests, four passing status rows, one storage row, one NCCL row,
and 7,128 DL metrics across eight ranks with complete generations and receipts.

The live loop restarted at `2026-09-23T03:35:34Z`. Its first cycle verified
candidate-only plans and checked 282 due candidates, all ineligible. The
`03:41:06Z` observation showed a running, idle loop; two fresh automated runs
were still unverified for lack of eligible capacity. This is dated rollout
evidence, not a claim about the current live state. The detailed acceptance
and restart record is retained locally under the release run logs.

The documentation/wrapper follow-up uses link/reference checks and the workspace
customization validator. The later approved bootstrap extraction has focused
renderer/caller regressions; no live mutation was performed for either change.
Publication alone does not deploy the shared bootstrap. Its exact published
commit still needs separately approved cluster acceptance before rollout.
DL transaction consolidation was subsequently source-implemented in Phase 4;
the later alias retirement is recorded above. Fresh runtime acceptance is still required.
Source cleanup never authorizes live deletion or evidence removal.