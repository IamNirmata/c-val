# Redesign Development Plan

Status: Phase 0 contracts complete; Phases 1.1-1.6 and 2.1-2.5 source implemented.
Fresh exact-commit execution/ingestion acceptance and activation remain pending;
Phase 3.1-3.4 cleanup source is implemented; Phase 3.5 evidence review is complete,
but fresh accepted-run comparisons remain pending.
Phase 4 source consolidation and preservation review are complete. Per the
2026-09-25 decision, real-node testing waits until all phases are implemented;
source completion is not cluster acceptance or activation approval.
Phase 5 packaging and recovery review are source-complete; in-pod acceptance
remains deferred until the remaining phases are implemented.
The live loop remains paused since 2026-09-24 UTC.
Phase 0 source baseline: `b79a5e0` (2026-09-23). The implementation and acceptance
states above are separate; publication does not authorize live mutations.
Shared contracts live in [Redesign.md](Redesign.md).
Component IDs refer to the [major-component overview](major-components-overview.md).

## In Simple Terms

First agree who owns each job and how to describe its results. Then make one small
change, validate the exact published commit with real tests on real nodes, and
remove replaced code after verification. Keep the framework and its tooling small.

## Working Rules

- Use phase numbers as the preferred rollout order; the map lists actual
  prerequisites. Independent design work may start earlier, not bypass gates.
- Preserve public commands, canonical results and raw stores unless a migration is
  reviewed. No one-release rewrite, evaluator, generic per-test DB or new service.
- Retain raw history, test-owned persistence and explicit mutation approvals.
- Check off reviewed, published tasks with the required real-run evidence and concise docs.
- Track **published**, **cluster-accepted** and **active** separately. Task completion
  never authorizes creation, restart, deletion or migration.
- Use real workloads on eligible real nodes through existing commands. Do not
  build fake-node/job fixtures, simulation harnesses or new test-wrapper scripts.
- Local syntax/link checks can validate documents, not runtime behavior. Existing
  tests are not automatically deleted; a new synthetic suite is not a phase gate.
- Keep one owner per decision; add helpers only when they remove duplicate logic.

## Phase Map

| Phase | What It Delivers | Components | Depends On | Main Risk |
| --- | --- | --- | --- | --- |
| 0. Agree Contracts | Clear boundaries, result categories and real-node check plan | All | Existing design | Unclear compatibility requirements |
| 1. Fix Correctness | Consistent eligibility, recoverable submission, exact-run evidence | C1-C4, C7 | Phase 0 | Unsafe jobs or misleading acceptance |
| 2. Consolidate Control | One lifecycle implementation for manual and continuous runs | C2-C4 | Phase 1 | Lost resume/cooldown state |
| 3. Simplify Runtime | One secure evidence setup and reproducible workloads | C1, C5-C6 | Phase 1; stable run-definition contract | Evidence integrity or changed measurements |
| 4. Simplify Persistence | Shared writer mechanics with unchanged raw contracts | C6-C7 | Phase 1; fixed writer/evidence contract | Partial or divergent writes |
| 5. Harden Delivery | Smaller verified bundles and approved real-environment recovery checks | C8-C9 | Phases 3-4 for final bundle/integration checks | Missing dependencies or incomplete recovery |
| 6. Retire Redundancy | Approved interface removals and compact current documentation | C2; all documentation | Phases 2-5 and caller migration | Breaking external users |

Controller consolidation is not a prerequisite for a private writer-helper change.
Runtime and persistence changes can be prepared independently against fixed contracts;
integrate and accept each exact commit separately before combining deployed changes.

## Phase 0: Agree Contracts

**What it does:** Fix the implementation contract before changing behavior.
**Changes:** Boundaries, result categories, real-node validation procedure and
baseline measurements; no runtime changes or new testing framework.

### Tasks

- [x] **0.1** Confirm each component's inputs, outputs and mutation boundary; list
  commands/callers that must keep working. Merge responsibilities where separation
  adds handoffs without independent logic; nine current labels are not nine required modules.
- [x] **0.2** Agree the [run result categories](Redesign.md#run-result-categories):
  not started, running, incomplete, missing results, results without DB writes,
  partial writes and fully recorded. Keep pass/fail and unknown/conflicting reads separate.
  Apply C3's coverage/retest rules to these facts; a category alone never authorizes rerun.
- [x] **0.3** Specify designated-host admission, durable intent/receipt and trusted
  expected identities using existing state/models. Check whether the same Job started
  before retrying a lost confirmation; introduce no general workflow engine.
- [x] **0.4** Define one lean real-node validation procedure using existing CLI,
  logs/results and read-only raw-DB verification. Record exact commit, node, settings,
  expected result category and acceptance evidence; obtain approval before execution.
- [x] **0.5** Record exact source, component sizes, subprocess/API counts, startup
  time and bundle size where measurable. Label unavailable measurements explicitly.

**Done when:** Each responsibility has one owner, the categories and failure rules
are clear, and the real-node procedure plus available baseline measurements are
recorded. No fake test infrastructure is created; unobserved cases remain unverified.
**Risk/control:** Low; unresolved caller/freshness decisions block affected changes.

### Phase 0 Completion Evidence

| Task | Recorded Decision / Evidence |
| --- | --- |
| 0.1 | [Ownership contract](major-components-overview.md#phase-0-ownership-contract) and [caller inventory](operator-interface-major-component.md#phase-0-compatibility-inventory); preserve existing interfaces, including externally unknown hooks. |
| 0.2 | [Result decision rules](Redesign.md#phase-0-decision-rules) and [retest decisions](discovery-scheduling-major-component.md#phase-0-retest-decisions); incomplete evidence never becomes pass/fresh coverage. |
| 0.3 | [State contract](job-control-major-component.md#phase-0-state-contract); use existing session storage, version new intent records, preserve legacy files and reconcile uncertain creates. |
| 0.4 | [Real-node procedure](../operations-runbook.md#real-node-development-procedure); verified against current CLI/collector flags, not executed as a submission. |
| 0.5 | [Baseline record](major-components-overview.md#phase-0-baseline-record); exact source/file counts and archive size measured, unmeasured runtime costs explicitly unavailable. |

Evidence belongs to this Phase 0 documentation change and the baseline named above.
No fake tests, new runtime modules or synthetic DBs were created. External caller
uncertainty blocks retirement; missing future provenance blocks activation of stricter
compatibility rules. Neither is treated as a successful validation or silently ignored.

## Phase 1: Fix Correctness

**What it does:** Close safety/evidence gaps before refactoring.
**Changes:** Config propagation, eligibility, receipts and verification;
CLI names and raw schemas remain.

### Implementation Status

Source implementation for **1.1 through 1.6 is complete**. The unchecked task
boxes below include real-cluster acceptance and must not be interpreted as accepted
deployment. Read-only real-history and isolated local OS checks are recorded; no
new-source benchmark or controller turnover has occurred.

Current behavior belongs to the linked component contracts. Detailed release and
validation histories remain in Git and the retained `run-logs/phase*` records;
do not duplicate them in each task. Phase 2 supersedes routine full-store audits
and complete-failure coverage, without removing exact-run acceptance checks.

### Tasks

- [ ] **1.1** Freeze effective config per command/controller session and pass it
  through callers; require complete main/per-test TOMLs with no setting defaults.
  See the [strict config contract](../configuration.md#strict-two-level-contract).
- [ ] **1.2** Share a structured eligibility function. Require explicit Ready and
  complete resource evidence; permit cordon only for approved targeted validation.
  See [eligibility and verification](discovery-scheduling-major-component.md#phase-12-implementation).
- [ ] **1.3** Serialize all supported submitters through C4's shared admission state;
  recheck eligibility for `node_name` immediately before create. Retain source/policy gates.
  See [implementation and limits](job-control-major-component.md#phase-13-implementation).
- [ ] **1.4** Persist intent before create and retain every outcome. Test lost replies,
  intent/receipt write failures, concurrent submitters and a misleading single NotFound.
  See [intent recovery](job-control-major-component.md#phase-14-implementation).
- [ ] **1.5** Implement outcome-specific exact-run verification against expected
  request identity; bound generation-change retries. Keep evidence and acceptance separate.
  See [exact-run verification](raw-evidence-major-component.md#phase-15-implementation).
- [ ] **1.6** Implement compatible-coverage reads and exact-run exports; distinguish
  forbidden/unavailable history from empty or incomplete evidence. Keep the declared
  legacy age policy until new dependency provenance is produced and its comparison
  is accepted; review output compatibility before activating stricter coverage rules.
  See [coverage and exports](discovery-scheduling-major-component.md#phase-16-implementation).

**Done when:** Approved real-node runs and retained real failure evidence demonstrate
the required eligibility, receipt and exact-run evidence behavior. Document cases
not safely exercised; never claim they passed. Complete the [release gate](#release-gate)
before activation; inspecting busy nodes does not authorize submitting to them.
**Risk/control:** High; isolate eligibility, receipts and verification in separate
commits with compatibility checks.

## Phase 2: Consolidate Control

**What it does:** Track each submitted Job through Kubernetes status and compact
runtime logs, using one small shared control path. Do not restart failed workloads.
**Changes:** Loop/CLI delegation, explicit no-restart policy, and producer-owned
completion logs; preserve commands, evidence, approval gates and cooldown semantics.

### Reviewed Direction

User decision (2026-09-24): ordinary tracking needs bounded `kubectl` Job/pod
observations plus the corresponding container logs. It does not need a second
workflow engine, new tracking service, or repeated full database/artifact audits.
Keep detailed exact-run audits for separately approved release acceptance and
diagnosis; logs are not a substitute for that acceptance evidence.

The runtime produces the information the tracker needs. C5's supervisor owns the
final run summary, the runner owns test progress/outcomes, and C7 owns truthful
ingestion completion acknowledgements. Reuse `CVAL_EVENT` and existing files;
do not add another log schema/parser stack. See the
[compact runtime log contract](validation-runtime-major-component.md#phase-2-compact-runtime-logs).

A definitively failed terminal Job is recorded and left failed. Move on to other
eligible nodes; never restart its pod, restart/recreate that Job, or immediately
resubmit the same run. Later testing is a new run selected by the scheduler after
normal eligibility, age and cooldown checks. Controller resume means observe the
existing Job, not resume or replay the workload. Unknown ownership, a lost create
reply or an unavailable API is not a confirmed failed Job.

Current code can persist `fail`/`incomplete` status rows, and Phase 1 coverage could
count a fully exercised failed run. Therefore absence of DB rows is not a valid
failure/retest rule. Phase 2.5 now explicitly separates the latest attempt from the
latest usable successful-test evidence: failure is retained, not relabeled as a
fresh pass. Success-based retest freshness is an intentional change
to the prior complete-failure coverage policy, to document and verify before
activation; no status rows are suppressed or deleted to force retesting.

### Tasks

- [ ] **2.1** Define and produce compact run/test/ingestion milestones and one final
  summary through existing runtime logging. Only the component completing an action
  may acknowledge it; keep test outcome and ingestion outcome separate.
  See [runtime verification](validation-runtime-major-component.md#phase-21-verification).
- [ ] **2.2** Make validation single-attempt at container, Job and task levels. Verify
  the installed Volcano retry/restart behavior, including failure policies; do not
  assume `restartPolicy: Never` alone covers it. Settings belong in TOML and must
  be validated by rendering. Preserve failed Jobs/pods and their evidence.
  Installed-controller/CRD inspection was RBAC-forbidden; live compatibility is
  unverified. See [single-attempt settings](../configuration.md#single-attempt-validation).
- [ ] **2.3** Share one bounded status/log observation step between active, resumed
  and targeted tracking. Reuse Phase 1 submission/intent safeguards; do not rebuild
  them. `run` submits, `validate` also watches/reports, and `plan` stays read-only.
  See [shared tracking](job-control-major-component.md#phase-23-implementation).
- [ ] **2.4** Batch Job/pod reads, bind logs to the expected pod/container and run,
  and propagate one remaining deadline. Read bounded incremental logs and require
  an explicit ingestion success event, never a generic "Completed" substring.
  Retain fresh per-create node checks and unknown-outcome safeguards.
  See [bounded batch reads](job-control-major-component.md#phase-24-implementation).
- [ ] **2.5** Retire duplicated shell monitoring/conversions after equivalent real-run
  comparison. Record terminal failures and advance other work without workload
  retries. Implement the reviewed retest-freshness rule without erasing failed raw
  status. Keep detailed audits separate from routine tracking and preserve the
  independently gated acceptance workflow.
  See [shared shell control](job-control-major-component.md#phase-25-implementation)
  and [freshness policy](discovery-scheduling-major-component.md#phase-25-successful-freshness).

**Done when:** On approved real runs, Kubernetes state and the compact runtime log
explain execution, retained logs/results and ingestion outcome. Confirmed failure
does not restart/recreate work or stop unrelated scheduling; uncertain submission
is not blindly repeated. Verify controller resume, cooldown and retest semantics,
with fewer subprocess/API calls and no routine full-store rescans. Missing real
failure coverage remains explicit rather than replaced by a simulator.
**Risk/control:** High; migrate one path at a time, reconcile outstanding Jobs and
verify a single active controller before any separately approved activation.

## Phase 3: Simplify Runtime

**What it does:** Fix demonstrated runtime defects and remove duplicate setup paths.
**Changes:** Reuse existing supervisor/runner helpers and environment projections.
Historical results, test settings and test-specific behavior remain readable.

### Reviewed Scope

User direction (2026-09-24): keep the codebase trimmed. A change must fix a
demonstrated defect or remove duplication, not introduce another runtime framework.
Image, CUDA, PyTorch and iterations are deliberately varied test inputs. Preserve
the existing configuration and raw evidence used to compare those environments;
do not fix them to one version or require a new image. A per-run snapshot records
that run's inputs, not a fleet-wide version constraint.

Start with the confirmed child-cleanup defect in **3.3**, then take **3.1** and
**3.2** individually. Keep standalone execution test-only, preserve result binding
and ingestion authorization, and remove superseded implementations when replacing
them. No new service, script, raw store or generic context framework. Custom image,
FIO packaging and additional timing fields are deferred unless separately justified.

### Tasks

- [ ] **3.1** Share secure reservation for production and supported standalone
  callers; reuse existing helpers before retiring old path setup, not a new compatibility layer.
  See [reservation ownership](validation-runtime-major-component.md#phase-31-shared-reservation).
- [ ] **3.2** Reuse the existing config snapshot and secure layout to derive child
  environment and compatibility aliases once. Reject disagreements; add no new context layer.
  See [environment projections](validation-runtime-major-component.md#phase-32-environment-projection).
- [ ] **3.3** Preserve separate phase/outcome values and bounded child cleanup;
  freeze the finalized result before ingestion and test interruption at each boundary.
  See [bounded child cleanup](validation-runtime-major-component.md#phase-33-child-cleanup).
- [ ] **3.4** Address mutable external DL code/plan only after defining the smallest
  per-run consistency change. Keep source/plan selectable across runs; do not pin
  image/CUDA/PyTorch versions or build a new image as part of this phase.
  See [per-run source isolation and its limits](test-workloads-major-component.md#phase-34-run-local-dl-source).
- [ ] **3.5** Use existing timing/events and recorded environment fields to compare
  approved runs. Add instrumentation only for a demonstrated missing measurement.
  Review found no need for instrumentation. See [timing interpretation](../result-schema.md#timing-interpretation)
  and [historical comparison limits](validation-runtime-major-component.md#phase-35-evidence-review).

**Done when:** Approved exact-commit real-node validation confirms intended coverage,
retained artifacts and dependency identity. Check safe rejection cases without
altering canonical evidence; separately approve any signal/failure exercise.
**Risk/control:** High for evidence handling; preserve variable test environments.
Keep runtime fixes separate from environment changes so comparisons remain interpretable.

## Phase 4: Simplify Persistence

**What it does:** Remove duplicated writer mechanics without changing data ownership.
**Changes:** One private helper in the existing DL module. Keep all seven raw
stores, explicit schemas/SQL and public writer contracts; no reader rewrite or
new configuration, persistence framework, script or database.

### Tasks

- [ ] **4.1** Extract common DL receipt/replacement/generation/commit mechanics;
  leave fixed schemas, columns and test-specific validation explicit.
  Source implemented. Expanding the helper reproduces both original writer ASTs
  exactly. See [transaction ownership and verification](raw-evidence-major-component.md#phase-4-writer-consolidation).
- [ ] **4.2** Preserve independent writer authorization, persistent POSIX locking,
  idempotent retries and final status publication only after required metrics.
  Preservation review complete: these paths, receipt comparison and cross-store
  finalization are unchanged. Commit/rollback behavior remains to be exercised.
- [x] **4.3** Review reader optimization only for a measured need. No change is
  justified in this phase: scheduling uses status history and canonical summaries,
  not per-rank metric scans. Keep existing read bounds and exact-run verification.
- [ ] **4.4** Compare exact-run rows/receipts and publication behavior from approved
  real runs. Inspect retained real failures read-only; any controlled interruption
  needs separate approval. Do not corrupt records to manufacture test cases.
  Deferred until all phases are implemented; no DB writes or node tests were run
  for the extraction. The unchecked implementation tasks retain this acceptance gate.

**Done when:** Rows, schemas, receipts and complete-generation semantics match the
contract on real-run evidence and historical readers work. Unobserved failure paths
remain unverified; do not replace missing proof with synthetic DBs or live corruption.
**Risk/control:** High; never shadow-write live data with both implementations,
delete partial raw rows, or use a generic transaction abstraction across stores.

## Phase 5: Harden Delivery

**What it does:** Trim the source bundle without breaking the existing delivery path.
**Changes:** One archive-selection exclusion with measured savings;
reuse the current source renderer, bootstrap, release checks and recovery tools.

### Reviewed Scope

The Phase 1-4 retrospective favors one owner per behavior, not new frameworks or
extra fields. Source hashing, embedded commit checks and immutable ConfigMap delivery
already exist. Do not rebuild them or add a package manager, wheel-distribution
workflow, bundle variants, new service or generated dependency-analysis system.

The top-level `tests/` archive entry is now excluded, not deleted from the
repository. Extracted-bundle checks cover imports, package data and dynamic helpers:
the ingestion lock and admission cooldown are loaded from `scripts/`. Preserve
complete registered workload directories, descriptors, plugin support files,
templates and the shared bootstrap. Do not blindly remove all scripts or inspect
only static Python imports when deciding what the runtime needs.

Images, CUDA, PyTorch and workload settings remain selectable test inputs. No new
image, dependency version pin, automatic deployment, backup or cleanup is in scope.
Real-node and recovery exercises remain deferred until all phases are implemented,
with exact approval required for each live mutation.

### Tasks

- [x] **5.1** Measure a candidate bundle with confirmed host-only files excluded.
  Change only the existing source renderer's selection; keep required files byte-identical
  to the commit. Report compressed size, member count and missing dependencies.
  Keep uncertain files rather than add wrappers or fallback downloads.
  Implemented: only top-level tests are excluded; 33 fewer files and about 33%
  smaller compressed archive. All other archived bytes match the same commit.
- [ ] **5.2** Reuse existing archive/hash/commit and render checks on the prepared
  bundle, independently of the host checkout. Preserve both source transports and
  placement templates. Local import/render checks are packaging checks, not GPU
  acceptance; actual in-pod startup/ingestion verification waits until all phases finish.
  Local checks passed: 43 modules imported from an isolated extracted tree, both
  dynamic helpers loaded, and both templates rendered with both source transports.
  One real-archive regression is retained in the existing renderer suite; this
  checkbox stays open for in-pod acceptance. See [packaging checks](source-delivery-major-component.md#phase-5-packaging).
- [x] **5.3** Review recovery instructions against existing tools and retained
  manifests, read-only. Fix only demonstrated inconsistencies. No backup, journal
  migration, restore, PVC move or raw-data modification is part of implementation.
  Help flags and operation-specific guards agree; a retained consolidation plan
  digest passed. Failed/empty historical records were not treated as verified backups.
- [x] **5.4** Leave maintenance schemas and APIs unchanged by default. Different
  operations do not need identical manifests. Consolidate only a proven duplicate
  with a smaller replacement; otherwise record no change as the outcome.
  No demonstrated defect or useful common schema: maintenance code is unchanged.
- [x] **5.5** Link the existing runbook procedure and retain one concise acceptance
  checklist for the final exact commit. No new script, test harness or duplicate
  release document. Keep publication, source creation, acceptance and activation separate.
  The [existing runbook table](../operations-runbook.md#real-node-development-procedure)
  is the final acceptance checklist; it records the separate post-phase approval gates.

**Source complete when:** Justified exclusions and local packaging checks pass,
recovery inconsistencies are resolved or documented, and deferred gates are explicit.
**Accepted when:** After all phases, approved real Jobs run from the bundle without
undeclared host files; any separately approved recovery exercise preserves evidence.
**Risk/control:** Do not trade a few archive bytes for fragile startup or hidden
downloads. Keep old bundles and raw evidence; never run live recovery to tick a box.

## Phase 6: Retire Redundancy

**What it does:** Remove proven duplicate interfaces and documentation after migration.
**Changes:** Approved compatibility hooks/aliases, obsolete branches and duplicate
explanations. Do not remove safety checks or historical readers to meet a size target.

### Tasks

- [ ] **6.1** Recheck Phase 0's caller inventory; obtain explicit retirement decisions
  and publish replacements before removing any supported command or alias.
- [ ] **6.2** Delete replaced implementations only after callers and regression tests
  have been reviewed and real runs use the retained owner; preserve inspection versus mutation.
- [ ] **6.3** Update current architecture, config, runbook and schema references to
  implemented behavior; retain dated acceptance records and repair links.
- [ ] **6.4** Remeasure source/config/test sizes and operating costs with the baseline
  method. Explain reductions by removed duplication, not relocated or compressed code.
- [ ] **6.5** Reconcile the improvement table and component proposals with completed
  work; list remaining decisions and acceptance/activation status explicitly.

**Done when:** No known caller or operational contract is stranded; required real-run
evidence and links verify; measurements and remaining backlog are recorded.
**Risk/control:** Medium compatibility risk; retain rollback source and defer
retirement when external usage is unknown.

## Release Gate

Gate each runtime change, not just phase completion. Documentation-only changes
need content/link checks and publication, not GPU jobs.

1. Review only task changes; use lightweight syntax/content checks as needed and
  publish an isolated exact commit. Runtime acceptance always uses real-node tests.
2. Render the exact source ConfigMap offline; verify archive identity/hash and
   source/config/manifest references against trusted expectations. Retain previous artifacts.
3. Obtain exact create/rollback approval; create and read back the immutable source
   object. Creation is not activation or permission to update an active session.
4. Obtain validation approval; recheck one fully free Ready node, required resources,
   exact source and known workload state immediately before submission.
5. Require terminal Job phase, canonical results, passing required tests, exact-run
  raw evidence and retained logs under the [acceptance contract](Redesign.md#outcome-and-evidence-contract).
  Scheduler-selected quick tests do not replace targeted release acceptance.
6. Obtain separate controller activation approval, reconcile outstanding Jobs and
   verify fresh execution at the approved source. Keep publication, acceptance and
   active identities separate in the release record.

On failure, retain evidence and stop. Rollback requires approval and compatible
source. No blind resubmit, incompatible writer downgrade, restore over newer raw
writes or second controller against unresolved work.

## Review Before Closing a Task

1. Name the behavior being preserved or deliberately changed, and its owning component.
2. Link real-run evidence for the behavior; label failure cases not safely exercised.
3. Compare public outputs, raw schemas/rows and historical readability where affected.
4. Record exact commit/config, focused checks and remaining acceptance/activation gates.
5. Delete the old path only after caller migration; a file move or added wrapper
  alone is not a reduction in complexity.
