# Redesign Development Plan

Status: Phase 0 contracts complete; Phases 1.1-1.6 and 2.1-2.5 source implemented.
Fresh exact-commit execution/ingestion acceptance and activation remain pending;
Phase 3.1-3.4 cleanup source is implemented; Phase 3.5 evidence review is complete,
but fresh accepted-run comparisons remain pending.
The live loop remains paused since 2026-09-24 UTC.
Phase 0 source baseline: `b79a5e0` (2026-09-23). No runtime changes or new real-node
acceptance are claimed. Implementation and live mutation approvals remain separate.
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

The final implementation also closes shared cooldown and lost-reply resume paths:
all submitters use the existing cooldown contract, resume reconciles durable intents
without depending on CLI receipts, derived CSVs can be rebuilt from intact state,
and multi-intent reconciliation shares a bounded deadline. Phase 2 consolidation,
Phase 3 dependency provenance, and cross-host fencing are not included.

Activation remains separately gated. Next: approve the final exact source object,
review session turnover with retained outstanding work, then perform approved
fresh execution/ingestion and recovery acceptance. Completing source is not
permission to stop, restart, cancel or submit live work.

### Tasks

- [ ] **1.1** Freeze effective config per command/controller session and pass it
  through callers; require complete main/per-test TOMLs with no setting defaults.
  Source published at `45b4d5eb23a218868038189d1055b1d96f561979`; the approved
  source ConfigMap was created. The approved validation on `slc01-cl02-hgx-0052`
  stopped before submission because all eight GPUs were occupied. Execution,
  ingestion and session-turnover acceptance remain pending. See
  the [strict config contract](../configuration.md#strict-two-level-contract).
- [ ] **1.2** Share a structured eligibility function. Require explicit Ready and
  complete resource evidence; permit cordon only for approved targeted validation.
  Source implemented; bulk/targeted decisions matched on one retained real
  snapshot of 500 nodes and 12,592 pods (2026-09-24 UTC), including restartable
  init containers and two NotReady nodes. Automatic cordon exclusion, NoExecute
  blocking and per-node unverified diagnostics are explicit. See
  [implementation and verification](discovery-scheduling-major-component.md#phase-12-implementation).
  Source published at `c3524b67163561dc558d2f7ee1eedf41bbf080f7`; its approved
  ConfigMap was created and verified. The approved `0307` validation stopped before
  submission because all eight GPUs were busy and free CPU was insufficient.
  Execution/ingestion acceptance remains pending; no validation Job was submitted.
- [ ] **1.3** Serialize all supported submitters through C4's shared admission state;
  recheck eligibility for `node_name` immediately before create. Retain source/policy gates.
  Source implemented in the existing manager/state helper. Explicit host/cluster
  scope, an absolute shared state root, session/config binding, outstanding-work
  checks and the short nonblocking lock cover both submitters and state writers.
  Real read-only checks rejected busy `0307`, pending validation on `0262`, and the
  legacy active session; real local processes verified lock exclusion/release.
  See [implementation and limits](job-control-major-component.md#phase-13-implementation).
  Source published at `8a9953e1b18663c1d876d85a6c532767ad577f68`; its approved
  immutable source ConfigMap was created and verified on 2026-09-24 UTC.
  No new live session, validation Job or controller activation was performed.
  Live turnover is deferred until Phase 1 is finished, as requested. Successful
  Job creation and exact-run acceptance remain pending.
- [ ] **1.4** Persist intent before create and retain every outcome. Test lost replies,
  intent/receipt write failures, concurrent submitters and a misleading single NotFound.
  Source implemented with v2 session intents, retained approval/manifest identities,
  durable create observations and partial batch responses. Same-identity read-only
  reconciliation never retries create; missing/conflicting/unavailable observations
  retain reservations. Real Job/NotFound reads and isolated real filesystem failures
  were checked without synthetic Kubernetes clients or live session changes. See
  [implementation and verification limits](job-control-major-component.md#phase-14-implementation).
  Source published at `31023001db1010780c4d35eefb25ce064627a24e`; its approved
  immutable ConfigMap was created and verified on 2026-09-24 UTC.
  Real create reply loss, partial batches and concurrent submitters remain unverified;
  live-loop activation stays deferred until Phase 1 is complete.
- [ ] **1.5** Implement outcome-specific exact-run verification against expected
  request identity; bound generation-change retries. Keep evidence and acceptance separate.
  Source implemented with one read-only C7 verifier shared by targeted validation
  and terminal intent reconciliation. Canonical identity/digests/events, exact status
  rows, storage/NCCL values, DL ranks/rows/receipts and generation stability are checked
  against submitted context. The historical `0192` run at `1790178532` verified fully;
  a failed historical Job without a retained pod remained unavailable, and the absent
  approved `0307` run remained incomplete. No synthetic DBs or new validation Jobs
  were used. See [verification and limits](raw-evidence-major-component.md#phase-15-implementation).
  Source published at `f0cdff9dbb603823adc5cfa956a17b21484d8554`; its approved
  immutable ConfigMap was created and verified on 2026-09-24 UTC.
  Fresh exact-commit execution, live release transitions and generation-change retry
  acceptance remain pending; activation stays deferred until Phase 1 is finished.
- [ ] **1.6** Implement compatible-coverage reads and exact-run exports; distinguish
  forbidden/unavailable history from empty or incomplete evidence. Keep the declared
  legacy age policy until new dependency provenance is produced and its comparison
  is accepted; review output compatibility before activating stricter coverage rules.
  Implemented `status --coverage`, shared complete-run history for CLI/live planning,
  labeled legacy passing coverage, exact `results --node --timestamp` selection,
  timestamp-matched metric joins and fail-closed status/metric reads. CSV columns
  remain unchanged; missing exact status produces no CSV. A real 478-node scan found
  379 canonical, 97 legacy passing and two unproven coverage records. All four
  built-in historical exports passed. See
  [coverage behavior](discovery-scheduling-major-component.md#phase-16-implementation).
  Complete failed-run coverage, unavailable/forbidden live failures and new-source
  controller behavior remain unexercised. Strict dependency compatibility is still
  deferred until its producer exists; no provenance was fabricated or backfilled.

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
  Source implemented: existing events gain identity/timing, ingestion emits per-store
  acknowledgements, and the supervisor emits one digest-bound final summary after
  child log capture. Unconfirmed ingestion/log capture never becomes success.
  Local pipe/descriptor checks and retained real-event compatibility checks passed;
  new-source real-pod execution and writer failure paths remain unverified. See
  [runtime verification](validation-runtime-major-component.md#phase-21-verification).
  This task does not change Job retries, process test-failure exit semantics,
  tracking, coverage policy or active cluster resources. The checkbox stays open
  until its exact-commit acceptance gate is met.
- [ ] **2.2** Make validation single-attempt at container, Job and task levels. Verify
  the installed Volcano retry/restart behavior, including failure policies; do not
  assume `restartPolicy: Never` alone covers it. Settings belong in TOML and must
  be validated by rendering. Preserve failed Jobs/pods and their evidence.
  Source implemented in the existing config, renderer and both validation templates:
  `Never`, explicit Job/task `maxRetry=1`, and inherited `TerminateJob` policies for
  PodFailed/PodEvicted/TaskFailed/JobUnknown. Zero is rejected because inspected
  upstream admission replaces it with three. Conflicting policies, container restart
  overrides, replica changes and automatic TTL cleanup are rejected by rendering.
  Both templates/source transports and strict snapshots passed local checks using
  retained real run inputs; an actual legacy Job with default retries was rejected.
  Installed controller Deployment/CRD reads were forbidden, so version/admission
  verification and real single-attempt acceptance remain pending. The approved
  `cval-source-20fe5537c3a34d24` ConfigMap was created and verified on 2026-09-24 UTC;
  the separately requested controller pause did not cancel Jobs or change the session.
  No new source was activated. See
  [configuration and upstream semantics](../configuration.md#single-attempt-validation).
- [ ] **2.3** Share one bounded status/log observation step between active, resumed
  and targeted tracking. Reuse Phase 1 submission/intent safeguards; do not rebuild
  them. `run` submits, `validate` also watches/reports, and `plan` stays read-only.
  Source implemented in the existing monitor, manager, targeted validator and state
  helper. Each observation verifies the submitted Job and owned pod/container, reads
  bounded logs under one deadline, and separates terminal tracking from confirmed
  runtime completion. Routine active/resumed tracking no longer scans raw stores;
  targeted validation retains its once-only exact-run audit outside polling.
  Real historical observations covered a retained successful pod and a terminal Job
  whose pod is gone. Neither old log history was promoted to new-runtime acceptance.
  Missing Jobs, unavailable status and identity conflicts retain reservations. See
  [implementation and verification limits](job-control-major-component.md#phase-23-implementation).
  Fresh new-source completion, failure, and live resume/state transitions remain
  separately approval-gated; the checkbox stays open until acceptance.
- [ ] **2.4** Batch Job/pod reads, bind logs to the expected pod/container and run,
  and propagate one remaining deadline. Read bounded incremental logs and require
  an explicit ingestion success event, never a generic "Completed" substring.
  Retain fresh per-create node checks and unknown-outcome safeguards.
  Source implemented in the existing monitor and manager. CLI phase reads use one
  complete Job inventory; multi-intent reconciliation shares Job/pod inventories
  and one deadline, retaining each outcome before deciding whether to block.
  Timestamped log reads resume from an identity-bound boundary second and preserve
  compact progress/acknowledgements; incomplete byte-limited output stays unconfirmed.
  Read-only real-Job checks matched exact phase reads, used four kubectl calls for
  two historical Jobs with one retained pod, and reduced that pod's repeated log
  read from 285,339 to 2,140 bytes. Expired deadlines and a real 4,096-byte cap were
  checked. See [implementation and limits](job-control-major-component.md#phase-24-implementation).
  Scalar shell state-helper calls remain until 2.5; do not claim all shell polling
  is batched. No new workload, source object or controller activation occurred.
- [ ] **2.5** Retire duplicated shell monitoring/conversions after equivalent real-run
  comparison. Record terminal failures and advance other work without workload
  retries. Implement the reviewed retest-freshness rule without erasing failed raw
  status. Keep detailed audits separate from routine tracking and preserve the
  independently gated acceptance workflow.
  Source implemented: active/resumed shell watching shares one batched durable
  reconciliation/report helper; separate phase reads and per-Job state updates are
  removed from routine polling. Five old private helpers became two shared helpers,
  reducing the shell by 194 lines. Terminal slot release is consumed explicitly,
  independent of successful tests/ingestion; unresolved identity still blocks.
  Coverage uses `successful_complete_run`, rejecting old-policy reports and skipping
  failures while preserving latest attempts, older successes, cooldown and raw rows.
  A real 478-node history scan retained 476 successful records, skipped seven failed
  attempts and preserved older successes on four nodes. A real Failed Job without
  a retained pod released tracking in the local predicate/report check without
  becoming accepted. See [control verification](job-control-major-component.md#phase-25-implementation)
  and [freshness policy](discovery-scheduling-major-component.md#phase-25-successful-freshness).
  Live state writes, actual scheduling after failure and controller resume were not
  exercised; the loop stays paused. The checkbox remains open for fresh acceptance.

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
  Source implemented: standalone runner entry reserves through the existing supervisor
  helper; inherited descriptors still belong to the supervisor. One execution path
  uses retained descriptors, while standalone retains its own logging and remains
  test-only. Removed three setup helpers and 149 runner lines; no new abstraction,
  setting, script or raw store. Marker-only adoption now fails closed, and the root
  must already exist. Three real-filesystem checks passed for ownership, reuse and
  symlink rejection without starting benchmarks. Fresh standalone/supervised workload
  acceptance remains pending. See
  [reservation ownership and limits](validation-runtime-major-component.md#phase-31-shared-reservation).
- [ ] **3.2** Reuse the existing config snapshot and secure layout to derive child
  environment and compatibility aliases once. Reject disagreements; add no new context layer.
  Source implemented: one evidence projection in the existing runtime module is
  shared by supervisor and runner, using the existing built-in alias table and
  selected descriptor filenames. The ingestion shell validates supplied paths
  instead of reconstructing them. Conflicting run/test/global aliases fail closed;
  matching canonical paths can be replaced with their reserved descriptor paths.
  Five focused filesystem/environment checks passed, including the real shell
  handoff; configuration projection, workloads, results and raw writers are unchanged.
  Image/CUDA/PyTorch/iterations remain variable. No new setting, module or script.
  Fresh in-pod execution/ingestion acceptance remains pending. See
  [projection ownership and limits](validation-runtime-major-component.md#phase-32-environment-projection).
- [ ] **3.3** Preserve separate phase/outcome values and bounded child cleanup;
  freeze the finalized result before ingestion and test interruption at each boundary.
  Cleanup source implemented first: supervisor reuses the existing process-group
  terminator, bounds runner/ingestion children through two required runtime limits,
  and rejects a zero-exit leader that leaves descendants. Existing result binding,
  writers and test settings are unchanged. Three focused real-OS regression tests
  passed, including timeout and abandoned descendants; no GPU test or raw DB was
  fabricated. Fresh pod execution, signal-boundary and ingestion-interruption
  acceptance remain unverified and separately gated. See
  [cleanup implementation](validation-runtime-major-component.md#phase-33-child-cleanup).
- [ ] **3.4** Address mutable external DL code/plan only after defining the smallest
  per-run consistency change. Keep source/plan selectable across runs; do not pin
  image/CUDA/PyTorch versions or build a new image as part of this phase.
  Source implemented in the existing DL scripts: copy `src`, the selected plan and
  any baseline into the existing work directory; compare them with the inputs and
  execute the local source. Reused work directories are rejected rather than erased.
  The existing summary `source_dir` identifies the execution copy; settings, schemas
  and raw writers are unchanged. Two checks on retained actual DL source passed for
  isolation, reuse and an observed change during copying. Real setup preflight also
  passed locally. No benchmark was run; baseline-file and fresh pod acceptance remain
  unverified. See [per-run source boundary](test-workloads-major-component.md#phase-34-run-local-dl-source).
- [ ] **3.5** Use existing timing/events and recorded environment fields to compare
  approved runs. Add instrumentation only for a demonstrated missing measurement.
  Evidence review complete; no runtime change is justified. Existing event boundaries
  separate approximate setup/workload/ingestion intervals, and emitted monotonic
  totals already cover tests and newer ingestion/supervisor stages. Historical runs
  `0192/1790178532` and `0359/1790277187` have matching recorded environments,
  descriptors, requested resources and iteration/plan settings, but different nodes
  and source revisions. They demonstrate inspection, not a speedup or regression.
  `0359` canonical digest and exact raw environment/iteration records were checked;
  resource usage and fresh Phase 3 execution remain unmeasured. See
  [timing interpretation](../result-schema.md#timing-interpretation) and
  [review evidence](validation-runtime-major-component.md#phase-35-evidence-review).
  Keep this acceptance checkbox open until separately approved new-source runs and
  comparable evidence are available; do not add fields/scripts to replace that gate.

**Done when:** Approved exact-commit real-node validation confirms intended coverage,
retained artifacts and dependency identity. Check safe rejection cases without
altering canonical evidence; separately approve any signal/failure exercise.
**Risk/control:** High for evidence handling; preserve variable test environments.
Keep runtime fixes separate from environment changes so comparisons remain interpretable.

## Phase 4: Simplify Persistence

**What it does:** Remove duplicated writer mechanics without changing data ownership.
**Changes:** Private DL transaction helper and bounded read implementations, building
on Phase 1's verifier. Keep all seven raw stores and their public writer contracts.

### Tasks

- [ ] **4.1** Extract common DL receipt/replacement/generation/commit mechanics;
  leave fixed schemas, columns and test-specific validation explicit.
- [ ] **4.2** Preserve independent writer authorization, persistent POSIX locking,
  idempotent retries and final status publication only after required metrics.
- [ ] **4.3** Optimize the shared readers after measuring query plans; scheduling
  must not scan every rank's metrics each cycle. Preserve exact-run verification semantics.
- [ ] **4.4** Compare exact-run rows/receipts and publication behavior from approved
  real runs. Inspect retained real failures read-only; any controlled interruption
  needs separate approval. Do not corrupt records to manufacture test cases.

**Done when:** Rows, schemas, receipts and complete-generation semantics match the
contract on real-run evidence and historical readers work. Unobserved failure paths
remain unverified; do not replace missing proof with synthetic DBs or live corruption.
**Risk/control:** High; never shadow-write live data with both implementations,
delete partial raw rows, or use a generic transaction abstraction across stores.

## Phase 5: Harden Delivery

**What it does:** Make lean packaging, regression checks and recovery repeatable.
**Changes:** Runtime archive selection and approved real-environment verification;
no automatic backup, migration or deployment.

### Tasks

- [ ] **5.1** Identify runtime dependency closure, including dynamically loaded helpers;
  exclude host-only tests/files from bundles only after proving they are not required.
- [ ] **5.2** Verify archive identity/hash locally, then validate startup and required
  helpers in real Jobs. Approve each placement workflow separately; measure bundle reduction.
- [ ] **5.3** Review recovery using existing tools and retained real manifests. Any
  backup, journal migration or isolated restore needs exact scope/rollback approval;
  use verified real evidence, never fabricated data or overwrite of authoritative stores.
- [ ] **5.4** Standardize shared maintenance identity/plan/verification fields only
  where duplication is real; retain explicit action-specific gates and manifests.
- [ ] **5.5** Keep the real-run verification procedure in the existing runbook and
  retain source/config/evidence references. Do not add a parallel CI simulation suite.

**Done when:** A smaller bundle runs on a real node without undeclared dependencies;
approved recovery checks verify preserved evidence, or their remaining gate is explicit.
**Risk/control:** Medium delivery risk; high risk for live recovery operations,
which remain separately approved and never run merely to complete a checklist.

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
