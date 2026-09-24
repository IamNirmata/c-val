# Redesign Development Plan

Status: Phase 0 contracts complete; Phase 1.1 source implementation is under verification.
Real-node execution/ingestion acceptance and activation remain pending; later tasks are not started.
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
  Fresh exact-commit execution, live release transitions and generation-change retry
  acceptance remain pending; activation stays deferred until Phase 1 is finished.
- [ ] **1.6** Implement compatible-coverage reads and exact-run exports; distinguish
  forbidden/unavailable history from empty or incomplete evidence. Keep the declared
  legacy age policy until new dependency provenance is produced and its comparison
  is accepted; review output compatibility before activating stricter coverage rules.

**Done when:** Approved real-node runs and retained real failure evidence demonstrate
the required eligibility, receipt and exact-run evidence behavior. Document cases
not safely exercised; never claim they passed. Complete the [release gate](#release-gate)
before activation; inspecting busy nodes does not authorize submitting to them.
**Risk/control:** High; isolate eligibility, receipts and verification in separate
commits with compatibility checks.

## Phase 2: Consolidate Control

**What it does:** Replace repeated shell orchestration with one job-control path.
**Changes:** Loop internals, monitoring and CLI delegation; preserve commands,
session records and cooldown semantics.

### Tasks

- [ ] **2.1** Extract one bounded lifecycle transition function into existing Python
  orchestration; reuse the existing clock/Kubernetes helpers without new test abstractions.
- [ ] **2.2** Share resume/active monitoring; preserve receipts, cooldown repair and
  occupied slots for uncertain submissions or pending verification. Block unknown ownership.
- [ ] **2.3** Route `run` and `validate` through shared submission/reconciliation;
  keep targeted watch/verification and read-only `plan` distinct.
- [ ] **2.4** Batch observation reads and propagate one remaining deadline. Retain
  fresh per-create checks; never cache eligibility across submissions.
- [ ] **2.5** Keep shell for launch/session duties; remove duplicated conversions
  after comparing behavior on equivalent approved real runs, never duplicate submits.

**Done when:** Crash/restart, uncertain create, missing Job, unavailable API,
cooldown and separately gated pruning observations agree with the retained contract
where verified. Comparable real runs show fewer subprocesses/API calls; missing
failure coverage remains explicit rather than replaced by a simulator.
**Risk/control:** High; migrate one path at a time, reconcile outstanding Jobs and
verify a single active controller before any separately approved activation.

## Phase 3: Simplify Runtime

**What it does:** Give execution one secure evidence owner and pin what benchmarks run.
**Changes:** Supervisor/runner handoffs, child environment construction and workload
dependency preparation. Historical results and test-specific behavior remain readable.

### Tasks

- [ ] **3.1** Share secure reservation for production and supported standalone
  callers; reuse existing helpers before retiring old path setup, not a new compatibility layer.
- [ ] **3.2** Derive child environment and compatibility aliases from one immutable
  run definition. Reject disagreements rather than applying hidden overrides.
- [ ] **3.3** Preserve separate phase/outcome values and bounded child cleanup;
  freeze the finalized result before ingestion and test interruption at each boundary.
- [ ] **3.4** Preinstall FIO and use a pinned image plus immutable DL artifact/run-local
  copy and plan. Test mutation between preflight and launch; never backfill old identities.
- [ ] **3.5** Record setup, execution and ingestion timings separately; compare
  outcomes, artifacts and resource use across real runs with equivalent workload inputs.

**Done when:** Approved exact-commit real-node validation confirms intended coverage,
retained artifacts and dependency identity. Check safe rejection cases without
altering canonical evidence; separately approve any signal/failure exercise.
**Risk/control:** High for evidence handling; separate runtime refactoring from
dependency upgrades so changed benchmark measurements have an attributable cause.

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
