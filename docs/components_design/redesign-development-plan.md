# Redesign Development Plan

Status: proposed; no development task below is marked complete.
Architecture baseline: `9930c63`; this pass revises the `37a69ca` development plan (2026-09-23).
Shared contracts live in [Redesign.md](Redesign.md).
Component IDs refer to the [major-component overview](major-components-overview.md).

## In Simple Terms

First make the safety rules and expected results testable. Then replace one
implementation at a time, compare it with the old behavior and retire the old
path only after verification. Writing these tasks does not complete or approve them.

## Working Rules

- Use phase numbers as the preferred rollout order; the map lists actual
  prerequisites. Independent fixtures/design work may start earlier, not bypass gates.
- Preserve public commands, canonical results and raw stores unless a migration is
  reviewed. No one-release rewrite, evaluator, generic per-test DB or new service.
- Retain raw history, test-owned persistence and explicit mutation approvals.
- Check off reviewed, published tasks with focused tests, updated docs and evidence links.
- Track **published**, **cluster-accepted** and **active** separately. Task completion
  never authorizes creation, restart, deletion or migration.
- Start automated offline checks in Phase 0. Never defer regression protection
  until the later packaging phase or combine an image upgrade with a large refactor.

## Phase Map

| Phase | What It Delivers | Components | Depends On | Main Risk |
| --- | --- | --- | --- | --- |
| 0. Agree Contracts | Fixed boundaries, callers and acceptance cases | All | Existing design | Unclear compatibility requirements |
| 1. Fix Correctness | Consistent eligibility, recoverable submission, exact-run evidence | C1-C4, C7 | Phase 0 | Unsafe jobs or misleading acceptance |
| 2. Consolidate Control | One lifecycle implementation for manual and continuous runs | C2-C4 | Phase 1 | Lost resume/cooldown state |
| 3. Simplify Runtime | One secure evidence setup and reproducible workloads | C1, C5-C6 | Phase 1; stable run-definition contract | Evidence integrity or changed measurements |
| 4. Simplify Persistence | Shared writer mechanics with unchanged raw contracts | C6-C7 | Phase 1; fixed writer/evidence contract | Partial or divergent writes |
| 5. Harden Delivery | Smaller verified bundles, recovery exercises and complete CI matrix | C8-C9; cross-cutting tests | Phases 3-4 for final bundle/integration checks | Missing dependencies or incomplete recovery |
| 6. Retire Redundancy | Approved interface removals and compact current documentation | C2; all documentation | Phases 2-5 and caller migration | Breaking external users |

Controller consolidation is not a prerequisite for a private writer-helper change.
Runtime and persistence changes can be prepared independently against fixed contracts;
integrate and accept each exact commit separately before combining deployed changes.

## Phase 0: Agree Contracts

**What it does:** Fix the implementation contract before changing behavior.
**Changes:** Decisions, fixtures and measurements; no runtime or storage changes.

### Tasks

- [ ] **0.1** Confirm each component's inputs, outputs and mutation boundary; list
  public commands, hidden writer hooks and external callers that must keep working.
- [ ] **0.2** Pin the coverage policy and outcome/evidence truth table in fixtures:
  full failure versus setup failure, disabled/result-only tests, changed workload
  inputs and unreadable history. Do not equate complete records with passing tests.
- [ ] **0.3** Specify designated-host admission, durable intent/receipt and trusted
  expected identities using existing state/models; define state-version migration.
- [ ] **0.4** Add fake node/job and temporary-store fixtures to existing suites;
  establish initial CI gates for offline tests, shell syntax and documentation links.
- [ ] **0.5** Record exact source, component sizes, subprocess/API counts, startup
  time and bundle size where measurable. Label unavailable measurements explicitly.

**Done when:** Contracts and failure checks are documented, baseline fixtures are
reproducible, and initial offline CI checks run without cluster mutation.
**Risk/control:** Low; unresolved caller/freshness decisions block affected changes.

## Phase 1: Fix Correctness

**What it does:** Close safety/evidence gaps before refactoring.
**Changes:** Config propagation, eligibility, receipts and verification;
CLI names and raw schemas remain.

### Tasks

- [ ] **1.1** Freeze effective config per command/controller session and pass it
  through callers; test edits during a session and retries using the original snapshot.
- [ ] **1.2** Share a structured eligibility function. Require explicit Ready and
  complete resource evidence; permit cordon only for approved targeted validation.
- [ ] **1.3** Serialize all supported submitters through C4's shared admission state;
  recheck eligibility/node UID immediately before create. Retain source/policy gates.
- [ ] **1.4** Persist intent before create and retain every outcome. Test lost replies,
  intent/receipt write failures, concurrent submitters and a misleading single NotFound.
- [ ] **1.5** Implement outcome-specific exact-run verification against expected
  request identity; bound generation-change retries. Keep evidence and acceptance separate.
- [ ] **1.6** Implement compatible-coverage reads and exact-run exports; distinguish
  forbidden/unavailable history from empty or incomplete evidence. Review output compatibility.

**Done when:** Fixtures reject busy/unknown nodes, retain earlier receipts after a
second-create failure, reconcile lost responses, and reject stale/missing evidence.
Existing CLI/discovery/submission/ingestion/export regressions pass. Complete the
[release gate](#release-gate) before activating these runtime changes.
**Risk/control:** High; isolate eligibility, receipts and verification in separate
commits with compatibility checks.

## Phase 2: Consolidate Control

**What it does:** Replace repeated shell orchestration with one job-control path.
**Changes:** Loop internals, monitoring and CLI delegation; preserve commands,
session records and cooldown semantics.

### Tasks

- [ ] **2.1** Extract one bounded lifecycle transition function into existing Python
  orchestration; inject clock and Kubernetes adapter for deterministic tests.
- [ ] **2.2** Share resume/active monitoring; preserve receipts, cooldown repair and
  occupied slots for uncertain submissions or pending verification. Block unknown ownership.
- [ ] **2.3** Route `run` and `validate` through shared submission/reconciliation;
  keep targeted watch/verification and read-only `plan` distinct.
- [ ] **2.4** Batch observation reads and propagate one remaining deadline. Retain
  fresh per-create checks; never cache eligibility across submissions.
- [ ] **2.5** Keep shell for launch/session duties; remove duplicated conversions
  after replaying old/new paths on identical fake observations.

**Done when:** Crash/restart, uncertain create, missing Job, unavailable API,
cooldown and separately gated pruning tests agree with the retained contract.
Comparable fixtures show fewer subprocesses/API calls without lost safeguards.
**Risk/control:** High; migrate one path at a time, reconcile outstanding Jobs and
verify a single active controller before any separately approved activation.

## Phase 3: Simplify Runtime

**What it does:** Give execution one secure evidence owner and pin what benchmarks run.
**Changes:** Supervisor/runner handoffs, child environment construction and workload
dependency preparation. Historical results and test-specific behavior remain readable.

### Tasks

- [ ] **3.1** Share secure reservation for production and supported standalone
  callers; keep pure unit-test helpers and an adapter before retiring old path setup.
- [ ] **3.2** Derive child environment and compatibility aliases from one immutable
  run definition. Reject disagreements rather than applying hidden overrides.
- [ ] **3.3** Preserve separate phase/outcome values and bounded child cleanup;
  freeze the finalized result before ingestion and test interruption at each boundary.
- [ ] **3.4** Preinstall FIO and use a pinned image plus immutable DL artifact/run-local
  copy and plan. Test mutation between preflight and launch; never backfill old identities.
- [ ] **3.5** Record setup, execution and ingestion timings separately; compare
  outcomes, artifacts and resource use against fixed workload fixtures.

**Done when:** Dynamic tests, disabled-test handling, signals, run reuse, symlink
swaps and descriptor-identity regressions pass; dependency changes alter recorded
identity. Approved exact-commit GPU acceptance confirms unchanged intended coverage.
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
- [ ] **4.4** Compare temporary stores before/after the change; inject lock contention,
  interruption, divergent receipts, failure between commits and lost acknowledgement
  after final commit. Preserve committed rows in every failure report.

**Done when:** Rows, schemas, receipts and complete-generation semantics match the
baseline; partial writes cannot publish final status. Historical readers still work.
**Risk/control:** High; never shadow-write live data with both implementations,
delete partial raw rows, or use a generic transaction abstraction across stores.

## Phase 5: Harden Delivery

**What it does:** Make lean packaging, regression checks and recovery repeatable.
**Changes:** Runtime archive selection, extended offline CI and combined recovery fixtures;
no automatic backup, migration or deployment.

### Tasks

- [ ] **5.1** Identify runtime dependency closure, including dynamically loaded helpers;
  exclude host-only tests/files from bundles only after proving they are not required.
- [ ] **5.2** Test unpacked offline startup for both placement templates, exact commit,
  archive hash, descriptor availability and required helpers; measure bundle reduction.
- [ ] **5.3** Exercise backup, journal migration and isolated restore, including
  absolute-path consumers, partial failures and detection of other active writers.
- [ ] **5.4** Standardize shared maintenance identity/plan/verification fields only
  where duplication is real; retain explicit action-specific gates and manifests.
- [ ] **5.5** Extend Phase 0's CI gates with unpacked-bundle and end-to-end recovery
  checks; reuse fixtures and record the tested source/config identities.

**Done when:** A smaller measured bundle starts offline without the source checkout;
restore exercises verify preserved evidence; CI checks are reproducible.
**Risk/control:** Medium delivery risk; high risk for live recovery operations,
which remain separately approved and are not required for local fixture testing.

## Phase 6: Retire Redundancy

**What it does:** Remove proven duplicate interfaces and documentation after migration.
**Changes:** Approved compatibility hooks/aliases, obsolete branches and duplicate
explanations. Do not remove safety checks or historical readers to meet a size target.

### Tasks

- [ ] **6.1** Recheck Phase 0's caller inventory; obtain explicit retirement decisions
  and publish replacements before removing any supported command or alias.
- [ ] **6.2** Delete replaced implementations only after callers and regression tests
  use the retained owner; preserve inspection versus mutation boundaries.
- [ ] **6.3** Update current architecture, config, runbook and schema references to
  implemented behavior; retain dated acceptance records and repair links.
- [ ] **6.4** Remeasure source/config/test sizes and operating costs with the baseline
  method. Explain reductions by removed duplication, not relocated or compressed code.
- [ ] **6.5** Reconcile the improvement table and component proposals with completed
  work; list remaining decisions and acceptance/activation status explicitly.

**Done when:** No known caller or operational contract is stranded; tests and links
pass; the measured result and remaining backlog are recorded.
**Risk/control:** Medium compatibility risk; retain rollback source and defer
retirement when external usage is unknown.

## Release Gate

Gate each runtime change, not just phase completion. Documentation-only changes
need content/link checks and publication, not GPU jobs.

1. Run focused local checks; review only task changes and publish an isolated exact commit.
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
2. Show the counterexample now rejected, not only a happy-path test result.
3. Compare public outputs, raw schemas/rows and historical readability where affected.
4. Record exact commit/config, focused checks and remaining acceptance/activation gates.
5. Delete the old path only after caller migration; a file move or added wrapper
  alone is not a reduction in complexity.
