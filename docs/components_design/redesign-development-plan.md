# Redesign Development Plan

Status: proposed; no development task below is marked complete.
Based on [Redesign.md](Redesign.md) at `9930c63` (2026-09-23).
Component IDs refer to the [major-component overview](major-components-overview.md).

## Working Rules

- Follow phase order with small, independently testable changes.
- Preserve public commands, canonical results and raw stores unless a migration is
  reviewed. No one-release rewrite, evaluator, generic per-test DB or new service.
- Retain raw history, test-owned persistence and explicit mutation approvals.
- Check off reviewed, published tasks with focused tests, updated docs and evidence links.
- Track **published**, **cluster-accepted** and **active** separately. Task completion
  never authorizes creation, restart, deletion or migration.

## Phase Map

| Phase | What It Delivers | Components | Depends On | Main Risk |
| --- | --- | --- | --- | --- |
| 0. Agree Contracts | Fixed boundaries, callers and acceptance cases | All | Existing design | Unclear compatibility requirements |
| 1. Fix Correctness | Consistent eligibility, recoverable submission, exact-run evidence | C1-C4, C7 | Phase 0 | Unsafe jobs or misleading acceptance |
| 2. Consolidate Control | One lifecycle implementation for manual and continuous runs | C2-C4 | Phase 1 | Lost resume/cooldown state |
| 3. Simplify Runtime | One secure evidence setup and reproducible workloads | C1, C5-C6 | Phase 2 | Evidence integrity or changed measurements |
| 4. Simplify Persistence | Shared writer mechanics with unchanged raw contracts | C6-C7 | Phases 1 and 3 | Partial or divergent writes |
| 5. Harden Delivery | Smaller verified bundles, recovery exercises and CI | C8-C9; cross-cutting tests | Phase 4 | Missing dependencies or incomplete recovery |
| 6. Retire Redundancy | Approved interface removals and compact current documentation | C2; all documentation | Phase 5 and caller migration | Breaking external users |

## Phase 0: Agree Contracts

**What it does:** Fix the implementation contract before changing behavior.
**Changes:** Decisions, fixtures and measurements; no runtime or storage changes.

### Tasks

- [ ] **0.1** Confirm each component's inputs, outputs and mutation boundary; list
  public commands, hidden writer hooks and external callers that must keep working.
- [ ] **0.2** Specify coverage/freshness rules for failed, disabled, missing and
  newly enabled tests; separate observation age from test outcome.
- [ ] **0.3** Define submission intent/receipt and verification-report fields using
  existing models and state files; do not add a raw history database.
- [ ] **0.4** Capture fake node/job responses and temporary seven-store fixtures;
  extend existing suites with missing failure cases.
- [ ] **0.5** Record exact source, component sizes, subprocess/API counts, startup
  time and bundle size where measurable. Label unavailable measurements explicitly.

**Done when:** Contracts and failure checks are documented; fixtures reproduce current behavior.
**Risk/control:** Low; unresolved caller/freshness decisions block affected changes.

## Phase 1: Fix Correctness

**What it does:** Close safety/evidence gaps before refactoring.
**Changes:** Config propagation, eligibility, receipts and verification;
CLI names and raw schemas remain.

### Tasks

- [ ] **1.1** Pass one effective config into discovery and callers; remove ambient
  default reloads within an operation. Test custom resources and tolerations.
- [ ] **1.2** Share a structured eligibility function. Require explicit Ready and
  complete resource evidence; permit cordon only for approved targeted validation.
- [ ] **1.3** Recheck eligibility at every create boundary, including explicit node
  lists. Retain exact source, namespace, batch limits and confirmation checks.
- [ ] **1.4** Persist submission intent and each outcome; return partial batches.
  Reconcile lost create responses by exact identity before any retry.
- [ ] **1.5** Implement the C7 exact-run verifier for canonical results, digests,
  statuses, required metrics/receipts and consistent DL generations; use it in acceptance.
- [ ] **1.6** Make exports join matching runs or expose separate metric timestamps;
  distinguish forbidden, unavailable, empty, stale and conflicting observations.

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
- [ ] **2.2** Route resume and active-job monitoring through it, preserving receipts,
  occupied slots, cooldown repair and pending-job behavior.
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

- [ ] **3.1** Route production and supported standalone callers through shared secure
  reservation; provide an adapter before retiring alternate path construction.
- [ ] **3.2** Derive child environment and compatibility aliases from one immutable
  run definition. Reject disagreements rather than applying hidden overrides.
- [ ] **3.3** Share deadline/process-group handling and explicit test transitions;
  preserve logs and incomplete outcomes on setup failure, timeout or interruption.
- [ ] **3.4** Preinstall FIO in a reviewed image; pin image digest, external DL source
  and copied plan identity. Record them without rewriting historical provenance.
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
- [ ] **4.3** Reuse exact-run readers for verification and exports; inspect query
  plans before proposing additive indexes or other read optimizations.
- [ ] **4.4** Compare temporary stores before/after the change; inject lock contention,
  interruption, divergent receipts and failure between database commits.

**Done when:** Rows, schemas, receipts and complete-generation semantics match the
baseline; partial writes cannot publish final status. Historical readers still work.
**Risk/control:** High; never shadow-write live data with both implementations,
delete partial raw rows, or use a generic transaction abstraction across stores.

## Phase 5: Harden Delivery

**What it does:** Make lean packaging, regression checks and recovery repeatable.
**Changes:** Runtime archive selection, offline CI and combined recovery fixtures;
no automatic backup, migration or deployment.

### Tasks

- [ ] **5.1** Identify runtime dependency closure, including dynamically loaded helpers;
  exclude host-only tests/files from bundles only after proving they are not required.
- [ ] **5.2** Test unpacked offline startup for both placement templates, exact commit,
  archive hash, descriptor availability and required helpers; measure bundle reduction.
- [ ] **5.3** Connect backup, journal migration and restore in temporary fixtures;
  verify rows, receipts, artifacts, metadata and rejection of unsafe preconditions.
- [ ] **5.4** Standardize shared maintenance identity/plan/verification fields only
  where duplication is real; retain explicit action-specific gates and manifests.
- [ ] **5.5** Gate CI on existing unittest suites, shell syntax, descriptors and doc
  links; reuse fixtures and record release evidence. Run focused checks in earlier
  phases even before CI automation lands.

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
  source/config/manifest references. Retain previous artifacts.
3. Obtain exact create/rollback approval; create and read back the immutable source
  object. Creation is not activation.
4. Obtain validation approval; recheck one fully free Ready node, required resources,
   exact source and known workload state immediately before submission.
5. Require terminal Job phase, canonical results, passing required tests, exact-run
   raw evidence and retained logs. A push, Completed Job or old pass is insufficient.
6. Obtain separate controller activation approval, reconcile outstanding Jobs and
   verify fresh execution at the approved source. Keep publication, acceptance and
   active identities separate in the release record.

On failure, retain evidence and stop. Rollback requires approval and compatible
source. No blind resubmit, incompatible writer downgrade, restore over newer raw
writes or second controller against unresolved work.
