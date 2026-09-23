# C3: Discovery and Scheduling

Status: proposed redesign. Baseline/counting: [overview](major-components-overview.md#measurement-method).

## In Simple Terms

First ask whether a node is safe to use now. Separately ask whether its tests are
due. A recent failed test is still a recent observation, not a healthy result.

## Overview

Answer two separate questions: which nodes could run validation, and which nodes
are due? Observe and order candidates without rendering Jobs or authorizing work.

## Details and Purpose

| Input | Responsibility | Output |
| --- | --- | --- |
| Kubernetes node/pod data | Check Ready, GPU use, CPU/memory/RDMA capacity and scheduling restrictions. | Timestamped eligibility observations with reasons |
| Configured workload requirements | Apply the same resource/taint contract as submission. | Eligible/ineligible/unknown decisions |
| Raw status history and retest age | Order never-tested first, then oldest expired coverage. | Deterministic candidate queue |

Job control owns cooldown, outstanding submissions and available slots. Discovery
does not claim hardware health and the priority queue is not a node ranking.

## Current Process Flow

```mermaid
flowchart LR
    Cluster[Node and pod reads] --> Capacity[Resource and taint calculation]
    Capacity --> Candidates[Candidate nodes]
    History[Raw status timestamps] --> Priority[Pure freshness ordering]
    Candidates --> Priority
    Priority --> Queue[Read-only queue]
```

## Codebase Size

| Kind | Files | Physical Lines |
| --- | ---: | ---: |
| Code | 5 | 806 |
| Configuration | 0 | 0 |
| Tests | 3 | 479 |

Counted: [kubectl client](../../cval/k8s/client.py),
[discovery](../../cval/k8s/discovery.py), [priority](../../cval/scheduler/priority.py),
and the Kubernetes/scheduler package initializers. Tests:
[discovery](../../tests/test_discovery.py), [client](../../tests/test_k8s_client.py),
[priority](../../tests/test_priority.py). Shared models/settings belong to C1.

## Functionality Assessment

| Category | Functionality | Decision |
| --- | --- | --- |
| Critical | Explicit readiness, complete resource checks, bounded reads, deterministic queue | Preserve and make consistent. |
| Good | Targeted node/pod reads, inventory-only listing, diagnostic reasons | Reuse to avoid full-cluster reads for each candidate. |
| Not important | Optional queue shuffling | Retain only for a demonstrated caller; deterministic ordering is the default. |
| Useless | Duplicate node-table and node-JSON interpretations of one observation | Replace with one structured representation. |

## Current Issues

1. [Bulk discovery](../../cval/k8s/discovery.py#L189) reloads defaults and does not
   explicitly test Ready. Targeted validation uses a stricter path.
2. Node capacity tables and JSON are separate reads; their snapshots can differ.
3. GPU/resource quantities and pod requests are calculated in multiple helpers.
   Supported Kubernetes request cases need verified real-cluster evidence, not guesses.
4. [Scheduling history](../../cval/storage/status.py#L40) takes the newest timestamp
   across tests. A partial or disabled-test run can obscure missing coverage.

## Proposed Design

1. Use one pure eligibility function over structured node/pod observations and
   the effective config. Pass an explicit policy for automatic versus targeted
   cordoned-node validation; neither allows active GPU work or unknown readiness.
2. Inventory once per planning pass; check selected nodes again immediately before
   create through the same function. C3 owns coverage-age rules; C4 owns observation
   age at admission, cooldown and create timing. Record node UID to detect replacement.
3. Keep the small pure priority function. Apply the coverage policy below before
   ordering candidates. C7 supplies read-only history/provenance; do not add direct
   SQL or a second result store to the scheduler.
4. Keep snapshot inputs for offline inspection; they can never authorize submission.
   Retain bounded calls and distinguish empty capacity from inaccessible evidence.

## Coverage Policy

Phase 0 decision for implementation; current scheduling remains unchanged until
the relevant producer/reader changes pass real-node acceptance.

| Observation | Scheduling Meaning |
| --- | --- |
| One run fully executed the requested compatible test set with complete expected evidence | Use its timestamp for coverage, even if a fully executed test reported fail. |
| Setup failure, timeout, interruption or missing required test output | Not full coverage; cooldown still prevents immediate retry storms. |
| Newly enabled test or changed execution-relevant test inputs | Previous coverage does not satisfy the new request. |
| Docs-only or host polling change | Does not invalidate coverage when workload compatibility is established. |
| Readable history without proven compatible evidence | Due with an explicit reason; never invent a compatible identity. |
| Forbidden/unavailable history | Pause automatic admission; do not reinterpret it as never tested. |

Use the newest single compatible run that fully exercised every currently required
test and has complete outcome-appropriate evidence. Do not combine unrelated runs.
An extra previously selected test does not invalidate coverage of the requested
subset; a newly required test needs its own coverage in that same run.

### Phase 0 Retest Decisions

| Decision | Rule |
| --- | --- |
| Age threshold | Retest when coverage age is strictly greater than configured `days_threshold`; the checked-in default is 7 days. At the boundary it is still fresh. |
| Full workload reported fail | May refresh coverage, never acceptance, only when summaries prove the intended workload finished; `phase=finished` alone is insufficient. |
| Incomplete latest attempt | Does not replace an older valid coverage timestamp. Retain the new failure/incomplete outcome separately. |
| No compatible complete run | Due with a reason; no history differs from readable but incompatible/incomplete history. |
| Pending/uncertain Job or ingestion | C4 reconciles it before another workload is admitted for that node. A DB problem is not an automatic benchmark retry. |
| Cooldown | Preserve the configured submission cooldown independently of coverage; checked-in default is 14,400 seconds. |
| Unknown history access | Pause automatic admission; a read failure is not permission to treat the node as never tested. |

Compatibility inputs are required test IDs, per-test descriptor digest, workload/
setup/summary code identity, image/dependency identity, and external DL source/plan
identity where applicable. Global polling/cooldown settings and documentation do
not affect workload compatibility. Source SHA always remains exact-run provenance;
it is not a reason by itself to rerun unchanged workload content.

The baseline envelope records Git ref, descriptor digests, image name and framework
versions, but not a complete immutable external-DL identity. Decision: no backfill,
invented digest or new compatibility database. Phase 1 can expose coverage/reasons
from existing evidence; stronger dependency comparisons become active only after
Phase 3 records their reviewed inputs. Until then retain the declared legacy
timestamp policy with its provenance limitation, rather than silently claiming
strict compatibility or causing repeated fleet-wide revalidation.

## Observation Limits

Node and pod reads are not an atomic reservation. Use structured responses,
validate node identity and reject unsupported resource-accounting cases as unknown.
Check deployed-version pod/node observations for init containers, overhead and
quantity forms before claiming scheduler-equivalent accounting. Keep Kubernetes resource
requests and node placement constraints as the final scheduling protection.

Keep automatic and targeted policies distinct: a drained cordon exception is not
a bypass for quarantine taints, resource shortage or unknown workload state.

## Acceptance and Risk

Verify real discovery and approved real-node runs using current commands. Observe
busy/unready/quarantined nodes read-only; never submit to them to prove rejection.
Check coverage and resource calculations against retained real evidence, including
failure or changed-UID cases when available. Mark unsupported/unobserved cases
unknown. Preserve stable queue order and zero renders during planning; measure
calls per real cycle before claiming savings. Follow the
[real-node policy](Redesign.md#real-node-validation-policy); no new fixtures or simulators.
Priority P0; medium risk because a false positive permits unsafe work.
