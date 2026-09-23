# C8: Source Delivery

Status: proposed redesign. Baseline/counting: [overview](major-components-overview.md#measurement-method).

## In Simple Terms

Publishing code, making it available to the cluster and starting Jobs with it are
three different actions. Keep proof of the exact code and settings at every step;
a hash only helps when its expected value came from a reviewed source.

## Overview

Make approved validation Jobs run the exact reviewed source with a verified,
self-contained startup contract. Prepare artifacts offline; publication,
ConfigMap creation, acceptance and activation remain separate stages.

## Details and Purpose

Inputs are an exact published commit, namespace, effective config and either
targeted or scheduler-selected placement. Outputs are immutable source bundles
and rendered Volcano Jobs. C4 authorizes creation; this component only prepares
and validates manifests. C6 owns benchmark dependency identity.

## Current Process Flow

```mermaid
flowchart LR
    Commit[Exact published commit] --> Bundle[Git archive and source hash]
    Bundle --> CM[Offline immutable ConfigMap manifest]
    Config[Config and placement] --> Render[Validate and render Job]
    CM --> Render
    Render --> Gate[Separately approved creation]
    Gate --> Bootstrap[Verify source and launch supervisor]
```

Git checkout remains a supported alternate source transport; a branch name is
never an acceptable submitted revision. ConfigMap and Git startup converge on
the same checked runtime contract.

## Codebase Size

| Kind | Files | Physical Lines |
| --- | ---: | ---: |
| Code | 3 | 757 |
| Configuration | 3 | 241 |
| Tests | 1 | 669 |

Counted: [renderer](../../cval/jobs/renderer.py),
[shared bootstrap](../../cval/jobs/bootstrap.sh), [source tool](../../scripts/cval-source.py),
[targeted template](../../ymls/specific-node-job.yml),
[scheduler-selected template](../../ymls/test-job.yml),
[packaging](../../pyproject.toml), and [renderer/source tests](../../tests/test_renderer.py).
The PVC access template is C9; global configuration is C1.

## Functionality Assessment

| Category | Functionality | Decision |
| --- | --- | --- |
| Critical | Exact commit, archive hash/identity, immutable source, semantic template checks | Preserve. |
| Good | Offline source delivery and one shared bootstrap | Already implemented; reuse rather than redesign again. |
| Not important | Inline custom bootstrap compatibility | Review real callers before retirement. |
| Useless | Host-only regression files in every runtime archive, once dependency closure proves non-use | Exclude after proof; do not delete the tests. |

## Current Issues

1. The source archive includes host tests and all scripts; runtime dependency
   closure is implicit, including the dynamically loaded DL lock helper.
2. An exact c-val commit does not pin a container tag or shared DL source tree.
3. Source publication and runtime activation are distinct but easy to confuse.
   A new source object does not update existing Jobs or an already-running loop.
4. Installation assumes a source checkout for descriptors/templates/scripts;
   editable-install behavior is not proof of a self-contained wheel.

## Proposed Design

1. Keep the current source renderer, immutable ConfigMap transport and shared
   bootstrap. Maintain a small explicit runtime file allowlist; validate it in a
   real Job using the prepared bundle, without the host source checkout.
2. Retain repository-local installation as the supported model initially. Do not
   add a wheel-distribution project solely to move files.
3. Bind source SHA, archive hash, manifest hash and effective config digest in one
   reviewed release receipt. Add pinned image/dependency identities through C6.
4. Separate source preparation, create approval, single-node acceptance and loop
   activation. Verify each observed identity; preserve previous artifacts and
   approval records. Never automatically apply a rollback on failed acceptance.

## Identity Chain

| Identity | What It Establishes | What It Does Not Establish |
| --- | --- | --- |
| Published Git SHA | Reviewed source revision in the configured repository | Container/dependency identity or current deployment. |
| Archive SHA-256 and embedded Git identity | Exact prepared bundle bytes and declared revision | Trust if both payload and expected hash came from an unreviewed source. |
| Config digest | The admitted effective test/settings definition | Compatibility of every prior run or permission to mutate. |
| Requested-manifest digest | The reviewed create request | Byte-for-byte equality with Kubernetes' defaulted object. |
| Image/DL artifact identities | Pinned workload inputs | Identical drivers, hardware conditions or timings. |
| Observed Job UID | The concrete created resource | Passing tests or complete raw evidence. |

Validate controlled workload fields as well as labels/annotations on readback.
SHA syntax alone is not publication verification. Controller startup must retain
current-remote-tip enforcement; an explicitly approved targeted validation may
use a known-published exact revision. Inspection and rendering remain nonmutating.

## Placement and Packaging Boundaries

Keep targeted and scheduler-selected templates distinct. A scheduler-selected quick
test cannot prove a prechecked target node or c-val priority selection; it is not
a substitute for targeted release acceptance. Record its actual assigned node.

Runtime bundle trimming must prove both successful startup and the failure paths
for ingestion, locking and recovery helpers used in-pod where safely observable.
Validate real Jobs without the host checkout or undeclared network access; label
unexercised paths. Declare interpreter/dependencies through
the reviewed image; do not quietly add downloads to recover missing bundle files.
Retain both supported source transports until an explicit caller/operations decision.

## Acceptance and Risk

Check archive/manifest identity locally, then require actual startup, placement,
resources and exact-run evidence on an approved fully free Ready node. Each source
transport/placement workflow is a separate real validation scope; do not submit
malformed Jobs or use a local boot simulation as acceptance. Verify active source
separately from object creation and measure bundle size under the
[real-node policy](Redesign.md#real-node-validation-policy).
Priority P2 for size reduction; medium startup/release risk.
