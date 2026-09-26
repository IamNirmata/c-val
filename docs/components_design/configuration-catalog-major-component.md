# C1: Configuration and Test Catalog

Status: strict config, shared projections and the 2026-09-26 C1 simplification are
source-implemented; new-source cluster acceptance and activation are separate gates.
Historical baseline and counting rules:
[component overview](major-components-overview.md#measurement-method).

## In Simple Terms

Choose the settings once and give every component the same values. Editing a
configuration file later must not silently change a running Job or controller.

## Overview

Turn reviewed configuration and repository-local test descriptors into one
immutable runtime definition. Own settings and test identity, not cluster reads,
job creation, benchmark execution or database writes.

## Details and Purpose

| Input | Responsibility | Output |
| --- | --- | --- |
| TOML, explicit config selection | Resolve settings once; reject invalid values. | Effective configuration |
| Registered test descriptors | Validate IDs, order, paths, settings and resource coverage. | Ordered enabled-test catalog |
| Config/export adapters | Validate declared capabilities and confined support files. | Checked extension contracts |
| Exact source and descriptor contents | Bind reproducible configuration identity. | Snapshot, digests and child environment |

Keep test-specific settings with tests. Environment variables are transport to
child processes, not a second source of truth after a run is admitted.

## Current Process Flow

```mermaid
flowchart LR
    Input[TOML or snapshot] --> Config[Load configuration]
    Config --> Registry[Load and order descriptors]
    Registry --> Check[Check resources and adapters]
    Check --> Snapshot[Encode snapshot and digests]
    Snapshot --> Environment[Build child environment]
```

## Codebase Size

| Kind | Files | Physical Lines |
| --- | ---: | ---: |
| Code | 8 | 2,227 |
| Configuration | 1 | 89 |
| Tests | 5 | 1,074 |

Counted code: [package initialization](../../cval/__init__.py),
[configuration](../../cval/config.py), [models](../../cval/models.py),
[built-in compatibility](../../cval/validation/builtins.py),
[registry](../../cval/validation/registry.py), [plugins](../../cval/validation/plugins.py),
[target catalog](../../cval/validation/operational_targets.py), and
[runtime environment](../../cval/validation/runtime.py).
Configuration: [global TOML](../../config/cval.toml).
Tests: config, operational targets/extensions, runtime environment and raw-only scope.
Workload descriptors are counted under C6, not twice here.

## Functionality Assessment

| Category | Functionality | Decision |
| --- | --- | --- |
| Critical | Immutable snapshots, descriptor confinement, resource coverage, deterministic order | Preserve. |
| Good | Descriptor-only inspection; optional config/raw-export adapters | Keep without eager imports for inspection. |
| Not important | Multiple legacy environment spellings | Keep at one compatibility boundary until callers migrate. |
| Removed | Re-resolving defaults after an effective config was supplied | Strict snapshots and explicit config propagation replaced this behavior. |

## Design Disposition

Implemented: one explicit effective config per command/session, complete strict
snapshots and shared environment projections. Discovery receives that config;
ingestion uses the run's original snapshot. The registry now owns duplicate/reserved
ID checks and enabled export selection. Configuration and CLI parsing no longer
build a second export catalog, and adapter dispatch lives with the plugin owner.
Existing target objects, catalog methods and operation imports remain compatible;
their import paths do not contain a second implementation. The
[strict configuration contract](../configuration.md#strict-two-level-contract)
and [runtime projection](validation-runtime-major-component.md#phase-32-environment-projection)
own the details; no second config/context layer was added.

Redundant fixed-setting CLI flags are retired as listed in
[configuration migration](../configuration.md#retired-setting-flags). Workload
settings, raw export names/columns, descriptor inspection and snapshot identity
are unchanged. This does not retire the C2 ingestion commands or C3-C9 features.

Retained: configuration dataclasses, registry helpers, optional adapters and
compatibility aliases. Adapter caching has no measured justification and is
deferred. Full dependency-based freshness comparisons are also deferred under the
[coverage policy](discovery-scheduling-major-component.md#coverage-policy); exact
run provenance is not proof of compatibility between different experiments.

## Boundaries and Failure Cases

| Boundary | Required Behavior |
| --- | --- |
| Invalid config or missing descriptor | Fail before admission or evidence creation; no fallback to defaults. |
| Environment disagrees with snapshot | Reject, except explicitly named transport/path mappings validated by the runtime. |
| Inspection without cluster access | Parse local config/descriptors without requiring remote source publication. |
| Adapter import | Treat repository Python as trusted executable code, not a sandboxed extension. |
| Secret inputs | Keep credentials out of snapshots, digests, reports and child environments unless explicitly required. |

Confining an adapter's path does not restrict what its Python code can execute.
No new plugin capabilities or on-disk identity fields are implied without a
reviewed version/compatibility decision.

## Acceptance and Risk

In approved real runs, compare the submitted snapshot with observed resources,
test selection and retained config identity. Inspect invalid configurations without
submission; controlled change/failure exercises need separate approval. Keep the
session/retry snapshot rules and record cases not observed. Follow the
[real-node policy](Redesign.md#real-node-validation-policy); do not add mock config consumers.
Priority P0; medium implementation risk because configuration reaches all components.
