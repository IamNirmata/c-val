# C1: Configuration and Test Catalog

Status: proposed redesign. Current baseline and counting rules:
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
| Useless | Re-resolving defaults after an effective config was supplied | Remove this conflicting behavior, not configuration validation. |

## Current Issues

1. Downstream discovery reloads defaults instead of using the caller's config;
   duplicate ownership can change eligibility without changing the displayed config.
2. Snapshot, registry JSON and built-in environment aliases overlap. The runner
   can reconstruct the registry separately; fewer independent representations
   would reduce drift.
3. [Adapter loading](../../cval/validation/plugins.py#L184) repeats imports and
   config hooks. Cost should be measured before adding a cache.

## Proposed Design

1. Resolve config once per command and once at continuous-controller startup.
   Freeze it across that session's cycles; changing source, tests or settings
   requires a new reviewed session, not a hidden reload. Pass it explicitly.
2. Keep the current snapshot/descriptor formats initially. Derive compatibility
   environment values from that checked snapshot in one owner. Ingestion retries
   must use the run's original snapshot, not the operator's current config.
3. Separate descriptor validation from optional adapter execution. Reuse checked
   adapter instances only within one operation; never cache across source changes.
4. Keep configuration dataclasses and registry helpers. Do not introduce a DI
   container, remote plugin marketplace, second config format or generic database API.
5. Separate exact run provenance from [coverage compatibility](discovery-scheduling-major-component.md#coverage-policy).
   A source SHA identifies a run; it does not by itself decide whether a docs-only
   change invalidates previous workload coverage. Define the compared test inputs.

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

Custom resources and tolerations must reach every consumer. Snapshot round trips,
descriptor tampering, disabled tests, adapter failures and resource mismatches
must retain fail-closed behavior. Test config-file changes during a session and
retries after a newer config is installed. Reuse [config tests](../../tests/test_config.py)
and [runtime tests](../../tests/test_validation_runtime.py).
Priority P0; medium implementation risk because configuration reaches all components.
