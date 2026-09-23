# C6: Test Workloads

Status: proposed redesign. Baseline/counting: [overview](major-components-overview.md#measurement-method).

## Overview

Own what is tested and what each measurement means. The framework supplies
identity, resources, supervision and evidence paths; each test owns setup,
execution, settings, summaries and optional reviewed persistence/export behavior.

## Details and Purpose

| Family | Current Responsibility | Boundary |
| --- | --- | --- |
| Storage | Six FIO workloads, IOPS and bandwidth summaries | Shared-storage behavior, not an exhaustive storage diagnosis. |
| NCCL | Single-node AllReduce and optional HCA transmit sampling | Not full multi-node fabric certification. |
| DL | Numerical, compute, collective and overlap tasks with rank coverage | Uses external DL source; raw numerical observations are not peer-derived health classes. |

The existing plugin contract supplies configuration validation and read-only raw
export. Built-in persistence is currently implemented in C7. A new test can be
canonical-result-only; any additional writer/destination needs an explicit
test-owned contract, not a generic per-test database.

## Current Process Flow

```mermaid
flowchart LR
    Descriptor[Descriptor and settings] --> Setup[Check or install prerequisites]
    Setup --> Execute[Run fio torchrun or DL workload]
    Execute --> Artifact[Retain raw outputs and rank artifacts]
    Artifact --> Summary[Validate test-owned summary]
    Summary --> Outcome[Return outcome to runner]
```

## Codebase Size

| Kind | Files | Physical Lines |
| --- | ---: | ---: |
| Code | 16 | 1,298 |
| Configuration | 9 | 189 |
| Tests | 1 | 1,743 |

Counted: all Python/shell files in the
[storage](../../validation-tests/storage/run-test.sh),
[NCCL](../../validation-tests/nccl/run-test.sh) and
[DL](../../validation-tests/dltest/run-test.sh) test directories, plus
[NCCL summary parsing](../../cval/validation/nccl_summary.py).
Configuration is three descriptors and six FIO job files.
The [validation script suite](../../tests/test_validation_scripts.py) owns the
counted tests; writer tests are C7. External DL source and installed libraries
are not in this repository and are excluded from these numbers.

## Functionality Assessment

| Category | Functionality | Decision |
| --- | --- | --- |
| Critical | Test-owned settings/results, finite metrics, rank completeness, correct exit status | Preserve and contract-test. |
| Good | HCA observations, retained raw artifacts, dependency preflight | Keep explicit cost/resource budgets. |
| Not important | Four shell command aliases across runtime/workloads | Small compatibility surface; retire only after caller review. |
| Useless | Unused workload variants | None proven; do not invent removals or drop coverage for line count. |

## Current Issues

1. [Storage setup](../../validation-tests/storage/setup.sh) can install FIO after
   GPU allocation, adding network dependence and unmeasured reservation overhead.
2. [DL execution](../../validation-tests/dltest/run-test.sh) imports a shared source
   tree outside the c-val commit. Pinning c-val alone does not pin the benchmark.
3. Built-in-specific settings are projected through framework environment aliases;
   adding a test still requires understanding those compatibility boundaries.
4. Shared storage/network traffic can affect other workloads even when the selected
   node is idle. Serial in-pod tests do not eliminate fleet-wide interference.

## Proposed Design

1. Keep one descriptor plus setup/run/summary code per test; reuse the runner for
   process handling. Avoid a universal benchmark engine or forced rewrite of FIO/PyTorch.
2. Build a reviewed image with dependencies preinstalled and pinned by digest.
   Pin external DL source and copied plan content; record identities in canonical
   provenance. Fail preflight before lengthy work if dependencies differ.
3. Separate setup, execution and ingestion timings so optimization measures real
   GPU reservation cost. Keep bounded controller concurrency as the initial
   shared-resource budget rather than adding a new traffic-scheduling service.
4. Preserve descriptor ordering and required resource checks. Future test-owned
   persistence must reuse authorization and explicit destinations without changing
   existing raw stores or expanding the plugin API silently.

## Acceptance and Risk

Compare summaries, rank coverage and raw metrics on fixed fixtures, then use
separately approved exact-commit GPU acceptance. Exercise missing tools, changed
DL source/plan, partial ranks and failed subprocesses. Measure startup overhead;
do not claim benchmark speedups from packaging alone.
Priority P1; medium risk to reproducibility and benchmark comparability.
