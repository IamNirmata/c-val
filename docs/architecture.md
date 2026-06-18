# Architecture

## Purpose

c-val 2.0 separates orchestration from validation execution. The Python package plans, previews, submits, monitors, and summarizes validation jobs. The in-pod validation scripts still run storage, NCCL, and DL checks and write deterministic artifacts.

## Component Diagram

```mermaid
flowchart TB
    Operator["Operator or Hermes"] --> CLI["cval.cli"]
    CLI --> Discovery["cval.k8s.discovery"]
    CLI --> Status["cval.storage.status"]
    CLI --> Workflow["cval.orchestrator.workflow"]
    Workflow --> Priority["cval.scheduler.priority"]
    Workflow --> Renderer["cval.jobs.renderer"]
    CLI --> Manager["cval.jobs.manager"]
    CLI --> Monitor["cval.jobs.monitor"]
    Manager --> Kube["Kubernetes / Volcano"]
    Monitor --> Kube
    Discovery --> Kube
    Status --> PVC["PVC access pod + SQLite"]
    Kube --> ValidationPod["Validation pod"]
    ValidationPod --> Scripts["validation-tests"]
    Scripts --> Artifacts["/data/continuous_validation"]
    Scripts --> ResultJson["cval.results.v1 JSON"]
    Scripts --> DB[(SQLite metadata DB)]
```

## Package Responsibilities

| Module | Responsibility |
| --- | --- |
| `cval.config` | Load typed TOML configuration and expose effective defaults. |
| `cval.cli` | Human/Hermes command surface. |
| `cval.k8s.client` | Thin, testable `kubectl` wrapper. |
| `cval.k8s.discovery` | GPU usage parsing, schedulable-node filtering, free-node discovery. |
| `cval.storage.status` | Read-only latest-status DB access through PVC access pod. |
| `cval.scheduler.priority` | Queue stale or never-tested free nodes. |
| `cval.jobs.renderer` | Render Volcano job YAML with node, timestamp, git repo, and git ref. |
| `cval.orchestrator.workflow` | Build a dry-run plan from discovery + history + template rendering. |
| `cval.policy` | Enforce namespace, batch-size, and confirmation gates. |
| `cval.jobs.manager` | Dry-run by default; explicitly submit approved plans. |
| `cval.jobs.monitor` | Read-only job phase polling and timeout classification. |
| `cval.validation.results` | Parse and validate structured result JSON. |
| `cval.storage.ingest` | Write result and metrics rows from inside validation pods. |
| `cval.baselines.stats` | Robust statistics kernels (median, MAD, percentiles, modified z-score, bootstrap). |
| `cval.baselines.build` | Build dynamic baselines from result DBs per stratum. |
| `cval.baselines.storage` | Persist versioned baselines (candidate/active/superseded). |
| `cval.baselines.classify` | Classify nodes against the active baseline. |

## Runtime Artifacts

```text
/data/continuous_validation/
  storage/<node>/storage-<node>-<timestamp>/
  nccl/<node>/nccl-<node>-<timestamp>/
  dltest/<node>/dltest-<node>-<timestamp>/
  results/<node>/cval-results-<node>-<timestamp>.json
  metadata/validation.db
  metadata/test-storage.db
  metadata/test-nccl.db
  metadata/dltest_numerical_correctness.db
  metadata/dltest_compute_performance.db
  metadata/dltest_collective_performance.db
  metadata/dltest_overlap_performance.db
```

`validation.db` also holds the `baselines` table (versioned dynamic baselines).
The four `dltest_*` DBs hold the tall DL metric rows used for DL baselines and
classification.

## Configuration Boundary

Operator-owned defaults live in `config/cval.toml`. Kubernetes resource shape
stays in `ymls/specific-node-job.yml`, but environment-specific values such as
namespace, queue, PVC, image, and resource requests are injected from TOML when
the job is rendered.

## Design Boundary

The orchestrator answers: what should run, where, and how safely. The validation scripts answer: did storage, NCCL, and DL checks pass on that node. This boundary keeps the agent from replacing deterministic checks with free-form shell guessing.