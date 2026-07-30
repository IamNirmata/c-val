# Original Modular c-val Source Draft

> **Historical source draft:** This file preserves the requirements that seeded
> the approval-gated tracker. It is not the current implementation contract.
> Current decisions and completion evidence are in [cval-update.md](cval-update.md).

The goal is to make c-val easier to manage and modular so updates and changes
are methodical.

## Update 1: Global Configuration and Job Lifecycle Architecture

**Objective:** Standardize configuration management by decoupling Kubernetes
scheduling from test-specific hardware and software requirements.

### Global configuration (`main_config.toml` in the draft)

Centralize shared environmental and hardware constraints. Since all validation
tests run within the same provisioned job, these requirements are static across
the test suite:

- Hardware specifications: CPU, GPU, memory, and RDMA requirements.
- Software specifications: container image references, CUDA/PyTorch versions,
  and dependency parameters.
- Infrastructure details: global parameters, node details, pod configuration,
  and PVC access paths.

## Update 2: Log, Result Directory, and Database Design

**Objective:** Standardize log and result directories and make database design
modular and manageable.

1. **Global log directory:** Save complete stdout and stderr for the job under
   `/data/continuous_validation/logs/job_logs/<node>/<node_timestamp>/`.
2. **Per-test log directory:** Save each validation test's stdout, stderr,
   result files, and related evidence under a path such as
   `/data/continuous_validation/logs/nccl/<node>/<node_timestamp>/`.
3. **Node run history:** Record one row per c-val run rather than one row per
   test, with fields such as node, LA timestamp, numeric timestamp, and tests
   run. The draft proposed `/data/continuous_validation/node_run_history.db`.
4. **Validation result databases:** Give each test a clean result database path,
   such as
   `/data/continuous_validation/validation_tests/nccl/nccl_results.db`, with
   nullable health-class assignment fields.
5. **Validation health-class databases:** Maintain versioned health-class
   baselines per test and environment combination, such as
   `/data/continuous_validation/validation_tests/nccl/nccl_health_classes.db`.

Proposed stable health classes:

- 0 — Excellent / exceeding baseline
- 1 — Nominal / within baseline threshold
- 2 — Underperforming / slight degradation
- 3 — Very Bad / significant degradation
- 4 — Terrible / outlier range
- 5 — DNR / Did Not Run

## Update 3: Automated Health Generation and Test Directory Standardization

**Objective:** Refactor evaluation naming and enforce a modular directory
structure for all validation tests.

### Evaluator pod refactoring

- Draft action: rename the then-existing evaluator pod to `cval-evaluator`.
- Draft role: use that CPU pod for automated health-threshold computation.

### Validation test directory constraints

The original proposal required each test directory to include:

- `README.md` with execution, behavior, and health methodology documentation.
- `test_config.toml` with test settings, paths, minimum records, class targets,
  and environment combination factors.
- Execution runners:
  - `setup.sh` for bootstrapping and dependency checks.
  - `run-test.sh` for the workload and raw evidence.
  - `logger.sh` in the draft for log/result persistence.
- `health_classes.py` in the draft for test-specific baseline construction and
  rebuild triggers.
- `evaluate.sh` or `evaluate.py` in the draft for evaluator-driven assignment of
  health-class fields.

The implemented contract intentionally refined these draft details: framework
code owns logging, candidate construction, persistence, and DNR; plugins expose
validated observations and optional final aggregation only.
