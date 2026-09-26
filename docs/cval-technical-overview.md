# c-val Features and Functionality

This is the implemented capability inventory, reviewed on September 26, 2026.
For the end-to-end diagrams and decision paths, read the
[process flow guide](architecture.md). For exact operating steps and approvals,
use the [operations runbook](operations-runbook.md).

Defaults below come from the checked-in TOMLs. A particular run uses its retained
configuration snapshot; this document is not a live cluster-status report.

## 1. Overview and Goals

c-val runs repeatable tests on available GPU nodes and preserves their outcomes,
measurements and diagnostic files. It complements Kubernetes readiness checks,
which cannot establish whether the selected numerical, communication or storage
operations work as expected.

The repository owns discovery, validation, raw ingestion and verification that the
Job and its evidence completed successfully. Its scope stops there: no downstream
baseline, classification, scoring, repair or remediation controller is included.

### Capability Map

| Area | What c-val provides |
| --- | --- |
| Configuration | Strict main/test TOMLs, validated resource requirements and frozen run inputs |
| Discovery | GPU inventory, current node eligibility and resource accounting |
| Scheduling | Successful-coverage priority, cooldown and shared concurrency limits |
| Submission | Policy checks, pinned source, durable requests and verified creation receipts |
| Tests | Storage FIO, NCCL/InfiniBand and deep-learning unit tests |
| Runtime | Ordered setup/execution, deadlines, child cleanup and structured results |
| Persistence | Authorized test-owned writes into seven raw SQLite stores |
| Verification | Exact-run comparison of Kubernetes identity, canonical artifacts and raw records |
| Reporting | CLI views, paired node/cluster status CSVs and local raw-result exports |
| Operations | Continuous/one-cycle control plus separately gated source and data maintenance |
| Extension | Repository-local test descriptors and optional configuration/export plugins |

## 2. Validation Boundary

Kubernetes Ready is a scheduling prerequisite, not proof of test success or node
health. Tests exercise selected operations and retain their outcomes and metrics;
an old pass or incomplete result must never substitute for the current run.
Validation consumes reserved capacity and can still affect shared storage/network
services. It is not comprehensive hardware certification or a workload diagnosis.

## 3. Architecture

### Configuration and Identity

| Feature | What it means |
| --- | --- |
| Two-level configuration | Main TOML owns framework settings; each test TOML owns its benchmark settings |
| Strict validation | Missing/unknown values, unsafe paths, duplicate IDs/orders and insufficient shared resources are rejected |
| Effective-config inspection | Print the resolved settings and inspect each registered test without running a GPU workload |
| Frozen transport | A deterministic snapshot carries the reviewed inputs into sessions and pods; packaged descriptors must still match |
| Explicit source identity | Submissions require a nonzero, lowercase 40-hex commit, not a moving branch name |
| Two source transports | Load a checksum-verified immutable ConfigMap archive or fetch/check out the exact Git commit |
| Shared bootstrap | Both placement templates use the same embedded, validated startup payload |
| Experiment identity | Results retain node, run ID, timestamp, source, image label, reported PyTorch/CUDA versions and config/test checksums |

The image, resources, test settings and external DL source are selectable inputs,
not a permanently fixed fleet environment. A source commit does not pin an image
tag or guarantee that external dependencies are unchanged.

Sources: [configuration](../cval/config.py), [registry](../cval/validation/registry.py),
[rendering](../cval/jobs/renderer.py), [bootstrap](../cval/jobs/bootstrap.sh).

### Discovery and Scheduling

| Feature | What it means |
| --- | --- |
| Inventory-only discovery | List matching GPU node names without querying pods; this is not an availability claim |
| Full or targeted eligibility | Inspect the fleet or one named node using structured Kubernetes evidence |
| Readiness and taints | Require explicit readiness and acceptable placement; ordinary loop work excludes cordoned nodes |
| Multi-resource accounting | Check GPU, CPU, memory and RDMA requests, including init containers, sidecar-style init containers and pod overhead |
| Fully free GPU policy | Require zero active pod GPU requests, not merely low utilization |
| Successful-run coverage | Failed recent attempts do not refresh coverage; bounded history can retain an older qualifying pass |
| Deterministic priority | Missing successful coverage first, then expired coverage oldest first, with node-name tie breaking |
| Independent cooldown | Limit repeat submissions to the same node even when its coverage is still missing or old |
| Slot refill | Rebuild the candidate list for open slots; one cycle can submit more than the concurrent Job limit over time |
| Read-only planning | Inspect candidates without rendering or submitting Jobs; the plan limit does not change submission concurrency |

Source: [eligibility](../cval/k8s/discovery.py), [priority](../cval/scheduler/priority.py),
[coverage](../cval/storage/status.py), [loop](../scripts/cval-live.sh).

### Admission and Recovery

| Feature | What it protects |
| --- | --- |
| Shared submission gate | Batch submission, targeted validation and the loop use the same admission owner |
| Scope and policy checks | Bind the configured control host, cluster, namespace, allowed batch size and exact confirmation |
| Pinned session | Retain the reviewed config and approval/rollback record; incompatible or legacy sessions need reviewed turnover |
| Durable intent | Save the request and its hashes before the external create call |
| Immediate node recheck | A previously free node must still be eligible at submission time |
| Shared reservations | Count live Jobs and unresolved requests against node/slot limits |
| Receipt verification | Check the returned Job UID and exact requested workload; reject conflicting identity |
| Read-only reconciliation | Recover an existing outcome without blindly recreating a Job after a lost reply |
| Single-attempt workload | Enforce the configured no-restart/terminate-on-failure Job contract |

Source: [admission and session ownership](../cval/jobs/manager.py),
[execution policy](../cval/policy.py).

## 4. Process Logic

### Supported CLI Commands

These are capabilities, not preapproved commands to run against a cluster.

| Command | Function | Effect |
| --- | --- | --- |
| `cval config` | Print effective configuration | Local inspection, JSON |
| `cval tests list` | List tests; optionally enabled tests only | Local inspection |
| `cval tests describe <id>` | Show one effective descriptor | Local inspection |
| `cval tests validate` | Check descriptors, plugins and shared resource requirements | Local validation; no benchmark |
| `cval nodes` | Show schedulable GPU capacity | Kubernetes reads |
| `cval nodes --inventory-only` | List matching GPU node names only | Kubernetes reads |
| `cval nodes --check-node <node>` | Explain one node's eligibility | Kubernetes reads |
| `cval status` | Show latest per-test outcome rows | Read-only PVC/raw access |
| `cval status --coverage` | Show successful coverage and provenance limitations | Read-only PVC/evidence access |
| `cval plan` | Show ordered candidates, optionally from supplied nodes/history | Read-only inspection |
| `cval run` | Submit the configured batch | Explicitly approved Kubernetes creation |
| `cval validate` | Submit, observe and verify one named node | Approved creation, followed by reads and normal workload writes |
| `cval jobs` / `cval jobs --watch` | Read phases once or wait to a bounded terminal/timeout result | Kubernetes reads; not full acceptance |
| `cval results` | Export latest or exact-run results | Read-only source access; writes a local CSV |
| `cval db-rebuild-dltest-metrics` | Reconcile DL stores from retained rank JSON | Raw DB mutation; separate maintenance approval |

Most inspection commands offer table or JSON output. Configuration output is
JSON; result exports are CSV. Exact export selectors use `--node` and `--timestamp`
together. `--no-metrics` suppresses metric joins. `all` and `overall` select the
same aggregate outcome, while test exports come from enabled export adapters.

`run` and `validate` require `--submit --confirm submit` and an exact source
commit. They have no dry-run mode; use `plan` to inspect candidates. `plan
--batch-size` only limits the displayed candidate page. Without a supplied
history or `--live-status`, standalone planning treats its input nodes as never tested.

Source: [CLI parser and handlers](../cval/cli.py).

### Supported Workflows

| Workflow | Purpose and boundary |
| --- | --- |
| Continuous loop | Run repeated discovery/admission/tracking cycles through tmux; one loop owner and a passive status reader |
| One cycle | `run-once` runs a cycle in the foreground; it can refill slots, so it is not necessarily one Job |
| Targeted validation | Use one named fully free Ready node; permits a drained cordoned node without uncordoning it |
| Scheduler-selected quick workflow | Use the existing unpinned-node template so Volcano chooses placement; same full tests, not a shorter benchmark |
| Existing-Job observation | Watch a known Job without resubmitting; use separate exact evidence verification for acceptance |

The live script exposes `start`, `stop`, `attach`, `status` and `run-once`;
`run-loop` is its internal continuous entry point. Start verifies the published
tip and pins the session. Stop does not delete submitted Jobs. Pending-Job pruning
has a separate confirmation gate and is not enabled by an ordinary timeout.

Prerequisites include Python 3.11+, the configured Python dependencies, Bash,
authenticated `kubectl`, Volcano, suitable GPU/RDMA capacity and the shared PVC.
Continuous host operation also uses Git, tmux and file locks. PVC reporting needs
the configured access pod and appropriate read/exec permissions. The workload
image and test dependencies must be available independently of source delivery.

## 5. Test Coverage and Runtime Configuration

### Enabled Test Families

The profiles below are checked-in defaults. Each test name links to its descriptor;
individual runs retain their effective configuration.

| Test family | Profile and measurements | Coverage boundary |
| --- | --- | --- |
| [Storage FIO](../validation-tests/storage/test_config.toml) | Order 10; 30-minute timeout. Shared-storage read/write profiles; 12 raw bandwidth/IOPS fields across depth, parallel-file and random I/O patterns | Not application checkpoint validation or a latency acceptance policy; shared-service contention influences results |
| [NCCL and HCA bandwidth](../validation-tests/nccl/test_config.toml) | Order 20; 20-minute timeout. Eight GPUs, 20 iterations, `data_size_gb=8`, IB transport, P2P/shared-memory paths disabled, IB bandwidth tracker enabled. Algorithm/bus bandwidth, latency and per-port transmit measurements | Single-node IB loopback; does not qualify the entire fabric, topology or cross-node all-to-all behavior |
| [Deep-learning unit test](../validation-tests/dltest/test_config.toml) | Order 30; 40-minute timeout. Eight ranks, `80gb-example`, 100 iterations. Numerical norms/parameters, CPU/GPU forward/backward timings, collective timings and compute/communication overlap | Selected primitives, not a complete training workload or certified numerical reference; exact task coverage depends on the external benchmark plan |

Storage runs six profiles: random write/read, single-file queued write/read and
multi-file parallel write/read. It retains FIO JSON and a readable summary while
removing the run-local scratch data. Raw bandwidth/IOPS are not automatically
compared with a fleet performance baseline.

NCCL samples configured or auto-detected InfiniBand ports while AllReduce runs.
It validates its summary and, when enabled, requires HCA samples. The number of
observed ports is not inferred from the GPU count or RDMA resource allocation.

DL makes a run-local copy of the source and selected plan, checks that copy,
runs `torchrun`, and validates the rank summaries. Numerical, compute, collective
and overlap measurements remain attributable to the plan and rank artifacts.

The suite is not comprehensive CPU/RAM/HBM diagnostics, all-pairs NVLink
qualification or a complete training workload.

Phase timeouts total **90 minutes**; the configured monitoring timeout is **100
minutes**. These are budgets, not measured durations. The scheduler-selected
[quick workflow](operations-runbook.md#quick-test-workflow) uses the same full
profile; it changes placement, not test coverage or guaranteed completion time.

### Runtime Safeguards

| Feature | Behavior |
| --- | --- |
| Registry order | Enabled tests run sequentially in unique descriptor order |
| Shared test deadline | Setup and workload consume the same per-test time budget |
| Continue after ordinary failure | Preserve one test's failure and attempt the later tests; the current descriptor schema requires this behavior |
| Explicit disabled state | Skipped tests are incomplete/not selected, never fabricated passes |
| Secure run reservation | Reject reused run paths, symlinks and changed evidence-directory identities |
| Child-process cleanup | Terminate process groups on timeout/interruption and clean up lingering descendants |
| Structured progress | Emit run-bound `cval.event.v1` events and `CVAL_EVENT` console records |
| Canonical results | Update whole-run and per-test results with status, phase, timings, exit codes, paths and checksums |
| Supervised ingestion | Run the Python writer coordinator after valid finalization; require its matching acknowledgement |
| Bounded capture | Preserve stdout/stderr and global/per-test logs without treating an arbitrary final log line as proof of success |

Source: [runtime owner](../cval/validation/supervisor.py),
[runner](../cval/validation/runner.py), [execution helpers](../cval/validation/execution.py).

### Scheduling and Resources

| Setting | Default | Meaning |
| --- | --- | --- |
| Retest age | 7 days | Eligibility threshold, not a weekly coverage guarantee |
| Batch size / policy maximum | 3 / 5 jobs | Configured concurrency bounded by the policy ceiling |
| Submission cooldown | 4 hours per node | Limits repeat submissions; separate from result freshness |
| Job resources | 8 GPUs, 100 CPU, 1,500 GiB memory, 256 GiB shared-memory volume, one RDMA allocation | Full-node-scale reservation; RDMA resource count is not a physical-link count |
| Image | `nvcr.io/nvidia/pytorch:26.05-py3` | Configured tag, not an immutable image digest |
| Phase watcher | 100-minute budget; 60-second polling | Read-only observation, not automatic Job cancellation |
| Targeted validation | 3-second polling; warning after 10 minutes Pending | Shares the overall observation budget; leaves queued work intact |
| Live pending threshold | 20 minutes | Relevant to separately enabled pending-Job pruning, not unconditional deletion |
| Whole runner / ingestion | 100 minutes / 5 minutes | Separate supervisor child deadlines |
| Status refresh / between-cycle delay | 5 minutes / 5 minutes | Different activities; neither is a coverage guarantee |

Opportunistic selection limits direct competition for busy GPUs but can leave
those nodes with stale evidence. Diagnostic traffic can still affect shared
storage and fabric. See the [configured limits](../config/cval.toml).

## 6. Raw Evidence

### Persistence Boundaries

| Layer | Contents and contract |
| --- | --- |
| Authoritative raw stores | Seven SQLite files under `metadata/`: validation status, storage, NCCL, and four DL stores for numerical, compute, collective and overlap observations |
| Canonical artifacts | `cval.results`, `cval.event.v1` progress, logs, summaries, run identity and configuration identity; historical result formats remain readable |
| Control-host state | Session approval, frozen config, durable intents/receipts, cooldown and cycle history |
| Public reporting | Two current CSVs; detailed operational files stay in the private state root |

Strict audit queries require SQLite `mode=ro` and `query_only=ON`. Bounded reads,
retry policy, exact identities, POSIX locks and DELETE journaling protect raw
ingestion on shared storage. See the [raw database contract](components_design/raw-evidence-major-component.md#existing-store-contract)
and [result schema](result-schema.md). No derived store or evaluation cursor is
part of the current framework.

### Ingestion and Verification Features

| Feature | Behavior |
| --- | --- |
| Test-owned persistence | Storage, NCCL and DL write their established stores; there is no automatic generic per-test DB |
| Authorized destinations | Every writer checks immutable run/config identity and approved evidence/DB paths |
| Metrics before status | Required passing-test metrics precede the atomic built-in outcome set |
| Failure-evidence preservation | Failed or disabled tests need no invented metrics; their outcomes remain visible |
| Bounded SQLite retries | Retry an authorized writer on lock contention, not an entire benchmark |
| Serialized DL publication | Persistent lock, four-store generation and per-run receipts expose partial publication |
| Per-store acknowledgements | Distinguish committed, not required and unconfirmed stores |
| Exact acceptance | Compare Job/pod identity, canonical files, event order, result digest and raw values for the exact run |
| Stable DL audit | Require expected ranks, plan, iterations, receipts and a generation unchanged across the read |

No single transaction covers all seven stores. Successful failure-evidence
ingestion is not a test pass. `Completed`, `fully_recorded` and `accepted` are
not interchangeable; the [flow guide](architecture.md#8-accept-only-matching-exact-run-evidence)
shows the distinction.

### Reporting and Export Features

| Output | Contents and limits |
| --- | --- |
| Node-status CSV | One row per observed node with UID, coverage, priority, attempt, recording/outcome, cooldown, resource state, source/config and observation time |
| Cluster-status CSV | Matching totals, coverage/readiness counts, recent fully recorded count and observation completeness |
| Paired generation | Both CSVs carry one `snapshot_id`; mismatched or missing pairs are rejected |
| Explicit stale/unknown state | Keep historical timestamps when possible while identifying stale, unverified, unavailable or forbidden observations |
| Overall result export | Aggregate outcome plus optional exact-timestamp storage/NCCL metrics |
| Storage export | Per-node outcome and the 12 FIO bandwidth/IOPS fields |
| NCCL export | Per-node outcome, run/software context and consolidated NCCL/HCA measurements |
| DL export | Per-node DL outcome summary; detailed rank metrics remain in the four raw DL stores and artifacts |
| Exact-run selectors | Node plus timestamp selects that run; absent exact status fails rather than falling back |
| Human-readable times | Preserve epoch identity and provide UTC/Los Angeles times where defined by the output schema |

The two live filenames are `node-status.csv` and `cluster-status.csv`. Exported
result snapshots are separate local outputs, not extra live-status files. Export
to a separate directory. Missing same-timestamp metrics remain blank; another
run's measurements are never substituted.

The recent fully recorded count can include failed tests, and normal
`fully_recorded` projection is not a fresh seven-store audit. Status CSVs are
informational; they cannot authorize submission.

Sources: [status projection](../cval/storage/status.py),
[result export](../cval/storage/results_export.py), [plugin dispatch](../cval/validation/plugins.py).

## 7. Operational Measures and Limitations

| Measure | Definition |
| --- | --- |
| Fresh coverage | Nodes with qualifying successful coverage inside the configured age window; separate busy and untestable nodes |
| Evidence completeness | Terminal runs with canonical results, successful ingestion and matching exact-timestamp raw rows / terminal runs |
| Capacity cost | Validation GPU-hours, startup and queue delays, shared-storage/network traffic |

Keep forbidden, unavailable, empty, stale and incomplete evidence distinct.
Busy nodes can remain untested; single-node short-duration tests do not qualify
every multi-node workload. See [progress](cval-progress.md) for dated verification
and the [improvement plan](cval-improvement-plan.md) for raw-framework proposals.

Coverage has an explicit legacy age-only path and does not establish full
cross-environment compatibility. Discovery is based on Kubernetes requests, not
a promise that no other scheduler can place work after an observation. Source
publication, cluster acceptance and live activation are separate milestones.

The framework does not provide a performance-baseline engine, node health
classification, node scoring, automatic repair, automatic source promotion,
whole-fleet certification or a web dashboard. Historical files and proposals
are not active features.

## 8. Maintenance and Source Delivery

These tools are separate from routine test execution. Inspection is not approval
to apply a plan, and a previously approved test does not authorize maintenance.

| Tool / capability | Inspection or preparation | Mutating operation and boundary |
| --- | --- | --- |
| [Source packaging](../scripts/cval-source.py) | Render one exact-commit immutable source ConfigMap manifest locally | Creating it and activating a session are separate approved operations; old objects are preserved |
| [Whole-root backup](../scripts/cval-backup.py) | Inventory source, check destination capacity and verify an existing manifest-backed backup | Apply requires explicit backup and writers-stopped confirmations, sufficient capacity and coherent source evidence |
| [PVC layout](../scripts/cval-pvc-layout.py) | Bounded inventory, receipt references, archive/consolidation plans | Apply/rollback require exact plan or transaction identity and idle-source confirmation; consolidation checks backup evidence |
| [SQLite journal maintenance](../scripts/cval-sqlite-journal.py) | Inspect four DL journal modes and probe backup-read suitability | Approved migration to DELETE journaling uses backup and quiescence gates; it is not an automatic runtime migration |
| [DL metric rebuild](../cval/storage/dltest_ingest.py) | The CLI exposes the command and its inputs; executing it is a write | Reconcile retained rank evidence into authorized DL stores; `--only-missing` skips complete runs and avoids full reconciliation |
| [Cooldown helper](../scripts/cval-node-cooldown.py) | Filter candidates and explain cooldown exclusions | Record submissions or migrate the local cooldown format; manual state changes need their own approval |
| [Session helper](../scripts/cval-live-state.py) | Read/observe the selected session and project status | Initialize/update/import approved session records; not a shortcut around unresolved intents |

Full DL rebuild is not read-only export or a harmless preview. Without
`--only-missing`, it can remove stored runs under the selected root when their
input artifacts are absent. Review the exact scope and preserve the evidence
before using that maintenance mode.
Backups, raw DBs, canonical files and historical artifacts are not automatically
removed by normal validation. The framework has no generic one-command restore
or automatic rollback to an older running source.

## 9. Extension and Compatibility

| Capability | Contract |
| --- | --- |
| Add a test | Register a repository-local descriptor with a unique ID/order, setup, entrypoint, requirements, settings and summary name |
| Validate the registration | Reject reserved IDs such as `all`/`overall`, duplicate paths, invalid quantities and incompatible resources |
| Optional plugins | `cval.plugin.v1` supports configuration validation and raw-result export; it is not a generic DB-writer interface |
| Result-only tests | Use the common runner and canonical result envelope without inventing a new raw database |
| Per-test persistence | Add intentional test-owned integration when new metrics require a store; the framework does not generate it automatically |
| Historical results | New runs emit `cval.results`; older `cval.results.v1` and `.v2` remain readable within their documented limits |
| Compatibility launchers | Environment, runner and DB shell entry points remain available; production supervision directly starts Python runner and ingestion children |
| Importable APIs | Discovery, queue construction, rendering, monitoring and verification remain separate Python owners behind the CLI |

Extension is source work: review, publish and validate the changed implementation
before live use. Editing a descriptor does not retune an already pinned session.

## References

- [Process flow and diagrams](architecture.md)
- [Configuration fields and sources](configuration.md)
- [Canonical results and raw schemas](result-schema.md)
- [Operational procedures and approvals](operations-runbook.md)
- [Operator-interface reference](components_design/operator-interface-major-component.md)
- [Ingestion implementation](../cval/storage/run_ingest.py)
- [Targeted acceptance implementation](../cval/orchestrator/validate.py)

The source baseline for this inventory is `0760c78`. These references provide
field-level detail without changing the scope described here.