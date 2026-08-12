# Workflow

## High-Level Logical Flow

```mermaid
sequenceDiagram
    participant User as Operator/Hermes
    participant CLI as cval.cli
    participant K8s as Kubernetes/Volcano
    participant DB as SQLite via PVC pod
    participant Pod as Validation pod
    participant PVC as /data/continuous_validation

    User->>CLI: cval status --output table
    CLI->>DB: read latest_status in mode=ro
    DB-->>CLI: latest node/test rows
    User->>CLI: cval nodes --inventory-only
    CLI->>K8s: kubectl get nodes GPU capacity
    K8s-->>CLI: all matching GPU node names
    User->>CLI: cval plan --live-status --git-ref SHA
    CLI->>CLI: prioritize stale/never-tested nodes
    CLI->>CLI: render job specs
    CLI-->>User: read-only queue inspection
    User->>CLI: cval validate --git-ref SHA --submit --confirm submit
    CLI->>K8s: kubectl create -f -
    K8s->>Pod: schedule Volcano job
    Pod->>PVC: write logs, summaries, result JSON
    Pod->>DB: write per-test + aggregate rows
    User->>CLI: cval jobs --watch
    CLI->>K8s: read job phase until terminal/timeout
```

## Planning Flow

```mermaid
flowchart TD
    A[List all matching GPU nodes] --> B[Read latest status]
    B --> C[Remove active cooldown nodes]
    C --> D{History fresh?}
    D -- Yes --> X[Exclude node]
    D -- No --> E[Queue candidate]
    E --> F[Sort never-tested and oldest first]
    F --> G[Check first node only]
    G --> H{Ready, schedulable, resources and all GPUs free?}
    H -- No --> I[Check next prioritized node]
    I --> H
    H -- Yes --> J[Render and submit one Volcano Job]
    J --> K[Rebuild priority queue for next slot]
```

## Live Runner Per-Slot Rebuild Policy

The live runner prioritizes before it performs expensive availability checks.
It does not fetch all pods across the cluster merely to decide which node should
be checked first.

For every open batch slot, it rebuilds the ranked list from scratch:

1. Reads the compact Kubernetes GPU-capacity table and lists every node matching
    the configured GPU-node filter (`hgx` by default). No pod inventory is read.
2. Re-reads latest validation status from SQLite.
3. Reads the local latest-submission cooldown table and excludes nodes whose
    configured cooldown has not expired.
4. Filters out nodes with valid `all` results inside the threshold window.
5. Ranks never-tested nodes first, then nodes with the oldest available results.
6. Checks nodes one at a time in that priority order. Each check reads only the
    named Node and pods assigned to that Node, then verifies Ready state, taints,
    CPU, memory, RDMA, and that every allocatable GPU is free.
7. Stops checking immediately when the first eligible free node is found and
    submits exactly one Job for it. Lower-priority nodes are not queried.
8. Records that node's latest submission timestamp atomically.
9. Rebuilds the entire priority queue before filling the next batch slot.

Availability checks operate in bounded priority pages (`CVAL_PLAN_LIMIT`, 50 by
default). If every node in a page is busy, cval excludes those checked nodes,
rebuilds, and continues with the next priority page. It stops only when it finds
the first free node or exhausts the due queue. Busy nodes are not removed from
validation history; they are reconsidered on the next full per-slot rebuild.

One local advisory lock under `run-logs/cval-live/` prevents two cval-live
processes on the same operator host from racing the same slots. Kubernetes
submission policy and exact node rechecks remain the final mutation gates.

The default cooldown is four hours. Its compact local state is
`run-logs/cval-live/node_cool_down.csv`, with exactly one row per node and the
latest successful cval-live submission timestamp. It is a scheduler guard, not
authoritative test evidence or run history. A malformed table fails planning
closed. Audit mode reads the table and reports exclusions but never updates it.

If a submitted job remains `Pending` and does not reach `Running` within the
configured pending-start timeout (480 seconds by default), the runner may delete that specific
validation job and open the slot only when submit mode and the independent
`CVAL_PRUNE_CONFIRM=delete-pending` gate are both active. Audit mode never
deletes it. Successfully self-pruned jobs are written to a per-cycle
`pruned-jobs.csv` receipt, so a restart does not misclassify the known deletion
as an unexplained missing job. Missing jobs without that exact local receipt
remain indeterminate and block new submissions.

## Saved-job resume goal

Before discovering new nodes, submit mode reads submission receipts from the
latest cycle and observes every unresolved Job by exact name. The goal is
restart safety: after a shell restart, host reboot, or transient Kubernetes API
failure, cval must not forget active Jobs, exceed its batch size, or submit a
duplicate replacement. Pending and Running Jobs continue to occupy slots;
terminal Jobs release slots. An unexplained `Unknown` phase fails closed rather
than guessing that capacity is available.

## Cycle artifact goal

Every pass receives a timestamped directory under `run-logs/cval-live/`. It
retains the raw node snapshot, latest-status snapshot and map, cooldown report,
plan, submission receipts, phase observations, prune receipts, and final status.
These files answer why a node was selected, excluded, submitted, monitored, or
pruned and allow the saved-job resume step to recover after restart. They are a
local operational audit trail; authoritative test results and metrics remain on
the PVC and in the raw metadata databases.

## Execution Flow

```mermaid
flowchart TD
    A[run] --> B{--submit?}
    B -- No --> C[Policy violation; use plan to inspect]
    B -- Yes --> D{--confirm submit?}
    D -- No --> E[Policy violation]
    D -- Yes --> F{Namespace allowed and batch size OK?}
    F -- No --> E
    F -- Yes --> G[kubectl create -f -]
    G --> H[Volcano job]
    H --> I[Validation pod]
```

## In-Pod Validation Flow

```mermaid
flowchart TD
    A[Pod starts] --> B[Checkout CVAL_GIT_REF]
    B --> C[Decode generic runtime context]
    C --> D[Reserve run ID and start global logging]
    D --> E[Generic Python runner]
    E --> F[Load enabled registry in order]
    F --> G[Run test setup with deadline]
    G --> H[Run test workload with remaining deadline]
    H --> I[Stream global and per-test logs/events]
    I --> J[Atomically write cval.results]
    J --> K{More enabled tests?}
    K -- Yes --> G
    K -- No --> L[source built-in aliases then db-update.sh]
    L --> M[Write validation.db per-test rows]
    L --> N[Write storage/NCCL metrics DBs]
```

## Completion Criteria

A one-node c-val 2.0 run is considered successful when all are true:

- Volcano job phase is `Completed`.
- Pod phase is `Succeeded` and exit code is `0`.
- Canonical `cval.results` JSON exists and validates.
- Every enabled test is terminal and global/per-test logs plus events exist.
- `storage`, `nccl`, `dltest`, and `all` rows exist in latest status for the node.
- Storage and NCCL metric DB updates complete.
- DL test log shows completed task output without failure markers.

## Baseline Classification Flow

After results land in the metric DBs, c-val runs two independent background
loops:

1. A daily builder creates/activates dynamic baselines under
    `/data/continuous_validation/baselines`.
2. A periodic classifier evaluates node metrics against the active baselines and
    writes derived decisions to the selected target classification DB.

```mermaid
flowchart TD
    A[Result DBs: storage / dltest] --> B[daily baseline build]
    B --> C[Robust stats: trim, median, MAD, percentiles]
    C --> D[Directional acceptance band per metric]
    D --> F[baseline DBs: candidate]
    F --> G[activate]
    G --> H[baseline DBs: active]
    H --> I[periodic baseline classify]
    A --> I
    I --> J{Node median vs band}
    J -- inside --> K[normal]
    J -- good-side tail --> L[improved]
    J -- failing side --> M[degraded]
    K --> N[per-target classification DBs]
    L --> N
    M --> N
```

NCCL is not part of these generic SQLite baseline loops. Its optional
PostgreSQL path uses calibration decisions, immutable baseline versions, and a
durable evaluation queue described in `docs/evals/nccl-eval-process.md`.