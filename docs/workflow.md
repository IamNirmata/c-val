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
    User->>CLI: cval nodes
    CLI->>K8s: kubectl get pods/nodes
    K8s-->>CLI: pod usage + node resources
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
    A[Read latest status] --> B[Discover nodes]
    B --> C{Node schedulable?}
    C -- No --> X[Exclude node]
    C -- Yes --> D{GPU fully free?}
    D -- No --> X
    D -- Yes --> E{History fresh?}
    E -- Yes --> X
    E -- No --> F[Queue candidate]
    F --> G[Sort never-tested and oldest first]
    G --> H[Take batch-size]
    H --> I[Render Volcano job YAML]
    I --> J[Read-only queue plan]
```

## Live Runner Per-Slot Rebuild Policy

The live runner treats the ranked node list as a short-lived snapshot. It does
not build one large queue and walk it blindly.

For every open batch slot, it rebuilds the ranked list from scratch:

1. Rebuilds the free schedulable node list from current Kubernetes state.
2. Re-reads latest validation status from SQLite.
3. Filters out nodes with valid `all` results inside the threshold window.
4. Ranks never-tested nodes first, then nodes with the oldest available results.
5. Submits exactly one job for the top currently valid node.
6. Repeats the same rebuild for the next open slot.

If a submitted job remains `Pending` and does not reach `Running` within the
configured pending-start timeout, the runner deletes that specific validation
job and opens the slot for a fresh live-ranked candidate.

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