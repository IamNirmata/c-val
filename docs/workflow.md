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
    User->>CLI: cval run --live-status
    CLI->>CLI: prioritize stale/never-tested nodes
    CLI->>CLI: render job specs
    User->>CLI: cval run dry-run
    CLI-->>User: submitted=false preview
    User->>CLI: cval run --submit --confirm submit
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
    I --> J[Dry-run plan]
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
    B -- No --> C[Return dry-run records]
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
    B --> C[source 0-env.sh]
    C --> D[run-test.sh]
    D --> E[Storage FIO]
    E --> F[Write result JSON and env state]
    F --> G[NCCL allreduce]
    G --> H[Write result JSON and env state]
    H --> I[DL unit test]
    I --> J[Write result JSON and env state]
    J --> K[db-update.sh]
    K --> L[Write validation.db per-test rows]
    K --> M[Write storage metrics DB]
    K --> N[Write NCCL metrics DB]
```

## Completion Criteria

A one-node c-val 2.0 run is considered successful when all are true:

- Volcano job phase is `Completed`.
- Pod phase is `Succeeded` and exit code is `0`.
- `cval.results.v1` JSON exists.
- `storage`, `nccl`, `dltest`, and `all` rows exist in latest status for the node.
- Storage and NCCL metric DB updates complete.
- DL test log shows completed task output without failure markers.

## Baseline Classification Flow

After results land in the metric DBs, c-val can build a baseline and classify
nodes against it.

```mermaid
flowchart TD
    A[Result DBs: storage / nccl / dltest] --> B[baseline build]
    B --> C[Robust stats: trim, median, MAD, percentiles]
    C --> D[Directional acceptance band per metric]
    D --> E{Store?}
    E -- candidate --> F[baselines table: candidate]
    F --> G[baseline activate]
    G --> H[baselines table: active]
    H --> I[baseline classify]
    A --> I
    I --> J{Node median vs band}
    J -- inside --> K[normal]
    J -- good-side tail --> L[improved]
    J -- failing side --> M[degraded]
```