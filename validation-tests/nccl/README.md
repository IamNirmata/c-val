## All Pair AllReduce Validation

This directory contains a self-contained harness for validating large-scale AllReduce performance across every pair of nodes in a cluster. The workflow generates a parallel-safe schedule (no node is reused in the same round), launches NCCL-based PyTorch jobs over MPI, and saves per-pair logs for later inspection.

The primary entry point is `allpair.sh`, which orchestrates the job rounds. The helper Python script `generate_permutations.py` produces the round-robin schedule, and `npairs.py` runs a configurable AllReduce benchmark.

### 1. Prerequisites

- A working MPI runtime (`mpirun` must be on `PATH`).
- NCCL-capable GPUs on each node and PyTorch built with distributed/NCCL support.
- Passwordless SSH between nodes if launching from a head node.
- A hostfile describing the target nodes (one node per line, first column is the hostname or IP). See `test_files/hostfile` for an example.

### 2. Directory Layout

- `allpair.sh` — main orchestrator script.
- `generate_permutations.py` — round generator (complete coverage with no conflicts per round).
- `npairs.py` — simple NCCL AllReduce workload used by default.
- `run_tests.sh` — example harness to demonstrate invoking `allpair.sh`.
- `logs/` — default output directory (created automatically).
- `test_files/hostfile` — example hostfile.

### 3. Quick Start

1. **Clone or copy the directory** to a location accessible from the control node.
2. **Prepare a hostfile** listing all nodes to test. Example format:

     ```text
     node-a slots=8
     node-b slots=8
     node-c slots=8
     ```

3. **Verify Python/NCCL environment** on each node (PyTorch with NCCL, CUDA devices visible).
4. **Run the harness** (adjust paths as needed):

     ```bash
     cd Cluster-Validation-Runbook/allpair
     HOSTFILE=/path/to/hostfile \
     NPERNODE=8 \
     LOGDIR=/tmp/allpairs_logs \
     MASTER_PORT_BASE=45000 \
     bash allpair.sh
     ```

     The script will iterate through every round, launch concurrent MPI jobs for the node pairs in that round, and write logs into `LOGDIR`.

5. **Review logs**: Each MPI job emits a file named `round<r>_job<j>_<nodeA>--<nodeB>.log` containing the AllReduce output and NCCL debug information.

### 4. Configuration

The following environment variables (with defaults) control the run:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOSTFILE` | `allpair/test_files/hostfile` | Hostfile with one node per line. |
| `GEN_SCRIPT` | `allpair/generate_permutations.py` | Scheduler script path. |
| `NPERNODE` | `8` | Number of MPI ranks per node. |
| `NP_TOTAL` | `2 * NPERNODE` | Total MPI ranks per job (auto-derived). |
| `LOGDIR` | `allpair/logs` | Directory for per-pair logs. |
| `MASTER_PORT_BASE` | `45566` | Base port; increments per job in a round. |
| `EXTRA_MPI_ARGS` | _(empty)_ | Additional flags passed to `mpirun`. |
| `APP_CMD_OVERRIDE` | _(empty)_ | Override the workload (see below). |

### 5. Changing the Workload

By default, `allpair.sh` runs `python npairs.py`, which performs repeated NCCL AllReduce operations on a 1 GB tensor. To run a custom benchmark, set `APP_CMD_OVERRIDE` to the desired command:

```bash
APP_CMD_OVERRIDE="python my_script.py --iterations 5" bash allpair.sh
```

Ensure your custom script reads the standard distributed environment variables (`MASTER_ADDR`, `MASTER_PORT`, `NCCL_DEBUG`, etc.).

### 6. End-to-End Test Harness

The helper script `run_tests.sh` shows how to invoke `allpair.sh` with sensible defaults:

```bash
bash run_tests.sh
```

Override any variables inline to target different clusters:

```bash
HOSTFILE=/opt/hostfile NPERNODE=4 LOGDIR=$PWD/logs bash run_tests.sh
```

### 7. Tips and Troubleshooting

- Enable additional NCCL diagnostics with `export NCCL_DEBUG=INFO` (already enabled by default).
- To restrict NCCL to a specific interface, append `EXTRA_MPI_ARGS="-x NCCL_SOCKET_IFNAME=eth0"`.
- If multiple rounds run concurrently across the cluster, ensure the port range (`MASTER_PORT_BASE`) does not collide with other jobs.
- Review `logs/` for failures. `allpair.sh` reports any job failures per round and continues to subsequent rounds.

### 8. Expected Output

At the end of a successful run you should see:

```text
=== Round 0 ===
    Pair: 0(node-a) & 1(node-b)
    ...
Round 0: all jobs completed.
Round 0 logs:
    /tmp/allpairs_logs/round0_job0_node-a--node-b.log

All rounds complete. Logs in: /tmp/allpairs_logs
```

Each log includes lines similar to:

```text
latency: 0.02393 busbw: 85.1
```

Use these metrics to track inter-node bandwidth and identify regressions or outliers.


