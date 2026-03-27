# Continuous Validation Framework for GPU Clusters
*Last updated:* Thursday, January 08, 2026

Continuous validation framework for large-scale GPU clusters. This system orchestrates health checks on free nodes in a prioritized manner, ensuring comprehensive coverage and automated result tracking via a Kubernetes-based workflow.

## Structure
- `c-eval/` - Main repository root.
- `job-runner.ipynb` - **Main Orchestrator**: Jupyter notebook that drives the end-to-end workflow (node discovery, queue building, submission, monitoring).
- `utils/`
    - `functions.py` - Core utility functions for Kubernetes interactions, database handling, and job management.
- `kubectl/` - Shell scripts for Kubernetes operations.
- `ymls/` - Kubernetes job templates.
    - `specific-node-job.yml` - Template for single-node validation jobs.
- `validation-tests/` - Test scripts and categories.
- `gitignored/reports/` - Generated daily reports.

## Directory Layout & Data Persistence
All test metadata and logs are stored in a predefined directory structure on a PVC/NFS mount accessible by all nodes (and the orchestrator via `gcr-admin-pvc-access` pod) at `/data/continuous_validation/`.

### 1. Logs Directory
Follows the structure: `/data/continuous_validation/<test-name>/<node-name>/`

- **Storage (sample category)**
  - `Node_001/`
    - `Storage_node001_timestamp1.log`
  - `Node_002/`
    - `Storage_Node002_timestamp1.log`

### 2. Test Results Metadata
**Database File:** `/data/continuous_validation/metadata/validation.db`

The database tracks the latest status per node/per test.

| node | test | timestamp | result |
|-----:|------|----------:|--------|
| 001 | DL unit test | 1736367000 | Pass |
| 002 | DL unit test | 1736367000 | Fail |

## Assumptions
- Random sampling (representative) is important and useful for continuous cluster health monitoring but some nodes may be missed if we only do random sampling.
- The bad nodes are more likely be free than good nodes ( jobs with bad nodes crash more often, freeing up the node).
- We may not afford to do online validation ( pre-flight or post-flight ) for all nodes due to time/resource constraints, too costly.
- Lambda's online validation ( Node Problem Detector for example) is not at application level ( DL unit test, loop back NCCL test, storage test etc).

## Workflow

The orchestration is handled by `job-runner.ipynb` implementing the following logic:

### 1. Get Free Node List
- **Function:** `get_free_node_list()` from `utils/functions.py`.
- **Action:** Queries the cluster manager for currently available nodes.
- **Output:** Saves to list `get_free_node_list[]`.

### 2. Get DB Latest Status
- **Context:** Accesses `validation.db` via the `gcr-admin-pvc-access` pod using `get_db_latest_status()` from `utils/functions.py`.
- **Action:** Retrieves the latest test timestamp for every node in the database.
- **Logic:**
    - If a node has no history, it is marked with a "very old" timestamp (highest priority).
    - Maps: `Node -> Test -> Latest Timestamp`.

### 3. Build Priority Queue
- **Function:** `build_priority_queue(free_nodes_list, db_latest_status, Z_days_threshold)`
- **Logic:**
    1.  **Filter:** Only considers nodes currently in the `free_nodes_list`.
    2.  **Qualify:** Skips nodes where the latest test result is *newer* than `Z` days.
    3.  **Sort:** Orders by timestamp (oldest = highest priority).
- **Output:** `self.job_priority_queue_list[]` structured as:
  ```python
  [
      [node1, 1, True],  # [nodename, priority_order, job_submission_status]
      [node2, 2, False],
      ...
  ]

  ```



### 4. Batch Submission & Monitoring
**Function:** `run_batch(node_name, job_name, template_path, dry_run=False, batch_size=N, monitor_interval=X)`

This function iterates through the `job_priority_queue_list` while there are unsubmitted jobs (False status for job_submission_status), processing them in batches of size `N`.

#### A. Submission Logic
For each node in the current batch, the system triggers `create_job()` from `utils/functions.py`:
1.  **Read Template:** Loads `ymls/specific-node-job.yml`.
2.  **Configure:**
  -   Injects `<node-name>`.
  -   Sets job name: `hari-gcr-ceval-<node-name>-<timestamp>`.
  -   Sets environment variable `GCRTIME`.
3.  **Submit:** Pushes the job to the Kubernetes cluster using `submit_job()` function from `utils/functions.py`.
4.  **Update Queue:** Marks job_submission_status as `True` for the node in

#### B. Monitoring & Lifecycle
Inside the loop, the orchestrator monitors the active batch every `X` minutes:
-   **Status Check:** Uses `get_job_status()` to update the local queue with current states (Pending, Running, Succeeded, Failed).
-   **Timeout Handling:**
  -   **Condition:** If a job remains `Pending` for > `X` minutes.
  -   **Action:** Cancels the job using `delete_job()` and marks the status as `timeout` in the queue.

### 5. Job Execution (Inside the Job Pod)
Once the job is scheduled on the specific node:
1.  **Setup:** Git clones `cluster_doctor` to `/opt/cluster_doctor`.
2.  **Execute:** Runs validation tests, piping output via `tee`.
3.  **Log Archival:** Saves STDOUT/STDERR to: `/data/continuous_validation/<test-name>/<node-name>/<node-name>-<testname>-<timestamp>.log`
4.  **DB Update:** Calls `add_result_local()` (from `/opt/cluster_doctor/utils/functions.py`) to update `validation.db` with the new timestamp and pass/fail status.

### 6. Generate Daily Report
- **Action:** Summarizes the run statistics.
- **Content:**
    - Summary of nodes tested.
    - Summary of pass/fail results.
    - List of nodes never tested.
- **Output:** Saved to `./gitignored/reports/daily_report_<date>.txt`.

## Requirements
- **Python 3.x** with Jupyter Notebook support.
- **Kubernetes Access:** `kubectl` configured with cluster admin rights.
- **PVC Access:** `gcr-admin-pvc-access` pod running for DB file access.
- **Volcano/Scheduler:** For specific node targeting.
- **Database:** SQLite (`validation.db`).