# c-val HPC Engineer

Use this skill when operating or improving c-val continuous validation for GPU clusters through Hermes Agent.

## When to Use

- Discovering free GPU nodes.
- Reading latest validation status.
- Building a dry-run validation plan.
- Reviewing rendered validation jobs before submission.
- Monitoring Volcano validation jobs.
- Summarizing validation history and result artifacts.
- Improving c-val orchestration, result ingestion, safety policy, or reporting.

## Safety Rules

Start with read-only commands:

```bash
python -m cval.cli status --output table
python -m cval.cli discover-free-nodes --output table
python -m cval.cli plan --live-status --threshold-days 4 --batch-size 3 --output json
python -m cval.cli submit-plan --live-status --threshold-days 4 --batch-size 3 --output json
```

Do not run real submission unless the operator explicitly approves it. Real submission requires:

```bash
python -m cval.cli submit-plan --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
```

Never run destructive cluster commands without explicit approval. This includes `kubectl delete`, `kubectl drain`, `kubectl cordon`, `kubectl taint`, `kubectl patch node`, `kubectl scale`, PVC/log/database deletion, driver restarts, kubelet/containerd restarts, and cluster-wide RBAC changes.

## Setup Checks

Run these from the c-val repo root:

```bash
python -m cval.cli status --output table
python -m cval.cli discover-free-nodes --output table
python -m unittest discover -s tests -p 'test_*.py'
```

If `kubectl` authentication fails, stop and ask the operator to refresh credentials directly on the machine. Do not ask for or print tokens.

## Dry-Run Validation Workflow

1. Read latest DB status:

   ```bash
   python -m cval.cli status --output table
   ```

2. Discover fully free GPU nodes:

   ```bash
   python -m cval.cli discover-free-nodes --output table
   ```

3. Build a dry-run plan:

   ```bash
   python -m cval.cli plan --live-status --threshold-days 4 --batch-size 3 --output json
   ```

4. Preview submit actions without creating Kubernetes resources:

   ```bash
   python -m cval.cli submit-plan --live-status --threshold-days 4 --batch-size 3 --output json
   ```

## Batch Execution Workflow

Use only after explicit operator approval.

1. Reduce blast radius: use `--batch-size 1` for the first run.
2. Keep namespace scoped to `gcr-admin` unless the operator gives another namespace.
3. Submit with the confirmation phrase:

   ```bash
   python -m cval.cli submit-plan --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
   ```

4. Monitor job phases with read-only polling:

   ```bash
   python -m cval.cli monitor-jobs --jobs <job-name> --timeout-seconds 180 --poll-interval-seconds 30 --output json
   ```

## Log Collection Workflow

- Prefer c-val result artifacts under `/data/continuous_validation/results/<node>/`.
- Use `python -m cval.cli result-env --result-json <result.json>` to inspect structured result status.
- Use the test-specific artifact paths from result JSON for logs and summaries.

## Failure Triage Workflow

1. Check job phase with `job-status` or `monitor-jobs`.
2. If the job completed, inspect structured result JSON first.
3. If a test failed, inspect that test's log and summary path from JSON.
4. Do not assume Kubernetes `Ready` means workload health is good.
5. Record whether the failure is storage, NCCL, DL correctness, scheduling, image/bootstrap, or DB ingestion.

## Outlier Classification Workflow

- Start with `status` to identify stale or missing results.
- Use storage/NCCL metric DBs only after result ingestion succeeds.
- Prefer peer comparison and historical baseline logic in c-val modules over ad hoc shell parsing.

## Verification Steps

Before and after code changes, run:

```bash
bash -n validation-tests/0-env.sh validation-tests/run-test.sh validation-tests/db-update.sh validation-tests/dltest/dltest.sh validation-tests/storage/storage.sh
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q cval tests
```

## Known Pitfalls

- `job-runner.ipynb` still contains legacy orchestration logic; prefer `cval.cli` and `cval.orchestrator.workflow`.
- Runtime jobs still clone c-val from GitHub; pinning image/code version remains future work.
- `submit-plan` dry-run does not submit resources. Real submission requires both `--submit` and `--confirm submit`.
- `monitor-jobs` is read-only and does not cancel timed-out jobs.
- Result JSON uses schema `cval.results.v1`; keep `CVAL_RESULT_ENV_FILE` fallback until all in-pod code uses JSON directly.