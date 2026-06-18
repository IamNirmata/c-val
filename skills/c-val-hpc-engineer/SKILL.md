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
python -m cval.cli nodes --output table
python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
```

Do not run real submission unless the operator explicitly approves it. Real submission requires:

```bash
python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
```

Never run destructive cluster commands without explicit approval. This includes `kubectl delete`, `kubectl drain`, `kubectl cordon`, `kubectl taint`, `kubectl patch node`, `kubectl scale`, PVC/log/database deletion, driver restarts, kubelet/containerd restarts, and cluster-wide RBAC changes.

## Setup Checks

Run these from the c-val repo root:

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
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
   python -m cval.cli nodes --output table
   ```

3. Build a dry-run plan:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
   ```

4. Preview submit actions without creating Kubernetes resources:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
   ```

## Batch Execution Workflow

Use only after explicit operator approval.

1. Reduce blast radius: use `--batch-size 1` for the first run.
2. Keep namespace scoped to `gcr-admin` unless the operator gives another namespace.
3. Submit with the confirmation phrase:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
   ```

4. Monitor job phases with read-only polling:

   ```bash
   python -m cval.cli jobs --jobs <job-name> --watch --timeout-seconds 180 --poll-interval-seconds 30 --output json
   ```

## Log Collection Workflow

- Prefer c-val result artifacts under `/data/continuous_validation/results/<node>/`.
- Use `python -m cval.cli result --result-json <result.json>` to inspect structured result status.
- Use the test-specific artifact paths from result JSON for logs and summaries.

## Failure Triage Workflow

1. Check job phase with `jobs` or `jobs --watch`.
2. If the job completed, inspect structured result JSON first.
3. If a test failed, inspect that test's log and summary path from JSON.
4. Do not assume Kubernetes `Ready` means workload health is good.
5. Record whether the failure is storage, NCCL, DL correctness, scheduling, image/bootstrap, or DB ingestion.

## Baseline and Outlier Classification Workflow

- Start with `status` to identify stale or missing results.
- Build a baseline from recent results (dry-run prints the robust metrics;
  `--store` saves a candidate, `--activate` promotes it):

  ```bash
  python -m cval.cli baseline build --test-type nccl --window-days 30 --output json
  python -m cval.cli baseline build --test-type storage --image-name <image> --store
  ```

- Classify nodes against the active baseline and act on `degraded` nodes:

  ```bash
  python -m cval.cli baseline classify --test-type nccl --output json
  python -m cval.cli baseline classify --test-type storage --node <node> --output json
  ```

- Promote a new baseline to active only with operator awareness; it redefines
  what "normal" means for future classification.
- Classification is read-only and never cordons or drains nodes; a `degraded`
  verdict is a signal to investigate.
- Prefer the c-val baseline modules (robust median/MAD logic) over ad hoc shell
  parsing of the metric DBs.

## Verification Steps

Before and after code changes, run:

```bash
bash -n validation-tests/0-env.sh validation-tests/run-test.sh validation-tests/db-update.sh validation-tests/dltest/dltest.sh validation-tests/storage/storage.sh
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q cval tests
```

## Known Pitfalls

- The repository is named `c-val`, but the importable package is `cval`; do not treat this as a duplicate checkout.
- Runtime jobs still clone c-val from GitHub; pinning image/code version remains future work.
- `run` dry-run does not submit resources. Real submission requires both `--submit` and `--confirm submit`.
- `jobs --watch` is read-only and does not cancel timed-out jobs.
- Result JSON uses schema `cval.results.v1`; keep `CVAL_RESULT_ENV_FILE` fallback until all in-pod code uses JSON directly.