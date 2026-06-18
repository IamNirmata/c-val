# Operations Runbook

## Safe Preflight

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --output json
```

Confirm the dry-run output before submitting. Look for:

- `dry_run: true`
- `submitted_count: 0`
- job names include the image segment, such as `pytorch-26-05-py3`
- selected node is not cordoned
- selected node is expected by the operator

## Continuous Live Runner

The tmux live runner is intentionally rolling, not static. It keeps at most the
configured batch size active at any moment, and before filling each open slot it
rebuilds the ranked candidate list from live Kubernetes state and current DB
status. This prevents it from submitting to nodes that were free during an older
scan but have since become occupied or unschedulable.

Start, inspect, attach, and stop:

```bash
scripts/cval-live.sh start
scripts/cval-live.sh status
scripts/cval-live.sh attach
scripts/cval-live.sh stop
```

If a job remains `Pending` past `pending_start_timeout_seconds`, the runner
deletes only that c-val validation job and then rebuilds the live ranked list
before submitting a replacement.

## One-Node Validation

1. Push code and capture the commit SHA.
2. Build a pinned dry-run plan:

   ```bash
   python -m cval.cli run \
     --live-status \
     --threshold-days 4 \
     --batch-size 1 \
     --git-ref <commit-sha> \
     --output json
   ```

3. Ask for operator approval.
4. Submit exactly one job:

   ```bash
   python -m cval.cli run \
     --free-nodes <node> \
     --live-status \
     --threshold-days 4 \
     --batch-size 1 \
     --git-ref <commit-sha> \
     --timestamp <timestamp> \
     --submit \
     --confirm submit \
     --output json
   ```

5. Monitor read-only:

   ```bash
    python -m cval.cli jobs \
     --jobs <job-name> \
       --watch \
     --timeout-seconds 1200 \
     --poll-interval-seconds 30 \
     --output json
   ```

6. Verify result JSON and DB rows:

   ```bash
   python -m cval.cli status --output json
   python -m cval.cli result --result-json <result-json>
   ```

## Validated Example

Pinned run:

```text
commit: c9a762a65bf9ae2989d71a01395d86dbc5c96af5
node: slc01-cl02-hgx-0204
job: cval-slc01-cl02-hgx-0204-pytorch-26-05-py3-1781134840
phase: Completed
result: storage=pass, nccl=pass, dltest=pass, overall=pass
```

## Baselines and Node Classification

Build a baseline from recent results, review it, then promote it to active:

```bash
# Dry-run: print the robust per-metric baseline (no write)
python -m cval.cli baseline build --test-type storage --window-days 30

# Store as a candidate and review
python -m cval.cli baseline build --test-type storage --window-days 30 \
  --baseline-id storage-2026Q2 --store
python -m cval.cli baseline show storage-2026Q2 storage

# Promote to active (changes what "normal" means going forward)
python -m cval.cli baseline activate storage-2026Q2 storage
```

Classify nodes against the active baseline and act on degraded nodes:

```bash
python -m cval.cli baseline classify --test-type storage --output table
python -m cval.cli baseline classify --test-type nccl --node <node> --output json
python -m cval.cli baseline classify --test-type dltest --store-results --output json
```

Classification itself is read-only against raw metric DBs. With `--store-results`,
the derived verdict is written to
`/data/continuous_validation/baselines/classification-results.db`; raw validation
`pass/fail/incomplete` rows remain untouched. A `degraded` verdict is a signal to
investigate, not an automated action; c-val does not cordon or drain nodes.

### Background Baseline Services

Run these where `/data/continuous_validation` is visible. The intended location
is the `gcr-admin` PVC access pod, in a tmux session:

```bash
# One-shot dry operational checks before enabling loops
scripts/cval-baseline-build.sh run-once
scripts/cval-baseline-classify.sh run-once

# Long-running tmux loops
scripts/cval-baseline-build.sh start
scripts/cval-baseline-classify.sh start

# Inspect/attach/stop
scripts/cval-baseline-build.sh status
scripts/cval-baseline-build.sh attach
scripts/cval-baseline-build.sh stop

scripts/cval-baseline-classify.sh status
scripts/cval-baseline-classify.sh attach
scripts/cval-baseline-classify.sh stop
```

To start them inside the PVC pod, first exec into the running access pod and run
the same commands from the c-val checkout. Do not paste credentials into docs or
commands; use the kubeconfig already present on the operator machine.

For DL tests, the scripts first rebuild the four DL metric DBs from remapped rank
JSON files under `/data/dltest-results`. To run that rebuild manually:

```bash
python -m cval.cli db-rebuild-dltest-metrics \
  --results-root /data/dltest-results \
  --output-dir /data/continuous_validation/metadata
```

## Cleanup

c-val 2.0 does not delete jobs automatically. If cleanup is required, ask for explicit approval and run the exact delete command only for the intended validation job.