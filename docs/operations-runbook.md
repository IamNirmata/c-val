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
job: hari-gcr-cval-slc01-cl02-hgx-0204-1781134840
phase: Completed
result: storage=pass, nccl=pass, dltest=pass, overall=pass
```

## Cleanup

c-val 2.0 does not delete jobs automatically. If cleanup is required, ask for explicit approval and run the exact delete command only for the intended validation job.