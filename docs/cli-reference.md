# CLI Reference

Run commands from the c-val repository root.

Use `--config /path/to/cval.toml` before the subcommand to load a non-default
configuration file.

## Public Commands

The public operator/Hermes CLI is intentionally small:

```text
cval config
cval status
cval nodes
cval run
cval jobs
cval result
```

The `baseline` command group (build, classify, activate, show, list) is also a
public command surface; see [`baseline`](#baseline).

Older command names remain available for compatibility, but are hidden from
`--help` and should not be used in new docs or automation.

## `config`

Print the effective TOML configuration:

```bash
python -m cval.cli config
python -m cval.cli --config /path/to/cval.toml config
```

## `status`

Read latest validation status from SQLite through the PVC access pod using
`mode=ro`:

```bash
python -m cval.cli status --output table
python -m cval.cli status --output json
python -m cval.cli status --output tsv
```

## `nodes`

Read pods and nodes, calculate GPU usage, and show schedulable free GPU nodes:

```bash
python -m cval.cli nodes --output table
python -m cval.cli nodes --output json
```

## `run`

Plan a validation batch. This is dry-run by default:

```bash
python -m cval.cli run \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <branch-tag-or-commit> \
  --output json
```

Real submission requires explicit confirmation:

```bash
python -m cval.cli run \
  --free-nodes <node> \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <branch-tag-or-commit> \
  --submit \
  --confirm submit \
  --output json
```

Policy gates:

- namespace must be allowlisted
- planned job count must not exceed max batch size
- confirmation must match `submit`

## `jobs`

Read Volcano job phase once:

```bash
python -m cval.cli jobs --jobs <job-name> --output json
```

Watch until terminal or timeout:

```bash
python -m cval.cli jobs \
  --jobs <job-name> \
  --watch \
  --timeout-seconds 1200 \
  --poll-interval-seconds 30 \
  --output json
```

`jobs --watch` is read-only; it does not delete or cancel timed-out jobs.

## `result`

Inspect structured result JSON in env-line form:

```bash
python -m cval.cli result --result-json <result.json>
```

Expected output:

```text
GCRRESULT1=pass
GCRRESULT2=pass
GCRRESULT3=pass
overall_result=pass
```

Or emit JSON:

```bash
python -m cval.cli result --result-json <result.json> --output json
```

## `baseline`

Build dynamic baselines from result DBs, manage their lifecycle, and classify
nodes. See [Baselines and Node Classification](baselines.md) for the method.

Build a baseline. Dry-run prints the computed robust metrics; add `--store` to
persist as a candidate, and `--activate` to also promote it to active:

```bash
python -m cval.cli baseline build --test-type nccl --window-days 30 --store
python -m cval.cli baseline build --test-type storage \
  --image-name pytorch:26.05-py3 --baseline-id storage-2026Q2 --activate
python -m cval.cli baseline build --test-type dltest --test-plan 80gb-example --output json
```

Promote, inspect, and list:

```bash
python -m cval.cli baseline activate storage-2026Q2 storage
python -m cval.cli baseline show storage-2026Q2 storage --output json
python -m cval.cli baseline list --test-type storage --output json
```

Classify nodes against the active (or a named) baseline. With no `--node`, every
node seen in the window is classified:

```bash
python -m cval.cli baseline classify --test-type storage
python -m cval.cli baseline classify --test-type nccl \
  --node <node> --baseline-id <id> --output json
```

Legacy directory-based references remain available:

```bash
python -m cval.cli baseline load   <baseline-dir> <test-type>
python -m cval.cli baseline ingest <baseline-dir> <test-type>
python -m cval.cli baseline compare <baseline-id> <test-type> --result-json <result.json>
```

## Internal Compatibility Commands

These names are hidden from `--help` but still parse for existing scripts and
old notes:

```text
discover-free-nodes
plan
submit-plan
job-status
monitor-jobs
result-env
prioritize
render-job
run-batch
db-add-result
db-add-storage-result
db-add-nccl-result
```
