# CLI Reference

Run commands from the c-val repository root.

Use `--config /path/to/cval.toml` before the subcommand to load a non-default
configuration file.

## Configuration

Print the effective configuration:

```bash
python -m cval.cli config
python -m cval.cli --config /path/to/cval.toml config
```

## Read-Only Commands

### `status`

Reads latest validation status from SQLite through the PVC access pod using `mode=ro`.

```bash
python -m cval.cli status --output table
python -m cval.cli status --output json
python -m cval.cli status --output tsv
```

### `discover-free-nodes`

Reads pods and nodes, calculates GPU usage, and excludes unschedulable nodes.

```bash
python -m cval.cli discover-free-nodes --output table
```

### `job-status`

Reads Volcano job phase once.

```bash
python -m cval.cli job-status --jobs <job-name> --output json
```

### `monitor-jobs`

Polls job phases until terminal or timeout. It does not delete or cancel jobs.

```bash
python -m cval.cli monitor-jobs \
  --jobs <job-name> \
  --timeout-seconds 1200 \
  --poll-interval-seconds 30 \
  --output json
```

## Dry-Run Commands

### `plan`

Builds a dry-run workflow plan.

```bash
python -m cval.cli plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 3 \
  --git-ref <branch-tag-or-commit> \
  --output json
```

### `submit-plan`

Without `--submit`, this is only a preview.

```bash
python -m cval.cli submit-plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <branch-tag-or-commit> \
  --output json
```

## Mutating Command

Real submission requires explicit confirmation:

```bash
python -m cval.cli submit-plan \
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

## Result Inspection

```bash
python -m cval.cli result-env --result-json <result.json>
```

Expected output:

```text
GCRRESULT1=pass
GCRRESULT2=pass
GCRRESULT3=pass
overall_result=pass
```

## In-Pod DB Ingestion Commands

These commands are called by `validation-tests/db-update.sh` inside validation
pods. They are package-native replacements for the removed legacy
`utils/functions.py` DB write commands.

```bash
python -m cval.cli db-add-result <node> <test> <pass|fail|incomplete> <timestamp> --db-path <validation.db>
python -m cval.cli db-add-storage-result <node> <timestamp> <storage-result-dir> --db-path <test-storage.db>
python -m cval.cli db-add-nccl-result <node> <timestamp> <busbw> <latency> --db-path <test-nccl.db>
```