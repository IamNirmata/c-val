# c-val

c-val is a dry-run-first continuous validation framework for GPU clusters. It discovers schedulable free GPU nodes, prioritizes stale or never-tested nodes, renders Volcano validation jobs, gates real submission behind explicit approval, monitors jobs read-only, and ingests deterministic storage, NCCL, and DL test results into SQLite metadata.

## Why `c-val` and `cval` Both Exist

- `c-val/` is the repository checkout and runtime clone directory. The hyphen is fine for a Git repository name and for in-pod paths such as `/workspace/c-val`.
- `cval/` is the Python package imported by the CLI. Python import names cannot contain hyphens, so the package is intentionally named `cval`.

There is not a nested duplicate repository; the repo root contains an importable package plus validation assets.

## Active Repository Layout

```text
cval/                  Python package and CLI orchestration code
docs/                  Architecture, workflow, operations, and result docs
config/                TOML defaults for cluster, scheduling, jobs, and policy
skills/                Hermes skill package for safe c-val operation
tests/                 Unit tests for planning, rendering, status, and ingestion
validation-tests/      Scripts and workloads executed inside validation pods
ymls/                  Active Kubernetes/Volcano templates
pyproject.toml         Package metadata and `cval` console entry point
```

Generated job YAML, old notebooks, backups, committed reports, and legacy helper scripts are intentionally not part of the active tree.

## Quick Start

Use the CLI module directly from the repo checkout:

```bash
python -m cval.cli --help
python -m cval.cli config
python -m cval.cli discover-free-nodes --output table
python -m cval.cli status --output table
```

Build a dry-run plan:

```bash
python -m cval.cli submit-plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref main \
  --output json
```

Real submission is explicit and policy-gated:

```bash
python -m cval.cli submit-plan \
  --live-status \
  --threshold-days 4 \
  --batch-size 1 \
  --git-ref <commit-or-tag> \
  --submit \
  --confirm submit
```

## Safety Model

- Read-only commands are the default for discovery, status, job phase checks, and monitoring.
- Planning and rendering are dry-run by default.
- Kubernetes job creation requires `--submit --confirm submit`.
- No command deletes or cancels jobs automatically.
- Runtime jobs should pin `CVAL_GIT_REF` to a commit or tag for reproducibility.

## Documentation

Start with [docs/README.md](docs/README.md), then use:

- [docs/configuration.md](docs/configuration.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/workflow.md](docs/workflow.md)
- [docs/cli-reference.md](docs/cli-reference.md)
- [docs/operations-runbook.md](docs/operations-runbook.md)
- [docs/result-schema.md](docs/result-schema.md)
- [docs/dl-test.md](docs/dl-test.md)
- [docs/hermes-integration.md](docs/hermes-integration.md)
- [docs/troubleshooting.md](docs/troubleshooting.md)

## Validation

Before pushing changes, run:

```bash
bash -n validation-tests/0-env.sh validation-tests/run-test.sh validation-tests/db-update.sh \
  validation-tests/dltest/dltest.sh validation-tests/storage/storage.sh
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q cval tests skills/c-val-hpc-engineer/scripts
```
