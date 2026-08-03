# Configuration

The operator config is `config/cval.toml`.

## Sections

- `[cluster]` — namespace, current PVC reader pod name, node filter, tolerated taints;
- `[scheduling]` — staleness threshold and batch size;
- `[job]` — validation template, prefix, repository, and ref;
- `[policy]` — namespace allowlist, max batch size, submit confirmation;
- `[monitoring]` — read-only polling and timeout values;
- `[storage]` — current authoritative raw DB paths;
- `[runtime]` — repository, evidence root, test directory, and DL artifact root;
- `[tests.<id>]` — only `enabled` and repository-relative `config_path`;
- `[job_template]` — validation-job resources;
- `[baseline]` — evaluator DB root, tolerances, robust statistics, DL aggregation,
  and loop cadence.

There are no normalized-history, alternate per-test ingestion, or dedicated
health-evaluator state/write sections.

`[job].git_ref` is an all-zero fail-closed placeholder in source control.
Every real `validate` or `run` submission must provide an exact published,
nonzero, lowercase 40-hex commit with `--git-ref`.

## Current storage paths

`[storage]` declares exactly:

- `validation_db_path`;
- `storage_db_path`;
- `nccl_db_path`;
- `dl_numerical_db_path`;
- `dl_compute_db_path`;
- `dl_collective_db_path`;
- `dl_overlap_db_path`.

## Test descriptors

A `cval.test.v1` file may contain:

- `[test]` execution metadata;
- `[requirements]` shared job resource requirements;
- `[settings]` test-specific settings;
- `[artifacts] summary_filename`;
- `[plugin]` with `config`, `baseline`, and/or `export` capabilities.

No framework-owned per-test DB is required or automatically created. A future
test with unique persistence implements that behavior in its plugin and
test-owned settings after the schema is designed.

Inspect effective configuration and descriptors with:

```text
cval config
cval tests list
cval tests describe <id>
```

Development acceptance is a real cluster validation from an exact published
commit; local registry validation is not a pre-submit gate.
