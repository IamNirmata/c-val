# Configuration

The operator configuration is [config/cval.toml](../config/cval.toml). Select a
reviewed alternative with `cval --config <path> <command>` or `CVAL_CONFIG`.
Public commands use that configuration for namespace, PVC reader, database paths,
job template/repository/prefix, namespace allowlist and maximum batch size;
duplicate command-line overrides for those settings have been removed.

## Sections

- `[cluster]`: namespace, PVC reader, node filter, tolerated taints;
- `[scheduling]`: stale threshold, concurrent batch size, submission cooldown;
- `[job]`: template, prefix, source repository, fail-closed Git ref;
- `[policy]`: namespace allowlist, maximum batch size, confirmation phrase;
- `[monitoring]`: polling and timeout values;
- `[sqlite_retry]`: SQLite lock retry policy used by ingestion and evaluation;
- `[evaluation]`: settings for the independent CPU evaluation service;
- `[storage]`: authoritative raw SQLite paths;
- `[runtime]`: repository and retained evidence paths;
- `[tests.<id>]`: `enabled` and descriptor `config_path` only;
- `[job_template]`: validation pod resources and image.

These sections are retained because current workflows consume them. The raw
framework does not classify or rank nodes; `[evaluation]` belongs to the separate
service and is not a reason to add evaluation to the live submission loop.

## Scheduling

`batch_size` defaults to `3`. `node_cooldown_seconds` defaults to `14400`.
`cval-live` plans all discovered nodes by default; a positive
`CVAL_PLAN_LIMIT` may bound an operational session explicitly.

`job.git_ref` is an all-zero placeholder. Every real submission supplies an
exact published lowercase 40-hex commit.

## Storage paths

`[storage]` declares `validation_db_path`, `storage_db_path`, `nccl_db_path`,
and the four `dl_*_db_path` values. The raw DL ingestion lock is
`<runtime.validation_root>/metadata/.dl-metric-ingest.lock`.

## Test descriptors

A `cval.test.v1` descriptor may contain `[test]`, `[requirements]`,
`[settings]`, `[artifacts]`, optional `[plugin]`, and test-owned `[evaluation]`
policy. Supported raw plugin capabilities are `config` and `export`; export
is read-only. The separate evaluator loads the evaluation policy.

Inspect effective inputs with:

```text
cval config
cval tests list
cval tests describe <id>
cval tests validate
```