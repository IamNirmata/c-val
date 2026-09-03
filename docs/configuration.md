# Configuration

The operator configuration is `config/cval.toml`.

## Sections

- `[cluster]`: namespace, PVC reader, node filter, tolerated taints;
- `[scheduling]`: stale threshold, concurrent batch size, submission cooldown;
- `[job]`: template, prefix, source repository, fail-closed Git ref;
- `[policy]`: namespace allowlist, maximum batch size, confirmation phrase;
- `[monitoring]`: polling and timeout values;
- `[storage]`: authoritative raw SQLite paths;
- `[runtime]`: repository and retained evidence paths;
- `[tests.<id>]`: `enabled` and descriptor `config_path` only;
- `[job_template]`: validation pod resources and image.

There is no evaluator, threshold, classification, scoring, or ranking section.

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
`[settings]`, `[artifacts]`, and an optional `[plugin]`. Supported plugin
capabilities are `config` and `export`; export is read-only.

Inspect effective inputs with:

```text
cval config
cval tests list
cval tests describe <id>
cval tests validate
```