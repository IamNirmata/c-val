# CLI reference

Run from the repository root with `python -m cval.cli` or the installed `cval`
entry point.

## Read-only commands

- `config` — effective configuration as JSON;
- `tests list|describe|validate` — registry and plugin validation;
- `nodes` — schedulable free-node discovery;
- `status` — latest pass/fail from `metadata/validation.db`;
- `jobs` — Volcano phases, optionally watched read-only;
- `result` — inspect canonical or historical structured result JSON;
- `results` — export latest raw status plus metrics/classification columns;
- `classifications` — merge/export latest per-target evaluator verdicts;
- `baseline list|show` — inspect stored baselines;
- `overview` — fleet coverage, queue, jobs, and merged verdict counts.

There is no separate normalized `history` command. Current status and result
exports use `metadata/validation.db` and its existing latest-status view.

## Planning and submission

- `plan --git-ref <40-hex-commit>` inspects the queue and rendered job names
  without submitting;
- `run` always requires `--submit --confirm submit`;
- `validate --node <node> --git-ref <40-hex-commit> --submit --confirm submit`
  runs one real on-demand cluster job and reports canonical raw evidence;
- `validate` rejects moving refs, partial SHAs, unavailable/busy/NotReady nodes,
  and unknown node state before any create call;
- derived baseline/classification writes are not coupled to `validate`.

## Canonical evaluator

```text
cval baseline build --test-type <target> [--store|--activate]
cval baseline list [--test-type <target>]
cval baseline show <baseline-id> <target>
cval baseline activate <baseline-id> <target>
cval baseline classify --test-type <target> [--store-results]
```

`--store-results` writes the deterministic per-target classification DB unless
`--classification-db-path` explicitly overrides it.

## Hidden in-pod current DB commands

- `db-preflight-result`;
- `db-add-run-results`;
- `db-add-result` (retained hook, not the active grouped status path);
- `db-add-storage-result`;
- `db-add-nccl-health`;
- `db-rebuild-dltest-metrics`.

The removed normalized-history, alternate per-test ingestion, health-class,
offline parity/backup/service, and compatibility inventory/audit commands are
not parseable.

## Optional NCCL PostgreSQL subsystem

`nccl-eval emit-outbox` validates a canonical `cval.results` file, NCCL
summary, runtime evidence, and the configured descriptor without a database.
Mutation requires `--apply --confirm emit-outbox` and writes one immutable
file to an explicit outbox root.

`nccl-eval ingest-outbox --outbox-root <absolute-path> --limit <1..5000>` is a
database-free scanner by default. It lists valid/invalid immediate `.json`
files and profile IDs. Apply requires `DATABASE_URL` plus
`--apply --confirm ingest-outbox`; invalid selected files prevent database
setup, per-file commits carry durable receipts, and processing stops on the
first error unless `--continue-on-error` is explicit.

`nccl-eval resident --outbox-root <absolute-path>` combines outbox ingestion,
due baseline builds, stale-claim recovery, and queue evaluation in one
signal-aware process. Mutation requires `--apply --confirm resident`.

The other exact gates are `schema`, `grant-runtime`, `ingest`,
`migrate-legacy`, `calibration`, `build-baselines`, `evaluate`, `worker`, and
`recover`.
