# c-val

c-val discovers GPU nodes, prioritizes stale raw validation coverage, runs
registry-defined tests in exact-commit Volcano jobs, and stores test outcomes
and metrics in SQLite.

c-val does not evaluate node health, build comparative thresholds, assign
health classes, or rank nodes. Those concerns are reserved for a future
evaluator design.

## Safety

- `plan`, `nodes`, `status`, `jobs`, and `results` are read-only.
- Validation submission requires an exact published commit plus explicit
  confirmation.
- `cval-live` operational commands are submit-only and require exact
  `CVAL_LIVE_CONFIRM=submit`; `start` pins the current remote branch tip.
- The source-controlled Git ref is an all-zero fail-closed placeholder.
- Test results never cordon, taint, reboot, delete, or otherwise mutate nodes.
- Kubernetes calls are bounded and manifests contain no embedded credentials.

## Raw data

Authoritative databases under `/data/continuous_validation/metadata/`:

- `validation.db`: per-test `pass`, `fail`, or `incomplete` outcomes and the
  `latest_status` view used for scheduling freshness;
- `test-storage.db`: FIO metrics;
- `test-nccl.db`: NCCL aggregate and InfiniBand port metrics;
- four `dltest_*.db` files: raw numerical, compute, collective, and overlap
  metrics.

Historical files outside `metadata/` are retained but are not read or written
by the framework.

## Common read-only commands

```text
cval tests validate
cval nodes --output json
cval status --output json
cval plan --live-status --git-ref <40-hex-commit>
cval results --test overall --type csv
```

The test registry uses repository-local `cval.test.v1` descriptors. New runs
emit canonical `cval.results`; historical `cval.results.v1` and
`cval.results.v2` artifacts remain readable.

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Operations](docs/operations-runbook.md)
- [Result schema](docs/result-schema.md)
