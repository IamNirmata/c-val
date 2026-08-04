# NCCL and HCA Health Validation Test

## Purpose

Runs a single-node PyTorch/NCCL all-reduce across the configured GPU count while sampling InfiniBand transmit counters. It validates collective execution and records aggregate bus bandwidth, latency, and per-port maximum bandwidth.

## Entrypoints

- `setup.sh` validates `python3`, `torchrun`, the benchmark, and the HCA monitor.
- `run-test.sh` is the canonical workload entrypoint.
- `run-nccl-allreduce.sh` is a compatibility wrapper for pinned jobs and older commands.
- `single-node-allreduce.py` contains the distributed workload and summary writer.
- `ibbw.sh` samples all discovered `/sys/class/infiniband` ports unless an optional device range is configured.

## Configuration

`test_config.toml` owns GPU count, iterations, BF16 data size, HCA monitoring,
NCCL environment settings, resource minimums, and the summary filename.

The global config owns only `[tests.nccl].enabled` and `config_path`.

The descriptor also owns optional PostgreSQL-evaluation profile constants:
test-definition version, collective, datatype, reduction, and exact message
bytes, warmup count, and latency-unit/conversion evidence.
`evaluation_enabled = false` keeps outbox emission disabled until production
cutover. GPU model, NCCL version, exact driver group, and normalized topology
class are collected in the GPU pod before the workload; no fallback is
invented. Validation jobs hold no PostgreSQL Secret.

## Execution and artifacts

The runner launches:

```text
torchrun --nproc_per_node=<gpu_count> single-node-allreduce.py ...
```

It captures benchmark output in `NCCL_LOG_FILE`, HCA samples in `NCCL_IBBW_LOG_FILE`, and writes machine-readable metrics to `NCCL_SUMMARY_FILE`. Monitor cleanup is protected by signal/exit traps.

The summary includes aggregate `GCR_BUSBW`, `GCR_LATENCY`, iteration count,
`GCR_DATA_SIZE_GB`, and `GCR_IB_PORT_BW_GBPS`. `db-update.sh` validates it and
writes the current `metadata/test-nccl.db` `IB_HEALTH` row plus
`LATEST_NODE_STATUS`/`NODE_RANKING`. `plugin.py` supplies configuration,
and raw-export hooks. Generic SQLite baseline/classification hooks are not
provided; PostgreSQL is the sole NCCL evaluator.

The historical workload and SQLite column report `GCR_LATENCY`/`LATENCY` in
milliseconds (`duration * 1000`). The optional PostgreSQL evaluator uses
canonical microseconds. Native outbox emission multiplies valid values by 1000,
so `628.2 ms` becomes `628200.0 us`; the descriptor records canonical unit
`us`, source unit `ms`, and conversion `ms_to_us_x1000` explicitly.

When enabled and runtime evidence exists, `db-update.sh` creates immutable
`pending/<c-val-run-id>.json` before authoritative raw SQLite writes, then
creates a digest-bound `committed/<c-val-run-id>.json` marker only after those
writes complete. Exact retries must be byte-equal; conflicts fail closed.
Failed selected NCCL tests with runtime evidence emit a `TEST_ERROR` or
`NO_RESULT` batch with null metrics and an exact error code. Setup failures that
occur before evidence collection still persist authoritative raw status but do
not emit an incomplete outbox. The credentialed NCCL process in the resident
evaluator consumes committed pairs idempotently and does not mutate or delete
them.

## Focused PostgreSQL evaluator

`cval nccl-eval` is a separate, optional subsystem implementing the immutable
five-class PostgreSQL process in `docs/evals/nccl-eval-process.md`. Install the
`postgresql` project extra only for these commands. Ordinary registry,
scheduler, and raw SQLite result commands do not import Psycopg.

All writes are separately exact-confirmation gated; inspection is nonwriting:

```text
cval nccl-eval schema
cval nccl-eval schema --apply --confirm schema
cval nccl-eval ingest --input batch.json
cval nccl-eval ingest --input batch.json --apply --confirm ingest
cval nccl-eval emit-outbox --result-json result.json --summary summary.json --runtime-evidence runtime-evidence.json --outbox-root /data/continuous_validation/nccl_eval/outbox
cval nccl-eval ingest-outbox --outbox-root /data/continuous_validation/nccl_eval/outbox --limit 5000
cval nccl-eval build-baselines
cval nccl-eval evaluate
cval nccl-eval status
```

`DATABASE_URL` is required only by commands that connect to PostgreSQL and is
never included in receipts. The production database name must be `cval`.
Copied-SQLite evaluator migration is removed; new PostgreSQL ingestion accepts
only native exact-provenance batches. Source deployment manifests remain
non-runnable until a reviewed phased rollout replaces placeholders and receives
separate live approval.

## Troubleshooting

- Setup failure: verify the pinned image supplies PyTorch, CUDA, NCCL, and `torchrun`.
- No HCA samples: inspect the read-only `/sys` mount and discovered ports.
- Collective failure: inspect NCCL debug output, GPU visibility, and configured P2P/SHM/network values.
- Missing summary: inspect rank failures and `NCCL_LOG_FILE`.
