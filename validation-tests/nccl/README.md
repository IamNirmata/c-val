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

`test_config.toml` owns GPU count, iterations, BF16 data size, HCA monitoring, NCCL environment settings, resource minimums, artifact paths, combination factors, and metric directions/tolerances.

The global config owns only `[tests.nccl].enabled` and `config_path`.

## Execution and artifacts

The runner launches:

```text
torchrun --nproc_per_node=<gpu_count> single-node-allreduce.py ...
```

It captures benchmark output in `NCCL_LOG_FILE`, HCA samples in `NCCL_IBBW_LOG_FILE`, and writes machine-readable metrics to `NCCL_SUMMARY_FILE`. Monitor cleanup is protected by signal/exit traps.

The summary includes aggregate `GCR_BUSBW`, `GCR_LATENCY`, iteration count,
`GCR_DATA_SIZE_GB`, and `GCR_IB_PORT_BW_GBPS`. `plugin.py` implements
`cval.plugin.v1` config, ingestion, and health capabilities. It accepts one
passing current-run summary, validates
typed finite metrics and HCA sample consistency, then writes one immutable wide
`IB_HEALTH` row, `LATEST_NODE_STATUS`/`NODE_RANKING`, schema version, and durable
receipt in one framework-owned transaction. The canonical DB is
`validation_tests/nccl/nccl_results.db`.

Canonical writes remain disabled while
`storage.per_test_ingestion_enabled=false`; `metadata/test-nccl.db` remains the
production compatibility surface.

## Health methodology

- Policy version: `nccl.health.v1`.
- Bus bandwidth: `low_bad` with 5% tolerance.
- Latency: `high_bad` with 5% tolerance.
- Per-port maxima remain diagnostic evidence and are not baseline metrics yet.

Each result contributes one exact stable sample for each aggregate metric.
Combination factors include image, CUDA, PyTorch, iteration count, and data
size; the reader also binds the canonical LA timestamp and HCA receipt content.
U8 uses robust statistics, `max_metric_class.v1`, eight qualifying results, ten
new results, and the candidate/active/superseded lifecycle.

`auto_activate=false`; no live NCCL health DB/evaluator is enabled. Existing
compatibility baselines remain operational until separately approved U9 and
migration work.

## Troubleshooting

- Setup failure: verify the pinned image supplies PyTorch, CUDA, NCCL, and `torchrun`.
- No HCA samples: inspect the read-only `/sys` mount and discovered ports.
- Collective failure: inspect NCCL debug output, GPU visibility, and configured P2P/SHM/network values.
- Missing summary: inspect rank failures and `NCCL_LOG_FILE`.
