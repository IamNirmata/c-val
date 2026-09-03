# NCCL and HCA validation test

Runs a single-node PyTorch/NCCL all-reduce across the configured GPU count while
sampling InfiniBand transmit counters. `run-test.sh` retains benchmark output,
the IBBW log, and a validated `summary.json`.

The summary contains aggregate bus bandwidth, algorithm bandwidth, latency,
iteration/data-size identity, and per-port bandwidth samples. For a passing
phase, `db-update.sh` writes one raw `metadata/test-nccl.db` `IB_HEALTH` row.
The retained table name denotes measurements, not a health-class decision.

Troubleshooting:

- verify the pinned image supplies PyTorch, CUDA, NCCL, and `torchrun`;
- inspect the read-only `/sys` mount if HCA samples are absent;
- inspect `NCCL_LOG_FILE`, `NCCL_IBBW_LOG_FILE`, and `NCCL_SUMMARY_FILE` for
  collective or summary failures.