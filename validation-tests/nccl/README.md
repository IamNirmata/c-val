# NCCL and HCA validation test

Runs a single-node PyTorch/NCCL all-reduce across the configured GPU count while
sampling InfiniBand transmit counters. `run-test.sh` retains benchmark output,
the IBBW log, and a validated `summary.json`.

[summary.py](summary.py) validates and combines benchmark metrics with HCA
samples. [logger.py](logger.py) owns the single-log hook using the shared safe
writer. New output lives in
`/data/continuous_validation/logs/nccl/<node>/<run-id>/`: `job.log`,
`summary.json` and `ibbw-<run-id>.log`. There is no per-test result file.

The summary contains aggregate bus bandwidth, algorithm bandwidth, latency,
iteration/data-size identity, and per-port bandwidth samples. For a passing
phase, `cval.storage.run_ingest` writes one raw `metadata/test-nccl.db` `IB_HEALTH` row.
The retained table name denotes measurements, not a health-class decision.

Troubleshooting:

- verify the pinned image supplies PyTorch, CUDA, NCCL, and `torchrun`;
- inspect the read-only `/sys` mount if HCA samples are absent;
- inspect `NCCL_LOG_FILE`, `NCCL_IBBW_LOG_FILE`, and `NCCL_SUMMARY_FILE` for
  collective or summary failures.