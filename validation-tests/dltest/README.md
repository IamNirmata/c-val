# Deep-Learning Unit Validation Test

## Purpose

Runs the JSON-native deep-learning unit-test package across local GPU ranks. It validates numerical correctness, compute performance, collectives, and communication/compute overlap.

## Entrypoints

- `setup.sh` validates `python3`, `torchrun`, the source package, and selected test plan.
- `run-test.sh` is the canonical workload entrypoint.
- `dltest.sh` is a compatibility wrapper for pinned jobs and older commands.
- `summarize_results.py` validates rank JSON artifacts and produces the run summary.

## Configuration

`test_config.toml` owns GPU count, test plan, iterations, minimum resources,
summary filename, and severity/count/fraction aggregation settings.

The global config owns only `[tests.dltest].enabled` and `config_path`.

## Execution and artifacts

The runner creates an isolated work directory, copies the selected test plan and optional source baseline, and launches:

```text
torchrun --nnodes=1 --nproc_per_node=<gpu_count> -m dl_unit_test ...
```

Raw rank JSON files are retained under the run work directory. `summarize_results.py` writes `DLTEST_SUMMARY_FILE` with plan, iterations, GPU count, task/status counts, and rank result references. Logs are written to `DLTEST_LOG_FILE`.

The evaluator rebuilds four current metric DBs from rank JSON:
`numerical_correctness`, `compute_performance`, `collective_performance`, and
`overlap_performance` under `metadata/`. `plugin.py` supplies configuration,
baseline aggregation, and export hooks; it does not create a second common DB.

## Health methodology

- Numerical correctness: `two_sided`, 0.1% tolerance.
- Compute and collective time: `high_bad`, 3% tolerance.
- Overlap metrics: `two_sided`, 20% tolerance.

Numerical metrics retain rank in the expanded metric name. Performance metrics
pool peer ranks. The sole baseline evaluator uses median/MAD bands and requires
severe misses to reach the configured count or fraction before degrading a DL
component.

## Troubleshooting

- Setup failure: verify the shared source package and selected `test_plan.json`.
- Torchrun failure: inspect every rank log/result for the first failing task.
- Summary failure: check missing/invalid rank JSON and task statuses.
- No classification: verify metric DB refresh, active compatible baseline, and combination factors.

Useful rank output signals include `status`, `norm_output`, weight/bias gradient
norms, forward/backward CPU and GPU timings, collective CPU/GPU timings, and
overlap `coll_*`/`layer_*` statistics. A healthy run has completed task states,
finite stable numerical values, and no traceback, exception, failure, NaN, Inf,
or mismatch markers. Keep failure classification specific to DL correctness,
runtime bootstrap, GPU execution, or collective behavior; compare storage and
NCCL results from the same canonical result before declaring a general node
failure.
