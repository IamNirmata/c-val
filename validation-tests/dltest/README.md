# Deep-Learning Unit Validation Test

## Purpose

Runs the JSON-native deep-learning unit-test package across local GPU ranks. It validates numerical correctness, compute performance, collectives, and communication/compute overlap.

## Entrypoints

- `setup.sh` validates `python3`, `torchrun`, the source package, and selected test plan.
- `run-test.sh` is the canonical workload entrypoint.
- `dltest.sh` is a compatibility wrapper for pinned jobs and older commands.
- `summarize_results.py` validates rank JSON artifacts and produces the run summary.

## Configuration

`test_config.toml` owns GPU count, test plan, iterations, minimum resources, artifact paths, health combination factors, component tolerances, and severity/count/fraction aggregation settings.

The global config owns only `[tests.dltest].enabled` and `config_path`.

## Execution and artifacts

The runner creates an isolated work directory, copies the selected test plan and optional source baseline, and launches:

```text
torchrun --nnodes=1 --nproc_per_node=<gpu_count> -m dl_unit_test ...
```

Raw rank JSON files are retained under the run work directory. `summarize_results.py` writes `DLTEST_SUMMARY_FILE` with plan, iterations, GPU count, task/status counts, and rank result references. Logs are written to `DLTEST_LOG_FILE`.

Current maintenance still rebuilds four compatibility metric databases from
rank JSON. Separately, `plugin.py` implements the U7 `cval.plugin.v1` config and
one-run ingestion plus U8 health capabilities. It validates the exact current summary,
identity, plan, iterations, GPU/rank coverage, invocation suffixes, task counts,
status counts, and all four metric components. It then writes
`numerical_correctness`, `compute_performance`, `collective_performance`, and
`overlap_performance` plus one durable receipt in a single framework-owned
transaction in `validation_tests/dltest/dltest_results.db`.

Canonical writes remain disabled while
`storage.per_test_ingestion_enabled=false`; the four metadata compatibility DBs
remain the production surfaces.

## Health methodology

- Policy version: `dltest.health.v1`.
- Numerical correctness: `two_sided`, 0.1% tolerance.
- Compute and collective time: `high_bad`, 3% tolerance.
- Overlap metrics: `two_sided`, 20% tolerance.

Numerical metrics retain rank in the expanded metric name. Pooled performance
metrics retain exact rank/task/metric sample keys, so a missing or extra rank is
DNR incomplete coverage rather than nominal. Candidate construction and every
metric verdict remain framework-owned. The custom
`dl_severity_count_fraction.v1` aggregation requires severe misses to reach the
configured count or fraction before degrading the aggregate; it cannot replace
metric evidence or DNR.

Baselines are stratified by image, CUDA, PyTorch, plan, and iterations; require
eight qualifying results and ten new results; and remain candidate-first with
`auto_activate=false`. No live DL health DB/evaluator is enabled.

## Troubleshooting

- Setup failure: verify the shared source package and selected `test_plan.json`.
- Torchrun failure: inspect every rank log/result for the first failing task.
- Summary failure: check missing/invalid rank JSON and task statuses.
- No classification: verify metric DB refresh, active compatible baseline, and combination factors.
