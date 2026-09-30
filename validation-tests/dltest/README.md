# Deep-learning unit validation test

Runs the JSON-native DL unit-test package across local GPU ranks. It exercises
numerical correctness, compute, collectives, and communication/compute overlap.

`run-test.sh` creates an isolated work directory, launches `torchrun`, retains
rank JSON and logs, and writes a validated summary. The configured shared source
is `/data/continuous_validation/deep-learning-unit-test-main`; execution uses its
checked run-local copy. `PYTHONPATH` uses that work directory's physical path so
rank processes can import `dl_unit_test` after inherited descriptors are closed.

[summary.py](summary.py) owns rank/task coverage checks; [logger.py](logger.py)
owns the consolidated log hook with the shared safe writer. New runs retain
`job.log`, `summary.json` and `workdir/` together under
`/data/continuous_validation/logs/dltest/<node>/<run-id>/`. Rank JSON remains
below `workdir/test_plans/<plan>/runs/`. No per-test result file is generated.

For a passing phase, `cval.storage.run_ingest` serializes ingestion through
`metadata/.dl-metric-ingest.lock` and writes the four raw `dltest_*.db` metric
files.

Troubleshooting:

- verify the shared source package and selected test plan;
- inspect every rank result and log for the first failing task;
- verify complete rank coverage and finite metric values before ingestion.