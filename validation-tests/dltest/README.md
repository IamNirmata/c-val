# Deep-learning unit validation test

Runs the JSON-native DL unit-test package across local GPU ranks. It exercises
numerical correctness, compute, collectives, and communication/compute overlap.

`run-test.sh` creates an isolated work directory, launches `torchrun`, retains
rank JSON and logs, and writes a validated summary. The configured shared source
is `/data/continuous_validation/deep-learning-unit-test-main`; execution uses its
checked run-local copy. `PYTHONPATH` uses that work directory's physical path so
rank processes can import `dl_unit_test` after inherited descriptors are closed.

[summary.py](summary.py) owns rank/task coverage checks; [logger.py](logger.py)
owns stream formatting and the consolidated log hook with the shared safe writer. New runs retain
`job.log`, `summary.json` and `workdir/` together under
`/data/continuous_validation/logs/dltest/<node>/<run-id>/`. Rank JSON remains
below `workdir/test_plans/<plan>/runs/`. No per-test result file is generated.

For a passing phase, the registered [ingest.py](ingest.py) parses rank evidence,
validates complete coverage and writes the four raw `dltest_*.db` metric files.
It uses the shared `metadata/.dl-metric-ingest.lock` and bounded SQLite retries;
the coordinator requires all four receipts before publishing final status.
[plugin.py](plugin.py) owns runtime settings, evidence aliases, store declaration
and ingestion dispatch. Existing parser, generation and historical-read behavior
is retained; this change does not migrate raw data or add evaluation.

Troubleshooting:

- verify the shared source package and selected test plan;
- inspect every rank result and log for the first failing task;
- verify complete rank coverage and finite metric values before ingestion.