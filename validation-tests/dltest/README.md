# Deep-learning unit validation test

Runs the JSON-native DL unit-test package across local GPU ranks. It exercises
numerical correctness, compute, collectives, and communication/compute overlap.

`run-test.sh` creates an isolated work directory, launches `torchrun`, retains
rank JSON and logs, and writes a validated summary. For a passing phase,
`db-update.sh` serializes ingestion through
`metadata/.dl-metric-ingest.lock` and writes the four raw `dltest_*.db` metric
files.

Troubleshooting:

- verify the shared source package and selected test plan;
- inspect every rank result and log for the first failing task;
- verify complete rank coverage and finite metric values before ingestion.