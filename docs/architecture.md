# Architecture

## Runtime flow

1. Discover matching GPU nodes.
2. Read each node's latest raw validation timestamp from
   `metadata/validation.db`.
3. Prioritize never-tested and stale nodes, applying submission cooldown.
4. Check prioritized nodes for current GPU, CPU, memory, RDMA, readiness, and
   scheduling eligibility.
5. Render and submit Volcano jobs from an exact published commit after explicit
   confirmation.
6. Run enabled registry tests through the descriptor-anchored supervisor and
   generic runner.
7. Validate canonical artifacts and write raw status and metric databases.
8. Retain result JSON, logs, events, and test artifacts.

No step derives a node-health verdict, class, score, or ranking.

## Raw databases

- `metadata/validation.db`: built-in outcome rows and `latest_status`;
- `metadata/test-storage.db`: FIO metrics;
- `metadata/test-nccl.db`: consolidated `IB_HEALTH` metrics;
- `metadata/dltest_numerical_correctness.db`;
- `metadata/dltest_compute_performance.db`;
- `metadata/dltest_collective_performance.db`;
- `metadata/dltest_overlap_performance.db`.

`IB_HEALTH` is a retained raw table name. It contains measurements and does not
represent a framework health-class decision.

## Validation runtime

`[tests.<id>]` entries select repository-local `cval.test.v1` descriptors. A
descriptor owns execution metadata, minimum resources, test settings, artifact
summary name, and optional config/raw-export plugin methods.

The supervisor reserves canonical evidence paths through retained file
descriptors. The runner writes `cval.results` transitions and invokes tests in
registry order. `validation-tests/db-update.sh` verifies result identity,
configuration snapshot, digest, evidence paths, and database targets before
writing.

Passing DL runs serialize raw metric ingestion through
`metadata/.dl-metric-ingest.lock`. Storage and NCCL metrics are written only
for passing phases. The aggregate `all` row is committed after required metric
writes succeed.

## Safety boundaries

- Discovery and reporting are read-only.
- `plan` never submits.
- Validation creation requires an exact commit and explicit confirmation.
- Timeouts retain Kubernetes jobs and evidence.
- Backup apply requires separate capacity, quiescence, and confirmation gates.
- Existing historical non-raw databases are neither migrated nor deleted by
  normal operation.