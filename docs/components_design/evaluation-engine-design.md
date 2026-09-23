# Evaluation Engine Design

This note records rationale and deferred decisions, not deployment instructions
or schema DDL. c-val owns discovery, validation and raw SQLite evidence; the
independent evaluator owns derived metric/test comparisons. Neither comparison
output nor a relative test class is a node diagnosis or an authorization to
change raw evidence.

## Current Contracts

- [Deployment and operation](../../deploy/evaluation-engine/README.md): commands,
  settings, source transport, mounts, locking, rollout and recovery.
- [Evaluation database](evaluation-database-design.md): implemented tables,
  identities, transaction boundaries and read semantics.
- [Raw database reference](raw-database-design.md): input tables and exact-run
  correlation; [result schema](../result-schema.md): canonical evidence.
- [Architecture](../architecture.md): separation from the raw validation loop.

Implementation lives in [evaluation_engine](../../evaluation_engine/) and the
test-owned descriptor modules. Code and effective configuration define behavior;
dated rollout observations belong in retained operational records.

## Inputs and Retired Draft Lessons

The retired research prototypes are not a second supported execution path.
Their lessons explain the explicit adapters, bounded history, compatible
cohorts and per-rank evidence in the maintained evaluator:

| Retired draft behavior | Replacement approach |
|---|---|
| First-table discovery; constructed `dltest-` keys | Explicit adapters; exact node/timestamp join; stored run keys |
| Hard-coded B200; mixed plans; global fallback | Verified cohort and full metric key; no cross-cohort fallback |
| Entire history loaded; ranks/repeated runs pooled | Bounded windows and indexed batches; equal node weighting |
| Numerical baseline voted by the evaluated run | Independent reference plus absolute/relative tolerance |
| Append unconstrained thresholds; replace all categorizations | Versioned baselines and transactional, idempotent evaluation history |

The comparisons explain measurements; they do not certify numerical truth or
hardware condition. Cross-node numerical consensus remains provisional, and
percentile timing bands require practical tolerances and held-out calibration.
Missing evidence must remain visible instead of inheriting a global threshold
or an older passing observation.

## Deferred Decisions

These are proposals, not implemented deployment or schema contracts:

- Automatic Pod recreation: consider a single-replica Recreate Deployment only
  after reviewing the fixed-name operator contract and single-writer recovery.
  The current fixed-name Pod does not recreate itself after deletion.
- Dedicated block storage: prefer a separate durable block PVC when provisioning
  is available. WAL would require a supported single-host filesystem and a
  separately approved migration; it is not an option for the shared NFS layout.
- Explicit pending discovery: a future discovery ledger could expose runs before
  evaluation. The current latest-result view does not discover unevaluated raw
  runs; changing cursor/receipt transactions requires replay and crash tests.
- Stronger references and provenance: certified numerical references, richer
  topology/cohort identity and calibrated false-alarm rates need their own
  evidence contracts. Do not infer missing fields or silently pool cohorts.

Future changes must cover late arrivals, conflicting duplicates, partial
ingestion, mixed cohorts, expired/missing thresholds, rank coverage, metric
direction, replay and baseline swaps. Deployment or migration requires separate
exact-command approval; raw evidence and derived history remain retained.