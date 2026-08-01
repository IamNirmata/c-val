# Design Decisions

## Dry-Run First

c-val operates GPU clusters. The safest default is to render intent without creating resources. `run` therefore defaults to dry-run and reports `submitted=false` until `--submit --confirm submit` is provided.

## Keep Deterministic Validation in Jobs

The orchestrator should not infer GPU health from arbitrary logs. It submits deterministic storage, NCCL, and DL validation jobs and consumes their structured outputs.

## Use the CLI as the Agent Contract

Hermes and human operators use the same `cval.cli` commands. This avoids a separate hidden agent API and makes every action reproducible from a shell.

## Use TOML for Operator Configuration

c-val defaults live in `config/cval.toml`. TOML gives operators comments,
typed nested sections, and Git-friendly review without adding dependencies,
because Python 3.11+ includes `tomllib`. YAML remains reserved for Kubernetes
manifests, JSON for machine outputs and result artifacts, and env vars for
last-mile runtime overrides.

The Volcano renderer uses PyYAML separately from TOML configuration. It rejects
duplicate semantic keys and validates the one-task/one-container object tree
both before and after substitutions, so escaped, tagged, flow-style, or
configuration-induced key collisions cannot reach submission.

## Read-Only Status Access

`cval status` opens SQLite with `mode=ro` through the PVC access pod. It must not create tables or mutate metadata.

## Explicit Runtime Code Ref

Validation jobs accept `CVAL_GIT_REF` so operators can pin a commit or tag. This prevents accidental validation against a moving `main` branch.

## Generic Runtime Payload Instead of Test-Specific YAML

The Volcano template contains shared infrastructure and a single encoded runtime
context rather than one environment placeholder for every storage/NCCL/DL
setting. The renderer builds that context from validated typed configuration,
shell-quotes every fixed-name value, and base64-encodes it for YAML-safe
transport. The pod sources it after checking out the pinned ref. This makes
adding test settings independent of Kubernetes template edits while preserving
current v1 runner variables during migration.

## Structured Result JSON Before DB Ingestion

The generic runner atomically writes `cval.results.v2` at initialization and
every state transition. Dynamic test IDs, phases, timings, config digests, and
canonical evidence paths are preserved even after interruption. `db-update.sh`
uses the v2 reader's legacy storage/NCCL/DL projection for compatibility writes
and can independently dispatch U7 canonical per-test ingestion. Historical
`cval.results.v1` remains readable and is never rewritten.

## Keep U6 and U7 Writes Independently Default-Off

Deploying code must not silently create a new live database. Run history and
canonical per-test ingestion therefore use separate strict snapshot-bound
Booleans, both defaulting to false. Compatibility writes continue unchanged.
Each gate needs its own backup, dry-run evidence, exact activation command, and
operator approval.

## Framework Owns Adapter Transactions

The common per-test raw row is committed independently from test-specific
metrics. A passing test's trusted adapter runs in a fresh spawned process and
uses SQL RPC; the parent retains the raw SQLite connection, authorizer,
transaction, schema checks, durable receipt validation, commit, and rollback.
Adapter failure rolls back all metric DDL/rows/receipt while preserving raw
status and other tests. Failed or interrupted tests never invoke metric parsing
but do validate an existing adapter schema.

Repository adapters are trusted extension code, not an OS sandbox. Direct
filesystem/process abuse or opening another SQLite connection is outside this
contract; framework APIs never hand an adapter the parent's live connection or
authority over another test's declared DB path.

## Core Owns Logs and Timeouts

Tests do not implement framework logging or process supervision. The generic
runner executes setup and workload as argument arrays, applies one bounded
per-test deadline, terminates the process group on timeout, streams stdout and
stderr into global and per-test files, emits `cval.event.v1`, and continues to
later tests after a test failure.

## Monitoring Does Not Delete; Live Pruning Is Separately Gated

`jobs --watch` reports timeout but does not delete or cancel jobs. The separate
continuous runner defaults to audit mode and cannot delete jobs. Submit mode
also leaves pruning disabled unless it has the exact independent
`CVAL_PRUNE_CONFIRM=delete-pending` gate; only then may it prune stale `Pending`
jobs matching its configured namespace and shared job-name prefix. Other
cleanup needs explicit operator approval.

## Rebuild Live Candidates Per Slot

The continuous runner rebuilds the ranked candidate list before filling each
open batch slot. GPU free/schedulable state can change within seconds as other
cluster users submit work, so a long-lived precomputed queue is unsafe. The
runner only carries short-cycle memory for nodes already submitted or deleted
for pending timeout in the current cycle.

This is a per-slot rebuild policy: when one slot opens, c-val re-reads live
Kubernetes state and validation DB state before selecting exactly one next node.

## Robust Statistics for Baselines

Baselines use the median and MAD (scaled by 1.4826), not mean and standard
deviation. GPU-fleet performance metrics are skewed and contaminated by a few
degraded nodes; the mean has a breakdown point of 0 and an inflated standard
deviation widens the band and hides the next anomaly. The median tolerates up to
50% contamination, which is what fleet validation needs. Extreme outliers are
trimmed with the modified z-score before the baseline is computed, and the whole
stack stays stdlib-only (no numpy/scipy).

## Directional Acceptance Bands

Each metric carries a direction. Performance metrics are one-sided
(`busbw`/IOPS are `low_bad`; latency/time are `high_bad`), so a better-than-median
result is never a violation. Correctness metrics are `two_sided`. The band
half-width is `max(z * 1.4826 * MAD, tolerance_pct/100 * |median|)`, so the
configured engineering tolerance acts as a floor under the data-driven width.
Deterministic DL numerical metrics (MAD = 0) fall back to the relative tolerance.

## Versioned Baselines: Candidate Then Active

Built baselines are immutable records with a `candidate -> active -> superseded`
lifecycle. New baselines are candidates by default and must be explicitly
activated, so a slowly degrading fleet cannot silently re-baseline itself as
"normal". Activation supersedes only the previous active baseline for the same
`(test_type, stratum)`.

## Keep Baseline Decisions Separate From Raw Results

Raw validation `pass/fail/incomplete` rows stay in `metadata/validation.db` and
raw metric DBs. Dynamic baselines and derived node classifications live under
`/data/continuous_validation/baselines`. This keeps deterministic test results
immutable while allowing baseline decisions to evolve as active baselines change.
`classification-results.db` records whether a node passed the active baseline at
classification time without rewriting the original validation outcome.

## Content-Address U8 Candidates From Exact Evidence

U8 candidate identity includes canonical environment factors, current
descriptor and health-policy versions, one adapter schema version, effective
robust-z policy, every source/result/config/combination/receipt digest, exact
observation content, exact per-result sample keys, statistics, thresholds, and
lifecycle parent. Creation/storage timestamps are excluded. Observations are
canonically sorted before all order-sensitive statistics, so adapter iteration
order cannot create a different baseline from identical evidence.

This is stricter than identifying a candidate only from median/MAD summaries:
two evidence sets that happen to summarize alike must not share an identity.

## Keep U8 Candidate and Metric Classification Logic in Core

Repository adapters are trusted readers, not alternative health engines. They
declare exact metric specs, return typed finite observations, and expose an
explicit `health_policy_version`. Core validates provenance and sample coverage,
builds robust candidates, creates normalized bands, applies DNR precedence, and
produces metric verdicts. A `strategy="custom"` plugin may only aggregate those
metric verdicts and must record a versioned aggregation policy. Exporting a
custom `build_candidate` hook is rejected.

This boundary prevents a plugin from overriding raw failure DNR, hiding a
missing rank, changing metric evidence, or fabricating content identity.

## Treat Exact Sample Membership as Health Evidence

Distinct result-ID coverage is insufficient for pooled metrics: a result can
still omit half its ranks while retaining the same ID. U8 therefore persists
every `(source, expanded_metric, result_id, sample_key)` membership. Training
requires one stable non-empty sample-key set across all source results;
classification requires exact current membership. Missing or extra samples are
DNR `incomplete_metric_coverage`, never nominal.

## Make Immutable SQLite Evidence Authoritative

Candidate sources, receipt provenance, sample coverage, statistics, bands, and
build-trigger decisions are correctness-authoritative and protected by exact
schema manifests plus SQL update/delete guards. Every baseline has one immutable
trigger row and belongs to one unbranched candidate chain. Legal lifecycle
updates are limited to `candidate -> active -> superseded`; a partial unique
index enforces one active baseline per test/combination. Multi-query reads begin
an explicit query-only transaction so concurrent activation cannot mix
lifecycle snapshots.

`health_build_state` is deliberately advisory. It can optimize a future
evaluator and can be rebuilt; corruption there cannot authorize or block a
candidate activation.

## Keep U8 Operationally Inactive Until U9 Approval

U8 ships callable pure engine and local SQLite lifecycle services, not an
automatic evaluator. Built-in descriptors retain `auto_activate=false`; no
current CLI/background loop creates or activates live health DBs. U9 evaluator,
classification-history writes, live migration, and deployment are separate
approval-gated changes. Existing compatibility baseline commands and DBs remain
unchanged.