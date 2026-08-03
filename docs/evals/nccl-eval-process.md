# NCCL Loopback AllReduce Evaluation System

## 1. Purpose

Build a PostgreSQL-backed NCCL loopback AllReduce evaluation system for C-VAL.

The system must:

- Store raw NCCL test results for all nodes.
- Build independent baselines for each compatible test environment.
- Require at least 40 eligible results before activating a baseline.
- Create a new immutable baseline version after every additional 10 eligible results.
- Classify `BUS_BW` and `LATENCY` into five health classes.
- Store the exact baseline version used for every evaluation.
- Support multiple concurrent writers, readers, baseline builders, and evaluator workers.
- Preserve raw results and historical evaluations without overwriting them.

## 2. Technology and deployment

Use one PostgreSQL database named `cval` with three schemas:

```text
cval
├── nccl_raw
├── nccl_baseline
└── nccl_validation
```

Do not use separate SQLite files.

Recommended Kubernetes components:

```text
NCCL test Job (no PostgreSQL Secret)
    ↓ immutable native JSON on shared PVC
Resident evaluator Deployment (PostgreSQL runtime Secret)
    ├── durable outbox ingestion
    ├── baseline building
    ├── stale-claim recovery
    └── queue evaluation
             ↓
         PostgreSQL
```

The evaluator runs continuously as one Deployment replica. Polling every few
seconds is sufficient. PostgreSQL `LISTEN/NOTIFY` may be added as a wake-up
optimization, but the database queue remains the durable source of truth.

## 3. Health classes

Create a fixed lookup table:

| ID | Code | Label |
|---:|---|---|
| 1 | `EXCEEDING` | Exceeding baseline |
| 2 | `WITHIN` | Within baseline |
| 3 | `UNDERPERFORMING` | Underperforming |
| 4 | `DEGRADED` | Degraded |
| 5 | `CRITICAL` | Critical / suspected hardware failure |

Higher class number means worse health.

For numeric metrics:

```text
lower_bound <= measured_value < upper_bound
```

`upper_bound = NULL` means no upper limit.

Overall health:

```text
overall_health_class = GREATEST(bus_bw_class, latency_class)
```

Do not average health classes.

### Versioned median-centered derivation

`nccl-median-bands-v2` replaces quintile health labels. Eligible baseline
samples must be finite and strictly positive. Let `m = p50`.

BUS_BW (higher is better) uses ascending class order `5,4,3,2,1`:

- class 5 below `70% * m`;
- class 4 through `85% * m`;
- class 3 through `min(p05, 95% * m)`, raised only as needed to keep a strict
    non-empty range;
- class 2 through `max(p95, 105% * m)`;
- class 1 above that boundary.

LATENCY (lower is better) uses ascending class order `1,2,3,4,5`:

- class 1 below `min(p05, 95% * m)`;
- class 2 through `max(p95, 105% * m)`;
- classes 3 and 4 end at progressively worse `115% * m` and `130% * m`
    boundaries, raised only as needed to remain strictly contiguous;
- class 5 covers the remaining high-latency tail.

Both class-2 ranges contain `p50`, including tied/equal distributions. A value
equal to `p50` has severity exactly `50`; overall class and severity are the
worse (maximum) metric values. For an all-44 baseline, BUS_BW 44 and LATENCY
44 are both class 2 with severity 50.

## 4. Baseline compatibility profile

A baseline must not be selected using only CUDA + PyTorch + GPU model.

A baseline profile should include all fields that materially affect NCCL performance:

- Test name.
- Test definition/configuration version.
- GPU model.
- GPUs per node.
- CUDA version.
- PyTorch version.
- NCCL version.
- Relevant driver compatibility group.
- Relevant topology class.
- Message size or message-size group.
- Collective, datatype, reduction operation, and other important test settings.
- Iterations, nullable samples, and warmup iterations.
- Canonical `latency_unit = "us"` and any source-unit conversion evidence.

Human-readable example:

```text
nccl-loopback-ar:b200:8gpu:cuda13.2:pt2.12:nccl2.27:test-v1:8g
```

Use:

- `profile_id`: internal UUID primary key.
- `profile_key`: unique readable identifier.

## 5. PostgreSQL schema

### 5.1 `nccl_raw.test_run`

One row per NCCL test execution.

```sql
CREATE SCHEMA IF NOT EXISTS nccl_raw;
CREATE SCHEMA IF NOT EXISTS nccl_baseline;
CREATE SCHEMA IF NOT EXISTS nccl_validation;

CREATE TABLE nccl_raw.test_run (
    run_id                   UUID PRIMARY KEY,
    test_name                TEXT NOT NULL,
    test_definition_version  TEXT NOT NULL,

    started_at               TIMESTAMPTZ NOT NULL,
    completed_at             TIMESTAMPTZ,

    image_name               TEXT,
    image_digest             TEXT,

    cuda_version             TEXT NOT NULL,
    pytorch_version          TEXT NOT NULL,
    compiled_nccl_version    TEXT NOT NULL,
    runtime_nccl_package_version TEXT NOT NULL,
    driver_version           TEXT,

    gpu_model                TEXT NOT NULL,
    gpus_per_node            SMALLINT NOT NULL CHECK (gpus_per_node > 0),

    iterations               INTEGER NOT NULL CHECK (iterations > 0),
    samples                  INTEGER,

    test_config              JSONB NOT NULL DEFAULT '{}'::jsonb,
    test_config_fingerprint  TEXT NOT NULL,
    cval_run_id              TEXT NOT NULL,
    cval_result_digest       TEXT NOT NULL,
    summary_sha256           TEXT,
    runtime_evidence_sha256  TEXT NOT NULL,
    source_commit            TEXT NOT NULL,
    implementation_identity  TEXT NOT NULL,
    legacy_source            BOOLEAN NOT NULL,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`test_config` should include values such as:

```json
{
  "collective": "all_reduce",
  "datatype": "float32",
  "reduction": "sum",
  "message_size": "8G",
    "warmup_iterations": 5,
    "latency_unit": "us"
}
```

### 5.2 `nccl_raw.node_result`

One row per node result.

```sql
CREATE TABLE nccl_raw.node_result (
    result_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id             UUID NOT NULL
                       REFERENCES nccl_raw.test_run(run_id)
                       ON DELETE CASCADE,

    node_name          TEXT NOT NULL,
    test_timestamp     TIMESTAMPTZ NOT NULL,
    la_timestamp       TIMESTAMPTZ,

    bus_bw_gbps        DOUBLE PRECISION,
    latency_us         DOUBLE PRECISION,

    result_status      TEXT NOT NULL DEFAULT 'SUCCESS'
                       CHECK (
                           result_status IN (
                               'SUCCESS',
                               'TIMEOUT',
                               'TEST_ERROR',
                               'NO_RESULT'
                           )
                       ),

    error_code         TEXT,
    error_message      TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (run_id, node_name)
);
```

Raw results are append-only after ingestion.

`SUCCESS` requires both BUS_BW and LATENCY. Non-success rows may retain partial
diagnostic metrics but create no evaluation row; copied legacy rows missing
either metric map to `NO_RESULT`.

Do not store a Boolean `classified` column here.

### 5.3 `nccl_raw.nic_result`

Store mlx5 device values as rows, not columns.

```sql
CREATE TABLE nccl_raw.nic_result (
    result_id          BIGINT NOT NULL
                       REFERENCES nccl_raw.node_result(result_id)
                       ON DELETE CASCADE,

    device_name        TEXT NOT NULL,
    max_bus_bw_gbps    DOUBLE PRECISION,

    PRIMARY KEY (result_id, device_name)
);
```

Example:

```text
result_id | device_name | max_bus_bw_gbps
1001      | mlx5_0      | 44.5172
1001      | mlx5_4      | 44.5172
1001      | mlx5_6      | 44.5120
1001      | mlx5_7      | 44.5100
```

NIC values are diagnostic only and are not copied into the validation table.

### 5.4 `nccl_baseline.baseline_profile`

One row per compatible baseline environment.

```sql
CREATE TABLE nccl_baseline.baseline_profile (
    profile_id                  UUID PRIMARY KEY,
    profile_key                 TEXT NOT NULL UNIQUE,

    test_name                   TEXT NOT NULL,
    test_definition_version     TEXT NOT NULL,

    gpu_model                   TEXT NOT NULL,
    gpus_per_node               SMALLINT NOT NULL,

    cuda_version                TEXT NOT NULL,
    pytorch_version             TEXT NOT NULL,
    compiled_nccl_version       TEXT NOT NULL,
    runtime_nccl_package_version TEXT NOT NULL,
    driver_version_group        TEXT,
    topology_class              TEXT,
    source_commit               TEXT NOT NULL,
    image_digest                TEXT NOT NULL,
    implementation_identity     TEXT NOT NULL,

    test_config_fingerprint     TEXT NOT NULL,

    status                      TEXT NOT NULL
                                CHECK (
                                    status IN (
                                        'COLLECTING',
                                        'ACTIVE',
                                        'DISABLED'
                                    )
                                ),

    eligible_result_count       INTEGER NOT NULL DEFAULT 0,
    last_built_sample_count     INTEGER NOT NULL DEFAULT 0,

    active_baseline_version_id  UUID,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Add the foreign key to `active_baseline_version_id` after creating the baseline version table.

### 5.5 `nccl_baseline.baseline_version`

Baseline versions are immutable.

Never update thresholds of an existing active or superseded baseline version.

```sql
CREATE TABLE nccl_baseline.baseline_version (
    baseline_version_id         UUID PRIMARY KEY,
    profile_id                  UUID NOT NULL
                                REFERENCES nccl_baseline.baseline_profile(profile_id),

    version_number              INTEGER NOT NULL CHECK (version_number > 0),
    status                      TEXT NOT NULL
                                CHECK (
                                    status IN (
                                        'BUILDING',
                                        'ACTIVE',
                                        'SUPERSEDED',
                                        'FAILED'
                                    )
                                ),

    sample_count                INTEGER NOT NULL CHECK (sample_count >= 40),

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at                TIMESTAMPTZ,
    failure_reason              TEXT,

    supersedes_version_id       UUID
                                REFERENCES nccl_baseline.baseline_version(
                                    baseline_version_id
                                ),

    derivation_method_version   TEXT NOT NULL,

    bus_bw_mean                 DOUBLE PRECISION,
    bus_bw_p05                  DOUBLE PRECISION,
    bus_bw_p50                  DOUBLE PRECISION,
    bus_bw_p95                  DOUBLE PRECISION,

    latency_mean                DOUBLE PRECISION,
    latency_p05                 DOUBLE PRECISION,
    latency_p50                 DOUBLE PRECISION,
    latency_p95                 DOUBLE PRECISION,

    UNIQUE (profile_id, version_number)
);
```

Add:

```sql
ALTER TABLE nccl_baseline.baseline_profile
ADD CONSTRAINT baseline_profile_active_version_fk
FOREIGN KEY (active_baseline_version_id)
REFERENCES nccl_baseline.baseline_version(baseline_version_id);
```

### 5.6 `nccl_baseline.baseline_version_sample`

Store exact sample lineage.

```sql
CREATE TABLE nccl_baseline.baseline_version_sample (
    baseline_version_id   UUID NOT NULL
                          REFERENCES nccl_baseline.baseline_version(
                              baseline_version_id
                          )
                          ON DELETE CASCADE,

    result_id             BIGINT NOT NULL
                          REFERENCES nccl_raw.node_result(result_id),

    included              BOOLEAN NOT NULL,
    exclusion_reason      TEXT,
    added_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (baseline_version_id, result_id)
);
```

This table must show exactly which raw results were included or excluded from every baseline version.

### 5.7 `nccl_validation.health_class`

```sql
CREATE TABLE nccl_validation.health_class (
    class_id       SMALLINT PRIMARY KEY CHECK (class_id BETWEEN 1 AND 5),
    class_code     TEXT NOT NULL UNIQUE,
    class_label    TEXT NOT NULL,
    description    TEXT NOT NULL
);
```

Seed with the five fixed health classes.

### 5.8 `nccl_baseline.metric_threshold`

Store five ranges for `BUS_BW` and five ranges for `LATENCY` per baseline version.

```sql
CREATE TABLE nccl_baseline.metric_threshold (
    baseline_version_id   UUID NOT NULL
                          REFERENCES nccl_baseline.baseline_version(
                              baseline_version_id
                          )
                          ON DELETE CASCADE,

    metric_name           TEXT NOT NULL
                          CHECK (metric_name IN ('BUS_BW', 'LATENCY')),

    class_id              SMALLINT NOT NULL
                          REFERENCES nccl_validation.health_class(class_id),

    lower_bound           DOUBLE PRECISION NOT NULL,
    upper_bound           DOUBLE PRECISION,

    unit                  TEXT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        baseline_version_id,
        metric_name,
        class_id
    ),

    CHECK (lower_bound >= 0),
    CHECK (upper_bound IS NULL OR lower_bound < upper_bound)
);
```

The application must validate that ranges for a metric are:

- Non-overlapping.
- Contiguous.
- Exactly five.
- Covering all valid non-negative values.

Example BUS_BW thresholds:

```text
Class 1: [47, infinity)
Class 2: [42, 47)
Class 3: [40, 42)
Class 4: [35, 40)
Class 5: [0, 35)
```

Latency is lower-is-better, so class 1 has the lowest range and class 5 the highest.

### 5.9 `nccl_validation.evaluation_job`

This is the durable work queue and replaces the raw result `classified` field.

```sql
CREATE TABLE nccl_validation.evaluation_job (
    result_id          BIGINT PRIMARY KEY
                       REFERENCES nccl_raw.node_result(result_id)
                       ON DELETE CASCADE,

    profile_id         UUID
                       REFERENCES nccl_baseline.baseline_profile(profile_id),

    status             TEXT NOT NULL
                       CHECK (
                           status IN (
                               'PENDING',
                               'WAITING_FOR_BASELINE',
                               'PROCESSING',
                               'RETRY',
                               'COMPLETED',
                               'FAILED'
                           )
                       ),

    attempt_count      INTEGER NOT NULL DEFAULT 0,
    claimed_by         TEXT,
    claimed_at         TIMESTAMPTZ,
    claim_token        UUID,
    next_attempt_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ,
    last_error         TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

State meaning:

| State | Meaning |
|---|---|
| `PENDING` | Ready to evaluate |
| `WAITING_FOR_BASELINE` | Matching profile exists but no active baseline |
| `PROCESSING` | Claimed by an evaluator |
| `RETRY` | Temporary failure; retry later |
| `COMPLETED` | Evaluation committed |
| `FAILED` | Unrecoverable or retry limit reached |

### 5.10 `nccl_validation.evaluation`

Store one evaluation per raw result and baseline version.

```sql
CREATE TABLE nccl_validation.evaluation (
    evaluation_id                     BIGINT
                                      GENERATED ALWAYS AS IDENTITY
                                      PRIMARY KEY,

    result_id                         BIGINT NOT NULL
                                      REFERENCES nccl_raw.node_result(result_id),

    baseline_version_id               UUID NOT NULL
                                      REFERENCES nccl_baseline.baseline_version(
                                          baseline_version_id
                                      ),

    evaluation_scope                  TEXT NOT NULL
                                      CHECK (
                                          evaluation_scope IN (
                                              'OUT_OF_SAMPLE',
                                              'IN_SAMPLE',
                                              'REEVALUATION'
                                          )
                                      ),

    bus_bw_class                      SMALLINT NOT NULL
                                      REFERENCES nccl_validation.health_class(
                                          class_id
                                      ),

    bus_bw_severity_percentile        DOUBLE PRECISION NOT NULL
                                      CHECK (
                                          bus_bw_severity_percentile
                                          BETWEEN 0 AND 100
                                      ),

    latency_class                     SMALLINT NOT NULL
                                      REFERENCES nccl_validation.health_class(
                                          class_id
                                      ),

    latency_severity_percentile       DOUBLE PRECISION NOT NULL
                                      CHECK (
                                          latency_severity_percentile
                                          BETWEEN 0 AND 100
                                      ),

    overall_health_class              SMALLINT NOT NULL
                                      REFERENCES nccl_validation.health_class(
                                          class_id
                                      ),

    overall_severity_percentile       DOUBLE PRECISION NOT NULL
                                      CHECK (
                                          overall_severity_percentile
                                          BETWEEN 0 AND 100
                                      ),

    evaluated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    evaluator_version                 TEXT NOT NULL,

    failure_code                      TEXT,
    explanation                       TEXT,

    UNIQUE (result_id, baseline_version_id),

    CHECK (
        overall_health_class = GREATEST(
            bus_bw_class,
            latency_class
        )
    ),
    CHECK (
        overall_severity_percentile = GREATEST(
            bus_bw_severity_percentile,
            latency_severity_percentile
        )
    )
);
```

Do not duplicate node name, CUDA, PyTorch, GPU model, BUS_BW, LATENCY, baseline timestamp, or class labels here. Expose those through a joined view.

## 6. Required indexes

```sql
CREATE INDEX node_result_node_time_idx
ON nccl_raw.node_result(node_name, test_timestamp DESC);

CREATE INDEX node_result_created_at_idx
ON nccl_raw.node_result(created_at);

CREATE INDEX baseline_profile_lookup_idx
ON nccl_baseline.baseline_profile(
    test_name,
    test_definition_version,
    gpu_model,
    gpus_per_node,
    cuda_version,
    pytorch_version,
    compiled_nccl_version,
    runtime_nccl_package_version,
    source_commit,
    image_digest,
    implementation_identity,
    test_config_fingerprint
);

CREATE INDEX evaluation_job_pending_idx
ON nccl_validation.evaluation_job(
    next_attempt_at,
    created_at
)
WHERE status IN ('PENDING', 'RETRY');

CREATE INDEX evaluation_job_waiting_idx
ON nccl_validation.evaluation_job(profile_id)
WHERE status = 'WAITING_FOR_BASELINE';

CREATE INDEX evaluation_result_idx
ON nccl_validation.evaluation(result_id);

CREATE INDEX evaluation_health_time_idx
ON nccl_validation.evaluation(
    overall_health_class,
    evaluated_at DESC
);
```

## 7. Result ingestion transaction

When one NCCL run produces multiple node results:

1. Insert one `test_run`.
2. Insert all `node_result` rows.
3. Insert all `nic_result` rows.
4. Insert one `evaluation_job` per node result.
5. Commit all of the above in one transaction.

Use multi-row insert or `COPY` for large batches.

Example behavior:

```text
BEGIN
  insert test_run
  insert node_result rows
  insert nic_result rows
  insert evaluation_job rows
COMMIT
```

Insertion must be idempotent.

Use:

```text
UNIQUE (run_id, node_name)
```

to prevent duplicate raw rows.

## 8. Profile matching

For each raw result:

1. Join `node_result` to `test_run`.
2. Build or compute a deterministic `test_config_fingerprint`.
3. Match an existing `baseline_profile`.
4. If none exists, create one with:
   - `status = COLLECTING`
   - `eligible_result_count = 0`
   - `last_built_sample_count = 0`
5. Store `profile_id` in the evaluation job.

Profile creation must be concurrency-safe using a unique `profile_key` and `INSERT ... ON CONFLICT`.

## 9. Baseline sample eligibility

A result is eligible for baseline building only when:

```text
result_status = SUCCESS
BUS_BW is not NULL
LATENCY is not NULL
BUS_BW > 0
LATENCY > 0
test environment matches the profile exactly
result is not a duplicate
no blocking NCCL/GPU/test error exists
result passes configured sanity checks
```

Do not automatically allow every historical result into a baseline.

At minimum exclude:

- `TIMEOUT`
- `TEST_ERROR`
- `NO_RESULT`
- Missing BUS_BW
- Missing LATENCY
- Invalid or impossible values
- Mismatched test configuration

The baseline builder must store included and excluded samples in `baseline_version_sample`.

Avoid allowing degraded nodes to continuously pull the baseline lower. Support an eligibility policy such as:

- Explicitly approved calibration data.
- Known-good node cohort.
- Robust outlier filtering.
- Exclusion of known failed or remediated nodes.

The exact statistical method should be versioned using `derivation_method_version`.

### Calibration decision state machine

Calibration is an append-only state machine serialized per `result_id` in
PostgreSQL. The first decision is exactly version 1 `APPROVE`. Every later
decision is `latest + 1` and alternates `APPROVE`/`REVOKE`; skipped versions,
duplicate effective actions, and first-event revocation fail in a `BEFORE
INSERT` trigger. Exact `decision_id` replay is idempotent, but its receipt
reports the latest effective action rather than the historical event action.

Runtime roles cannot insert ledger rows directly. They execute the
security-definer `nccl_baseline.apply_calibration_decision(...)` function,
whose search path is fixed to `pg_catalog`. If a revoked result is an included
sample in the active version, that same transaction records an explicit
failure reason, moves the version `ACTIVE → FAILED`, clears the profile active
pointer, returns the profile to `COLLECTING`, and moves `PENDING`/`RETRY` jobs
to `WAITING_FOR_BASELINE`. Raw rows, sample lineage, and existing evaluations
are retained unchanged.

## 10. Baseline creation and update rules

### Initial build

```text
if active baseline does not exist
and eligible_result_count >= 40
then build the next version using 40 or more eligible samples
```

If the profile has no active pointer because its latest version is `FAILED`,
the replacement version supersedes that latest failed version. The prior
`last_built_sample_count` does not impose the +10 refinement gate while no
active baseline exists.

### Refinement

```text
if active baseline exists
and eligible_result_count >= last_built_sample_count + 10
then build next immutable version
```

Example:

```text
40 results → version 1
50 results → version 2
60 results → version 3
70 results → version 4
```

Do not modify an existing baseline version in place.

### Baseline build transaction

For a profile:

1. Acquire a transaction-level advisory lock for the profile.
2. Re-read `eligible_result_count`.
3. Re-check the 40/+10 rule.
4. Select eligible results.
5. Create a `BUILDING` baseline version.
6. Insert sample lineage.
7. Calculate distribution statistics.
8. Calculate five BUS_BW ranges.
9. Calculate five LATENCY ranges.
10. Validate ranges.
11. Mark the old active version `SUPERSEDED`.
12. Mark the new version `ACTIVE`.
13. Update:
    - `baseline_profile.active_baseline_version_id`
    - `baseline_profile.last_built_sample_count`
    - `baseline_profile.status = ACTIVE`
14. Move matching `WAITING_FOR_BASELINE` jobs to `PENDING`.
15. Commit.

Use a transaction-level advisory lock, for example:

```sql
SELECT pg_advisory_xact_lock(
    hashtextextended(:profile_id::text, 0)
);
```

Also rely on:

```text
UNIQUE (profile_id, version_number)
```

as the final correctness guarantee.

## 11. Evaluation worker concurrency

Multiple evaluator replicas must safely claim different jobs.

Use:

```sql
SELECT result_id
FROM nccl_validation.evaluation_job
WHERE status IN ('PENDING', 'RETRY')
    AND next_attempt_at <= now()
ORDER BY created_at, result_id
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;

-- For each locked result, generate a different UUID in the application:
UPDATE nccl_validation.evaluation_job
SET
    status = 'PROCESSING',
    claimed_by = :worker_id,
    claimed_at = now(),
    claim_token = :application_generated_uuid,
    attempt_count = attempt_count + 1
WHERE result_id = :result_id
RETURNING result_id, attempt_count, claim_token;
```

Commit the claim immediately.

Do not hold database row locks while running long calculations.
Every later load, completion, waiting/failure transition, and retry must match
the exact `claimed_by`, `attempt_count`, and `claim_token`. Recovery clears the
token; reclaim always assigns a new application-generated UUID, even when the
worker ID is reused.

## 12. Evaluation logic

For each claimed result:

1. Read the raw node result and test-run metadata.
2. Resolve the matching profile.
3. Fetch the profile’s active baseline version.
4. If no active baseline exists:
   - Set job to `WAITING_FOR_BASELINE`.
   - Do not create an evaluation row.
5. If raw `result_status != SUCCESS`:
   - Store `failure_code`.
   - Assign class 5 only if product requirements explicitly map the failure to critical health.
   - Otherwise mark the job `FAILED` and retain the raw failure state.
6. Classify BUS_BW.
7. Classify LATENCY.
8. Calculate severity percentiles.
9. Calculate overall class and overall severity.
10. Insert the evaluation and complete the job in one transaction.

### Metric classification

BUS_BW is higher-is-better.

Latency is lower-is-better.

Class lookup:

```sql
SELECT class_id
FROM nccl_baseline.metric_threshold
WHERE baseline_version_id = :baseline_version_id
  AND metric_name = :metric_name
  AND :value >= lower_bound
  AND (upper_bound IS NULL OR :value < upper_bound);
```

Exactly one threshold row must match.

### Severity percentiles

Use normalized severity percentiles where:

```text
0   = very healthy
50  = near baseline median
100 = very unhealthy
```

BUS_BW:

```text
higher BUS_BW → lower severity percentile
lower BUS_BW  → higher severity percentile
```

Latency:

```text
lower latency  → lower severity percentile
higher latency → higher severity percentile
```

Overall:

```text
overall_severity_percentile = GREATEST(
    bus_bw_severity_percentile,
    latency_severity_percentile
)
```

Do not average percentiles because averaging can hide a critical metric.

### Evaluation scope

Use:

- `IN_SAMPLE`: result was used to build the baseline version.
- `OUT_OF_SAMPLE`: new production result evaluated against an existing baseline.
- `REEVALUATION`: historical result intentionally evaluated against another baseline version.

The first 40 calibration results should not be represented as independent production validation. They may be backfilled as `IN_SAMPLE`.

## 13. Evaluation completion transaction

The evaluation insert and job completion must be atomic.

```sql
BEGIN;

INSERT INTO nccl_validation.evaluation (...)
VALUES (...)
ON CONFLICT (result_id, baseline_version_id)
DO NOTHING;

-- On conflict, read and compare every immutable evaluation value exactly.

UPDATE nccl_validation.evaluation_job
SET
    status = 'COMPLETED',
    completed_at = now(),
    claimed_by = NULL,
    claimed_at = NULL,
        claim_token = NULL,
    last_error = NULL
WHERE result_id = :result_id
    AND status = 'PROCESSING'
    AND claimed_by = :worker_id
    AND attempt_count = :attempt_count
    AND claim_token = :claim_token;

COMMIT;
```

Require exactly one job row to update. The evaluation insert and claim
completion roll back together on a stale receipt; immutable conflicts must
match the existing evaluation exactly.

## 14. Retry and stale claim handling

If evaluation fails temporarily:

```text
status = RETRY
claimed_by = NULL
claimed_at = NULL
claim_token = NULL
last_error = error text
next_attempt_at = backoff time
```

Use exponential backoff with a maximum delay.

Example:

```text
attempt 1 → 2 seconds
attempt 2 → 4 seconds
attempt 3 → 8 seconds
...
maximum → 5 minutes
```

After a configured maximum attempt count, set:

```text
status = FAILED
```

Recover abandoned processing jobs:

```sql
UPDATE nccl_validation.evaluation_job
SET
    status = 'RETRY',
    claimed_by = NULL,
    claimed_at = NULL,
    claim_token = NULL,
    next_attempt_at = now(),
    last_error = 'Worker claim expired'
WHERE status = 'PROCESSING'
  AND claimed_at < now() - INTERVAL '5 minutes';
```

## 15. Read views

Create a view that exposes the final readable result without duplicating data:

```text
nccl_validation.latest_result_view
```

The view should include:

- Node name.
- Test timestamp.
- LA timestamp.
- Test-run metadata.
- Profile ID.
- Profile key.
- Baseline version ID.
- Baseline version number.
- Baseline activation timestamp.
- BUS_BW.
- BUS_BW class ID and label.
- BUS_BW severity percentile.
- Latency.
- Latency class ID and label.
- Latency severity percentile.
- Overall class ID and label.
- Overall severity percentile.
- Evaluation scope.
- Evaluation timestamp.
- Failure code.
- Explanation.

Also create a raw-result status view exposing a derived Boolean:

```text
classified = evaluation_job.status = 'COMPLETED'
```

Do not persist this Boolean in `node_result`.

## 16. Configuration

Use application configuration or environment variables for:

```text
DATABASE_URL
EVALUATOR_BATCH_SIZE
EVALUATOR_POLL_INTERVAL_SECONDS
EVALUATOR_MAX_ATTEMPTS
EVALUATOR_STALE_CLAIM_SECONDS
BASELINE_MINIMUM_RESULTS=40
BASELINE_UPDATE_INCREMENT=10
BASELINE_BUILDER_INTERVAL_SECONDS
EVALUATOR_VERSION
DERIVATION_METHOD_VERSION
```

Do not hard-code database credentials.

## 17. Logging and observability

Use structured logs containing:

- `run_id`
- `result_id`
- `node_name`
- `profile_id`
- `baseline_version_id`
- `worker_id`
- `job_status`
- `attempt_count`
- `bus_bw_class`
- `latency_class`
- `overall_health_class`
- Duration
- Error code

Expose metrics such as:

```text
nccl_results_ingested_total
nccl_evaluation_jobs_pending
nccl_evaluation_jobs_waiting_for_baseline
nccl_evaluation_jobs_processing
nccl_evaluation_jobs_failed
nccl_evaluations_completed_total
nccl_baseline_profiles_collecting
nccl_baseline_versions_created_total
nccl_evaluation_duration_seconds
nccl_baseline_build_duration_seconds
```

## 18. Suggested application modules

```text
src/
├── config/
├── db/
│   ├── migrations/
│   ├── models/
│   └── repositories/
├── ingestion/
│   ├── test_run_ingester
│   ├── node_result_ingester
│   └── nic_result_ingester
├── baseline/
│   ├── profile_matcher
│   ├── eligibility_filter
│   ├── baseline_builder
│   ├── threshold_builder
│   └── percentile_model
├── evaluation/
│   ├── job_claimer
│   ├── classifier
│   ├── evaluator_worker
│   └── retry_handler
├── views/
├── telemetry/
└── tests/
```

The implementation language may be Python or Go. Prefer:

- SQLAlchemy/psycopg for Python.
- pgx/sqlc for Go.
- Alembic, Goose, or another migration framework.
- Explicit transactions.
- Parameterized SQL.
- Bounded connection pools.

## 19. Migration from the current SQLite-style table

Current columns resemble:

```text
Node
timestamp
la_timestamp
iterations
image_name
cuda
pytorch
samples
BUS_BW
LATENCY
mlx5_0 ... mlx5_13
```

Migration mapping:

```text
Shared run values
    → nccl_raw.test_run

Per-node values
    → nccl_raw.node_result

mlx5_0 ... mlx5_13
    → nccl_raw.nic_result rows
```

For each legacy row:

1. Create or reuse a deterministic `run_id`.
2. Insert or reuse a `test_run`.
3. Insert one `node_result`.
4. Convert each non-null `mlx5_x` value into a `nic_result` row.
5. Create an `evaluation_job`.
6. Do not mark old rows as classified until an evaluation is successfully committed.

The historical workload writes `LATENCY = duration * 1000` in milliseconds.
Legacy conversion therefore multiplies every valid value by 1000 before
writing canonical `latency_us`: legacy `628.2` becomes `628200.0 us`. The
material test config records `latency_unit = "us"`,
`latency_source_unit = "ms"`, and
`latency_conversion = "ms_to_us_x1000"`. CUDA and PyTorch metadata are
fallback-only: a nonblank row value is always preserved, while blank/null rows
use the explicit fallback.

## 20. Database ownership and destructive guards

The runtime/worker database role must not own any database, NCCL schema, or
relation. A reused role is accepted only after checking that it has no
superuser, create-database, create-role, replication, or `BYPASSRLS` attribute
and no memberships. Unsafe reuse fails before password rotation or grants; the
provisioner never silently removes ownership or memberships. Both create and
safe rotation explicitly apply `NOBYPASSRLS`.
Create schemas and migrations with a dedicated migration owner, then grant the
runtime role only the required schema usage, table DML, and sequence usage.
Do not grant `CREATE`, ownership, trigger bypass, or schema drop privileges to
the runtime role. For example, adapt these grants to deployment-managed role
names:

```sql
GRANT USAGE ON SCHEMA nccl_raw, nccl_baseline, nccl_validation TO cval_nccl_runtime;
GRANT SELECT, INSERT ON nccl_raw.test_run, nccl_raw.node_result,
    nccl_raw.nic_result, nccl_raw.outbox_receipt TO cval_nccl_runtime;
GRANT SELECT, UPDATE ON nccl_raw.outbox_scan_cursor TO cval_nccl_runtime;
GRANT SELECT ON nccl_baseline.calibration_decision TO cval_nccl_runtime;
REVOKE INSERT ON nccl_baseline.calibration_decision FROM cval_nccl_runtime;
GRANT EXECUTE ON FUNCTION nccl_baseline.apply_calibration_decision(
    UUID, BIGINT, TEXT, TEXT, TEXT, JSONB
) TO cval_nccl_runtime;
```

Append-only/immutable triggers remain authoritative. `BEFORE TRUNCATE` guards
cover the migration ledger and every raw, baseline, queue, lookup, and
evaluation table. Disposable test cleanup is allowed only for database names
matching `cval_test_[a-z0-9_]+` and requires the exact cleanup confirmation
token.

## 21. Required tests

### Database tests

- Foreign-key enforcement.
- Unique `(run_id, node_name)`.
- Unique `(profile_id, version_number)`.
- Unique `(result_id, baseline_version_id)`.
- Threshold ranges do not overlap.
- Threshold ranges are contiguous.
- Exactly five thresholds exist per metric/version.
- Overall class equals the worse metric class.

### Baseline tests

- No baseline before 40 eligible results.
- Version 1 created at 40.
- No new version at 41–49.
- Version 2 created at 50.
- Previous version becomes `SUPERSEDED`.
- Historical evaluations remain linked to old versions.
- Two concurrent builders create only one version.

### Evaluator tests

- Multiple workers do not claim the same job.
- Missing baseline produces `WAITING_FOR_BASELINE`.
- Completed evaluation updates job in the same transaction.
- Retry does not create a duplicate evaluation.
- Stale processing jobs are recovered.
- BUS_BW exact boundary values map to one class.
- Latency exact boundary values map to one class.
- Overall class uses the worse metric.
- Overall percentile uses the worse percentile.

### Ingestion tests

- Re-ingesting the same run/node is idempotent.
- Multiple node rows can be inserted in one transaction.
- Failure during ingestion rolls back raw rows and evaluation jobs together.
- NIC data is normalized correctly.

## 22. Acceptance criteria

The implementation is complete when:

1. PostgreSQL migrations create all schemas, tables, constraints, indexes, and views.
2. A batch NCCL result can be ingested atomically.
3. Each node result creates one evaluation job.
4. A new profile is created automatically for a new compatible environment.
5. Profiles remain `COLLECTING` until 40 eligible results exist.
6. Version 1 is built at 40 eligible results.
7. New immutable versions are built every additional 10 eligible results.
8. Waiting jobs become pending after baseline activation.
9. Multiple evaluator replicas safely process jobs concurrently.
10. BUS_BW and LATENCY receive classes 1–5.
11. Overall class and severity use the worse metric.
12. Evaluations store the exact baseline version used.
13. Retries are idempotent.
14. Raw data and historical evaluations are never overwritten.
15. The readable validation view returns all expected node, baseline, metric, class, percentile, and status fields.
16. Unit, integration, concurrency, and migration tests pass.

## 23. Example end-to-end result

```text
Raw result
  result_id: 1001
  node: slc01-cl02-hgx-0101
  BUS_BW: 44.5172 GB/s
  LATENCY: 628.9703 us

Matched profile
  profile_id: PROF-B200-C132-PT212-V1

Active baseline
  baseline_version_id: BASE-B200-003
  version_number: 3
  sample_count: 60

Classification
  BUS_BW class: 2
  BUS_BW severity percentile: 37
  LATENCY class: 2
  LATENCY severity percentile: 46

Final
  overall_health_class: 2
  overall_severity_percentile: 46
  evaluation_scope: OUT_OF_SAMPLE
  evaluation_job.status: COMPLETED
```

## 24. Implementation status and field mapping

Implemented as the self-contained optional `cval.nccl_eval` package. The base
package has no Psycopg import; PostgreSQL support is installed through the
`postgresql` project extra. Production-preparation manifests fail closed with
zero PostgreSQL and evaluator replicas. NCCL images are reviewed digest pins;
Git refs and the dedicated RWO storage class remain placeholders. The
Python 3.12 lock includes exact hashes for all transitive/bootstrap versions,
including setuptools, and has passed a clean bootstrap in the pinned image.
Validation jobs never receive PostgreSQL credentials. They create immutable
native `pending/<run>.json` before compatibility SQLite writes and an immutable
`committed/<run>.json` marker after all writes succeed. The non-root NCCL
process in the resident evaluator records durable INGESTED/REJECTED receipts,
builds due baselines, recovers stale claims, evaluates the queue, and never
deletes outbox files. See `docs/evals/nccl-rollout.md`.

CLI operations provide nonwriting inspection. Exact apply confirmations include `schema`,
`grant-runtime`, `ingest`, `emit-outbox`, `commit-outbox`, `ingest-outbox`,
`migrate-legacy`, `calibration`, `build-baselines`, `evaluate`, `worker`,
`resident`, and `recover`.
Confirmation is checked before `DATABASE_URL` is read or a connection pool is
created. `status` and inspection reports are read-only. Pools use explicit
open/wait/close and bounded sizes; repository operations use explicit
transactions.

Normalized JSON maps as follows:

| JSON input | PostgreSQL destination |
|---|---|
| `test_run.run_id` and execution timestamps | `nccl_raw.test_run` identity/timestamps |
| test name and test-definition version | raw run plus baseline profile identity |
| CUDA, PyTorch, compiled NCCL, runtime NCCL package, driver version | raw run evidence |
| GPU model/count, driver compatibility group, topology class | raw run plus baseline profile identity |
| collective, datatype, reduction, message size, iterations, nullable samples, warmup, latency unit, canonical config | `test_config` plus type-aware SHA-256 run/profile fingerprint |
| each `node_results[]` item | one immutable `nccl_raw.node_result` and one durable evaluation job |
| each `nics[]` item | one normalized `nccl_raw.nic_result` row |
| latest append-only `calibration_decision` | calibration eligibility; no decision is excluded |

The NCCL descriptor supplies only material test constants. GPU model,
CUDA/PyTorch/compiled NCCL/runtime NCCL package versions, driver compatibility group, and topology class must
come from runtime evidence in the ingestion payload. The descriptor's driver
and topology source values are `runtime_evidence`; no live hardware versions
are hard-coded.

Legacy conversion opens a copied `IB_HEALTH` SQLite DB with `mode=ro`, groups
compatible rows into deterministic UUID runs, and normalizes non-null `mlx5_*`
columns into NIC rows. Metadata absent from the wide table—test definition,
GPU model/count, NCCL and driver compatibility, topology, and canonical test
configuration and explicit legacy provenance sentinels—is required. Imported
rows have no calibration decision and remain excluded until an append-only
APPROVE event is applied. Nonblank CUDA/PyTorch row facts are
preserved; configured versions fill only blank/null cells. Legacy milliseconds
are converted to microseconds with explicit source/conversion evidence.

The migration creates only `nccl_raw`, `nccl_baseline`, and
`nccl_validation` inside the connected `cval` database. It includes a checksum
ledger, fixed class seed, FKs/checks/uniques/indexes/views, append-only raw
guards, immutable baseline transition guards, exact sample lineage, and a
deferred five-range semantic coverage constraint. Activation also validates
both metric sets, included lineage count, supersession, the sole-active-version
index, and profile pointer consistency. Baseline builds serialize per profile
with `pg_advisory_xact_lock`; migration ledger inspection uses its own
transaction advisory lock. Queue claims use short `FOR UPDATE SKIP LOCKED`
transactions and per-row application UUID fencing tokens.
Calibration inserts serialize per result and enforce contiguous alternating
actions in SQL. Runtime callers execute only the security-definer calibration
function; reused runtime roles are rejected if they own database-local objects,
hold memberships/default privileges, or have direct ACLs outside the exact
runtime allowlist. Included-sample revocation atomically fails the active
baseline, clears the profile pointer, and parks ready/retry jobs while
preserving history.
The exact unresolved live prerequisites are maintained in
`docs/evals/nccl-rollout.md`.
