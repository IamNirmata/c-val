-- cval NCCL PostgreSQL evaluator: initial schemas, constraints, queue, and views.
CREATE SCHEMA IF NOT EXISTS nccl_raw;
CREATE SCHEMA IF NOT EXISTS nccl_baseline;
CREATE SCHEMA IF NOT EXISTS nccl_validation;

CREATE TABLE IF NOT EXISTS nccl_raw.schema_migration (
    migration_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nccl_raw.test_run (
    run_id UUID PRIMARY KEY,
    test_name TEXT NOT NULL CHECK (length(test_name) BETWEEN 1 AND 256),
    test_definition_version TEXT NOT NULL CHECK (length(test_definition_version) BETWEEN 1 AND 256),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    image_name TEXT,
    image_digest TEXT NOT NULL,
    cuda_version TEXT NOT NULL CHECK (cuda_version <> ''),
    pytorch_version TEXT NOT NULL CHECK (pytorch_version <> ''),
    compiled_nccl_version TEXT NOT NULL CHECK (compiled_nccl_version <> ''),
    runtime_nccl_package_version TEXT NOT NULL CHECK (runtime_nccl_package_version <> ''),
    driver_version TEXT NOT NULL CHECK (driver_version <> ''),
    driver_version_group TEXT NOT NULL CHECK (driver_version_group <> ''),
    topology_class TEXT NOT NULL CHECK (topology_class <> ''),
    gpu_model TEXT NOT NULL CHECK (gpu_model <> ''),
    gpus_per_node SMALLINT NOT NULL CHECK (gpus_per_node > 0),
    iterations INTEGER NOT NULL CHECK (iterations > 0),
    samples INTEGER CHECK (samples >= 0),
    test_config_fingerprint TEXT NOT NULL
        CHECK (test_config_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    test_config JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(test_config) = 'object')
        CHECK (test_config ->> 'latency_unit' = 'us'),
    cval_run_id TEXT NOT NULL CHECK (cval_run_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$'),
    cval_result_digest TEXT NOT NULL,
    summary_sha256 TEXT,
    runtime_evidence_sha256 TEXT NOT NULL,
    source_commit TEXT NOT NULL,
    implementation_identity TEXT NOT NULL,
    legacy_source BOOLEAN NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (
        (legacy_source AND cval_result_digest ~ '^legacy:'
         AND runtime_evidence_sha256 ~ '^legacy:'
         AND (summary_sha256 IS NULL OR summary_sha256 ~ '^legacy:')
         AND source_commit ~ '^legacy:' AND image_digest ~ '^legacy:'
         AND implementation_identity ~ '^legacy:')
        OR
        (NOT legacy_source
         AND cval_result_digest ~ '^sha256:[0-9a-f]{64}$'
         AND runtime_evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'
         AND (summary_sha256 IS NULL OR summary_sha256 ~ '^sha256:[0-9a-f]{64}$')
         AND source_commit ~ '^[0-9a-f]{40}$'
         AND image_digest ~ '^sha256:[0-9a-f]{64}$'
         AND implementation_identity ~ '^sha256:[0-9a-f]{64}$')
    )
);

CREATE TABLE IF NOT EXISTS nccl_raw.node_result (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES nccl_raw.test_run(run_id) ON DELETE CASCADE,
    node_name TEXT NOT NULL CHECK (length(node_name) BETWEEN 1 AND 253),
    test_timestamp TIMESTAMPTZ NOT NULL,
    la_timestamp TIMESTAMPTZ,
    bus_bw_gbps DOUBLE PRECISION,
    latency_us DOUBLE PRECISION,
    result_status TEXT NOT NULL DEFAULT 'SUCCESS'
        CHECK (result_status IN ('SUCCESS', 'TIMEOUT', 'TEST_ERROR', 'NO_RESULT')),
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, node_name),
    CHECK (bus_bw_gbps IS NULL OR (bus_bw_gbps >= 0 AND bus_bw_gbps < 'Infinity'::double precision)),
    CHECK (latency_us IS NULL OR (latency_us >= 0 AND latency_us < 'Infinity'::double precision)),
    CHECK (
        result_status <> 'SUCCESS' OR
        (bus_bw_gbps IS NOT NULL AND latency_us IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS nccl_raw.nic_result (
    result_id BIGINT NOT NULL REFERENCES nccl_raw.node_result(result_id) ON DELETE CASCADE,
    device_name TEXT NOT NULL CHECK (device_name ~ '^mlx5_[0-9]+(\.[0-9]+)?$'),
    max_bus_bw_gbps DOUBLE PRECISION,
    PRIMARY KEY (result_id, device_name),
    CHECK (
        max_bus_bw_gbps IS NULL OR
        (max_bus_bw_gbps >= 0 AND max_bus_bw_gbps < 'Infinity'::double precision)
    )
);

CREATE TABLE IF NOT EXISTS nccl_baseline.baseline_profile (
    profile_id UUID PRIMARY KEY,
    profile_key TEXT NOT NULL UNIQUE CHECK (length(profile_key) BETWEEN 1 AND 255),
    test_name TEXT NOT NULL,
    test_definition_version TEXT NOT NULL,
    gpu_model TEXT NOT NULL,
    gpus_per_node SMALLINT NOT NULL CHECK (gpus_per_node > 0),
    cuda_version TEXT NOT NULL,
    pytorch_version TEXT NOT NULL,
    compiled_nccl_version TEXT NOT NULL,
    runtime_nccl_package_version TEXT NOT NULL,
    driver_version_group TEXT NOT NULL,
    topology_class TEXT NOT NULL,
    source_commit TEXT NOT NULL,
    image_digest TEXT NOT NULL,
    implementation_identity TEXT NOT NULL,
    test_config_fingerprint TEXT NOT NULL
        CHECK (test_config_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    test_config JSONB NOT NULL
        CHECK (jsonb_typeof(test_config) = 'object')
        CHECK (test_config ->> 'latency_unit' = 'us'),
    status TEXT NOT NULL CHECK (status IN ('COLLECTING', 'ACTIVE', 'DISABLED')),
    eligible_result_count INTEGER NOT NULL DEFAULT 0 CHECK (eligible_result_count >= 0),
    last_built_sample_count INTEGER NOT NULL DEFAULT 0 CHECK (last_built_sample_count >= 0),
    active_baseline_version_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nccl_raw.outbox_receipt (
    outbox_name TEXT PRIMARY KEY
        CHECK (length(outbox_name) BETWEEN 1 AND 255 AND right(outbox_name, 5) = '.json'),
    status TEXT NOT NULL CHECK (status IN ('INGESTED', 'REJECTED')),
    content_sha256 TEXT CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    run_id UUID REFERENCES nccl_raw.test_run(run_id),
    profile_id UUID REFERENCES nccl_baseline.baseline_profile(profile_id),
    safe_error TEXT CHECK (safe_error IS NULL OR length(safe_error) BETWEEN 1 AND 1000),
    observed_fingerprint TEXT NOT NULL CHECK (length(observed_fingerprint) BETWEEN 1 AND 256),
    terminal_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (status = 'INGESTED' AND content_sha256 IS NOT NULL AND run_id IS NOT NULL
         AND profile_id IS NOT NULL AND safe_error IS NULL)
        OR
        (status = 'REJECTED' AND run_id IS NULL AND profile_id IS NULL
         AND safe_error IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS outbox_ingested_run_unique_idx
ON nccl_raw.outbox_receipt(run_id) WHERE status = 'INGESTED';

CREATE TABLE IF NOT EXISTS nccl_raw.outbox_scan_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    outbox_name TEXT CHECK (
        outbox_name IS NULL OR
        (length(outbox_name) BETWEEN 1 AND 255 AND right(outbox_name, 5) = '.json')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO nccl_raw.outbox_scan_cursor(singleton, outbox_name)
VALUES (TRUE, NULL) ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS nccl_baseline.calibration_decision (
    decision_id UUID PRIMARY KEY,
    result_id BIGINT NOT NULL REFERENCES nccl_raw.node_result(result_id),
    decision_version BIGINT NOT NULL CHECK (decision_version > 0),
    action TEXT NOT NULL CHECK (action IN ('APPROVE', 'REVOKE')),
    actor TEXT NOT NULL CHECK (actor ~ '^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$'),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1000),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (result_id, decision_version)
);

CREATE TABLE IF NOT EXISTS nccl_baseline.baseline_version (
    baseline_version_id UUID PRIMARY KEY,
    profile_id UUID NOT NULL REFERENCES nccl_baseline.baseline_profile(profile_id),
    version_number INTEGER NOT NULL CHECK (version_number > 0),
    status TEXT NOT NULL CHECK (status IN ('BUILDING', 'ACTIVE', 'SUPERSEDED', 'FAILED')),
    sample_count INTEGER NOT NULL CHECK (sample_count >= 40),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    failure_reason TEXT CHECK (
        failure_reason IS NULL OR length(failure_reason) BETWEEN 1 AND 1000
    ),
    supersedes_version_id UUID REFERENCES nccl_baseline.baseline_version(baseline_version_id),
    derivation_method_version TEXT NOT NULL CHECK (derivation_method_version <> ''),
    bus_bw_mean DOUBLE PRECISION NOT NULL,
    bus_bw_p05 DOUBLE PRECISION NOT NULL,
    bus_bw_p50 DOUBLE PRECISION NOT NULL,
    bus_bw_p95 DOUBLE PRECISION NOT NULL,
    latency_mean DOUBLE PRECISION NOT NULL,
    latency_p05 DOUBLE PRECISION NOT NULL,
    latency_p50 DOUBLE PRECISION NOT NULL,
    latency_p95 DOUBLE PRECISION NOT NULL,
    UNIQUE (profile_id, version_number),
    CHECK (activated_at IS NULL OR activated_at >= created_at),
    CHECK (bus_bw_mean >= 0 AND bus_bw_mean < 'Infinity'::double precision),
    CHECK (bus_bw_p05 >= 0 AND bus_bw_p05 <= bus_bw_p50 AND bus_bw_p50 <= bus_bw_p95),
    CHECK (bus_bw_p95 < 'Infinity'::double precision),
    CHECK (latency_mean >= 0 AND latency_mean < 'Infinity'::double precision),
    CHECK (latency_p05 >= 0 AND latency_p05 <= latency_p50 AND latency_p50 <= latency_p95),
    CHECK (latency_p95 < 'Infinity'::double precision),
    CHECK (
        (status = 'ACTIVE' AND activated_at IS NOT NULL) OR
        (status <> 'ACTIVE')
    ),
    CHECK (
        (status = 'FAILED' AND failure_reason IS NOT NULL) OR
        (status <> 'FAILED' AND failure_reason IS NULL)
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'baseline_profile_active_version_fk'
          AND conrelid = 'nccl_baseline.baseline_profile'::regclass
    ) THEN
        ALTER TABLE nccl_baseline.baseline_profile
        ADD CONSTRAINT baseline_profile_active_version_fk
        FOREIGN KEY (active_baseline_version_id)
        REFERENCES nccl_baseline.baseline_version(baseline_version_id);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS nccl_baseline.baseline_version_sample (
    baseline_version_id UUID NOT NULL
        REFERENCES nccl_baseline.baseline_version(baseline_version_id) ON DELETE CASCADE,
    result_id BIGINT NOT NULL REFERENCES nccl_raw.node_result(result_id),
    included BOOLEAN NOT NULL,
    exclusion_reason TEXT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (baseline_version_id, result_id),
    CHECK (
        (included AND exclusion_reason IS NULL) OR
        (NOT included AND exclusion_reason IS NOT NULL AND exclusion_reason <> '')
    )
);

CREATE TABLE IF NOT EXISTS nccl_validation.health_class (
    class_id SMALLINT PRIMARY KEY CHECK (class_id BETWEEN 1 AND 5),
    class_code TEXT NOT NULL UNIQUE,
    class_label TEXT NOT NULL,
    description TEXT NOT NULL
);

DROP TRIGGER IF EXISTS health_class_immutable ON nccl_validation.health_class;
INSERT INTO nccl_validation.health_class (class_id, class_code, class_label, description)
VALUES
    (1, 'EXCEEDING', 'Exceeding baseline', 'Performance exceeds the healthy baseline range.'),
    (2, 'WITHIN', 'Within baseline', 'Performance is within the expected baseline range.'),
    (3, 'UNDERPERFORMING', 'Underperforming', 'Performance is below the expected healthy range.'),
    (4, 'DEGRADED', 'Degraded', 'Performance is materially degraded.'),
    (5, 'CRITICAL', 'Critical / suspected hardware failure', 'Performance is critically unhealthy.')
ON CONFLICT (class_id) DO UPDATE SET
    class_code = EXCLUDED.class_code,
    class_label = EXCLUDED.class_label,
    description = EXCLUDED.description;

CREATE TABLE IF NOT EXISTS nccl_baseline.metric_threshold (
    baseline_version_id UUID NOT NULL
        REFERENCES nccl_baseline.baseline_version(baseline_version_id) ON DELETE CASCADE,
    metric_name TEXT NOT NULL CHECK (metric_name IN ('BUS_BW', 'LATENCY')),
    class_id SMALLINT NOT NULL REFERENCES nccl_validation.health_class(class_id),
    lower_bound DOUBLE PRECISION NOT NULL,
    upper_bound DOUBLE PRECISION,
    unit TEXT NOT NULL CHECK (unit <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (baseline_version_id, metric_name, class_id),
    CHECK (lower_bound >= 0 AND lower_bound < 'Infinity'::double precision),
    CHECK (
        upper_bound IS NULL OR
        (upper_bound < 'Infinity'::double precision AND lower_bound < upper_bound)
    ),
    CHECK (
        (metric_name = 'BUS_BW' AND unit = 'GB/s') OR
        (metric_name = 'LATENCY' AND unit = 'us')
    )
);

CREATE TABLE IF NOT EXISTS nccl_validation.evaluation_job (
    result_id BIGINT PRIMARY KEY REFERENCES nccl_raw.node_result(result_id) ON DELETE CASCADE,
    profile_id UUID NOT NULL REFERENCES nccl_baseline.baseline_profile(profile_id),
    status TEXT NOT NULL
        CHECK (status IN ('PENDING', 'WAITING_FOR_BASELINE', 'PROCESSING', 'RETRY', 'COMPLETED', 'FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    claim_token UUID,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (status = 'PROCESSING' AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL
         AND claim_token IS NOT NULL) OR
        (status <> 'PROCESSING' AND claimed_by IS NULL AND claimed_at IS NULL
         AND claim_token IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS nccl_validation.evaluation (
    evaluation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    result_id BIGINT NOT NULL REFERENCES nccl_raw.node_result(result_id),
    baseline_version_id UUID NOT NULL
        REFERENCES nccl_baseline.baseline_version(baseline_version_id),
    evaluation_scope TEXT NOT NULL
        CHECK (evaluation_scope IN ('OUT_OF_SAMPLE', 'IN_SAMPLE', 'REEVALUATION')),
    bus_bw_class SMALLINT NOT NULL REFERENCES nccl_validation.health_class(class_id),
    bus_bw_severity_percentile DOUBLE PRECISION NOT NULL
        CHECK (bus_bw_severity_percentile BETWEEN 0 AND 100),
    latency_class SMALLINT NOT NULL REFERENCES nccl_validation.health_class(class_id),
    latency_severity_percentile DOUBLE PRECISION NOT NULL
        CHECK (latency_severity_percentile BETWEEN 0 AND 100),
    overall_health_class SMALLINT NOT NULL REFERENCES nccl_validation.health_class(class_id),
    overall_severity_percentile DOUBLE PRECISION NOT NULL
        CHECK (overall_severity_percentile BETWEEN 0 AND 100),
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    evaluator_version TEXT NOT NULL CHECK (evaluator_version <> ''),
    failure_code TEXT,
    explanation TEXT,
    UNIQUE (result_id, baseline_version_id),
    CONSTRAINT evaluation_overall_class_is_worst CHECK (
        overall_health_class = GREATEST(bus_bw_class, latency_class)
    ),
    CONSTRAINT evaluation_overall_severity_is_worst CHECK (
        overall_severity_percentile = GREATEST(
            bus_bw_severity_percentile,
            latency_severity_percentile
        )
    )
);

CREATE INDEX IF NOT EXISTS node_result_node_time_idx
ON nccl_raw.node_result(node_name, test_timestamp DESC);
CREATE INDEX IF NOT EXISTS node_result_created_at_idx
ON nccl_raw.node_result(created_at);
CREATE INDEX IF NOT EXISTS baseline_profile_lookup_idx
ON nccl_baseline.baseline_profile(
    test_name, test_definition_version, gpu_model, gpus_per_node,
    cuda_version, pytorch_version, compiled_nccl_version,
    runtime_nccl_package_version, driver_version_group, topology_class,
    source_commit, image_digest, implementation_identity, test_config_fingerprint
);
CREATE INDEX IF NOT EXISTS evaluation_job_pending_idx
ON nccl_validation.evaluation_job(next_attempt_at, created_at)
WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX IF NOT EXISTS evaluation_job_waiting_idx
ON nccl_validation.evaluation_job(profile_id)
WHERE status = 'WAITING_FOR_BASELINE';
CREATE INDEX IF NOT EXISTS evaluation_result_idx
ON nccl_validation.evaluation(result_id);
CREATE INDEX IF NOT EXISTS evaluation_health_time_idx
ON nccl_validation.evaluation(overall_health_class, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS baseline_sample_result_idx
ON nccl_baseline.baseline_version_sample(result_id);
CREATE UNIQUE INDEX IF NOT EXISTS baseline_one_active_per_profile_idx
ON nccl_baseline.baseline_version(profile_id)
WHERE status = 'ACTIVE';

CREATE OR REPLACE FUNCTION nccl_raw.reject_immutable_raw_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NCCL raw rows are append-only';
END
$$;

DROP TRIGGER IF EXISTS schema_migration_immutable ON nccl_raw.schema_migration;
CREATE TRIGGER schema_migration_immutable
BEFORE UPDATE OR DELETE ON nccl_raw.schema_migration
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();

DROP TRIGGER IF EXISTS test_run_immutable ON nccl_raw.test_run;
CREATE TRIGGER test_run_immutable
BEFORE UPDATE OR DELETE ON nccl_raw.test_run
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();
DROP TRIGGER IF EXISTS outbox_receipt_immutable ON nccl_raw.outbox_receipt;
CREATE TRIGGER outbox_receipt_immutable
BEFORE UPDATE OR DELETE ON nccl_raw.outbox_receipt
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();
CREATE OR REPLACE FUNCTION nccl_raw.guard_outbox_scan_cursor()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW.singleton IS DISTINCT FROM OLD.singleton THEN
        RAISE EXCEPTION 'NCCL outbox scan cursor singleton identity is protected';
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS outbox_scan_cursor_protected ON nccl_raw.outbox_scan_cursor;
CREATE TRIGGER outbox_scan_cursor_protected
BEFORE UPDATE OR DELETE ON nccl_raw.outbox_scan_cursor
FOR EACH ROW EXECUTE FUNCTION nccl_raw.guard_outbox_scan_cursor();
DROP TRIGGER IF EXISTS calibration_decision_immutable ON nccl_baseline.calibration_decision;
CREATE TRIGGER calibration_decision_immutable
BEFORE UPDATE OR DELETE ON nccl_baseline.calibration_decision
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();
CREATE OR REPLACE FUNCTION nccl_baseline.guard_calibration_decision_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    latest_version BIGINT;
    latest_action TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('nccl-calibration:' || NEW.result_id::text, 0)
    );
    IF EXISTS (
        SELECT 1 FROM nccl_baseline.calibration_decision
        WHERE decision_id = NEW.decision_id
    ) THEN
        RETURN NEW;
    END IF;
    SELECT decision_version, action
      INTO latest_version, latest_action
      FROM nccl_baseline.calibration_decision
     WHERE result_id = NEW.result_id
     ORDER BY decision_version DESC
     LIMIT 1;
    IF latest_version IS NULL THEN
        IF NEW.decision_version IS DISTINCT FROM 1
           OR NEW.action IS DISTINCT FROM 'APPROVE' THEN
            RAISE EXCEPTION 'first calibration decision must be version 1 APPROVE';
        END IF;
    ELSIF NEW.decision_version IS DISTINCT FROM latest_version + 1 THEN
        RAISE EXCEPTION 'calibration decision version must be latest plus one';
    ELSIF NEW.action IS NOT DISTINCT FROM latest_action THEN
        RAISE EXCEPTION 'calibration decisions must alternate effective action';
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS calibration_decision_sequence_guard
ON nccl_baseline.calibration_decision;
CREATE TRIGGER calibration_decision_sequence_guard
BEFORE INSERT ON nccl_baseline.calibration_decision
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.guard_calibration_decision_insert();
DROP TRIGGER IF EXISTS node_result_immutable ON nccl_raw.node_result;
CREATE TRIGGER node_result_immutable
BEFORE UPDATE OR DELETE ON nccl_raw.node_result
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();
DROP TRIGGER IF EXISTS nic_result_immutable ON nccl_raw.nic_result;
CREATE TRIGGER nic_result_immutable
BEFORE UPDATE OR DELETE ON nccl_raw.nic_result
FOR EACH ROW EXECUTE FUNCTION nccl_raw.reject_immutable_raw_mutation();

CREATE OR REPLACE FUNCTION nccl_baseline.guard_profile_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_profile_id UUID;
    target_status TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'baseline profiles cannot be deleted';
    END IF;
    IF ROW(
        NEW.profile_key, NEW.test_name, NEW.test_definition_version,
        NEW.gpu_model, NEW.gpus_per_node, NEW.cuda_version,
        NEW.pytorch_version, NEW.compiled_nccl_version,
        NEW.runtime_nccl_package_version, NEW.driver_version_group,
        NEW.topology_class, NEW.source_commit, NEW.image_digest,
        NEW.implementation_identity, NEW.test_config_fingerprint, NEW.test_config,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.profile_key, OLD.test_name, OLD.test_definition_version,
        OLD.gpu_model, OLD.gpus_per_node, OLD.cuda_version,
        OLD.pytorch_version, OLD.compiled_nccl_version,
        OLD.runtime_nccl_package_version, OLD.driver_version_group,
        OLD.topology_class, OLD.source_commit, OLD.image_digest,
        OLD.implementation_identity, OLD.test_config_fingerprint, OLD.test_config,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'baseline profile identity is immutable';
    END IF;
    IF NEW.last_built_sample_count < OLD.last_built_sample_count THEN
        RAISE EXCEPTION 'baseline profile build cursor cannot move backward';
    END IF;
    IF NEW.active_baseline_version_id IS NOT NULL THEN
        SELECT profile_id, status INTO target_profile_id, target_status
        FROM nccl_baseline.baseline_version
        WHERE baseline_version_id = NEW.active_baseline_version_id;
        IF target_profile_id IS DISTINCT FROM NEW.profile_id
           OR target_status IS DISTINCT FROM 'ACTIVE' THEN
            RAISE EXCEPTION 'profile active pointer must reference its own ACTIVE version';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS baseline_profile_identity_immutable ON nccl_baseline.baseline_profile;
CREATE TRIGGER baseline_profile_identity_immutable
BEFORE UPDATE OR DELETE ON nccl_baseline.baseline_profile
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.guard_profile_identity();

CREATE OR REPLACE FUNCTION nccl_baseline.guard_baseline_version()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    included_count INTEGER;
    threshold_count INTEGER;
    class_count INTEGER;
    first_lower DOUBLE PRECISION;
    valid_chain BOOLEAN;
    actual_order SMALLINT[];
    expected_order SMALLINT[];
    metric TEXT;
    prior_profile_id UUID;
    prior_version_number INTEGER;
    prior_status TEXT;
    latest_prior_version_number INTEGER;
    profile_active_version_id UUID;
    version_owner TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'baseline versions cannot be deleted';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status IS DISTINCT FROM 'BUILDING' OR NEW.activated_at IS NOT NULL THEN
            RAISE EXCEPTION 'baseline versions must be inserted in BUILDING state';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(
        NEW.baseline_version_id, NEW.profile_id, NEW.version_number,
        NEW.sample_count, NEW.created_at, NEW.supersedes_version_id,
        NEW.derivation_method_version, NEW.bus_bw_mean, NEW.bus_bw_p05,
        NEW.bus_bw_p50, NEW.bus_bw_p95, NEW.latency_mean,
        NEW.latency_p05, NEW.latency_p50, NEW.latency_p95
    ) IS DISTINCT FROM ROW(
        OLD.baseline_version_id, OLD.profile_id, OLD.version_number,
        OLD.sample_count, OLD.created_at, OLD.supersedes_version_id,
        OLD.derivation_method_version, OLD.bus_bw_mean, OLD.bus_bw_p05,
        OLD.bus_bw_p50, OLD.bus_bw_p95, OLD.latency_mean,
        OLD.latency_p05, OLD.latency_p50, OLD.latency_p95
    ) THEN
        RAISE EXCEPTION 'baseline version payload is immutable';
    END IF;
    IF OLD.status = 'BUILDING' AND NEW.status = 'ACTIVE' THEN
        IF NEW.activated_at IS NULL THEN
            RAISE EXCEPTION 'active baseline requires activated_at';
        END IF;
        SELECT count(*) FILTER (WHERE included)
          INTO included_count
          FROM nccl_baseline.baseline_version_sample
         WHERE baseline_version_id = NEW.baseline_version_id;
        IF included_count <> NEW.sample_count THEN
            RAISE EXCEPTION 'active baseline included lineage count must equal sample_count';
        END IF;
        FOREACH metric IN ARRAY ARRAY['BUS_BW', 'LATENCY'] LOOP
            expected_order := CASE metric
                WHEN 'BUS_BW' THEN ARRAY[5, 4, 3, 2, 1]::SMALLINT[]
                ELSE ARRAY[1, 2, 3, 4, 5]::SMALLINT[]
            END;
            SELECT count(*), count(DISTINCT class_id), min(lower_bound),
                   bool_and(
                       CASE WHEN next_lower IS NULL
                            THEN upper_bound IS NULL
                            ELSE upper_bound = next_lower
                       END
                   ),
                   array_agg(class_id ORDER BY lower_bound)
              INTO threshold_count, class_count, first_lower, valid_chain, actual_order
              FROM (
                  SELECT class_id, lower_bound, upper_bound,
                         lead(lower_bound) OVER (ORDER BY lower_bound) AS next_lower
                    FROM nccl_baseline.metric_threshold
                   WHERE baseline_version_id = NEW.baseline_version_id
                     AND metric_name = metric
              ) AS ordered_ranges;
            IF threshold_count <> 5 OR class_count <> 5 OR first_lower <> 0
               OR valid_chain IS NOT TRUE OR actual_order IS DISTINCT FROM expected_order THEN
                RAISE EXCEPTION 'active baseline requires complete semantic thresholds for %', metric;
            END IF;
        END LOOP;
                SELECT active_baseline_version_id
          INTO profile_active_version_id
          FROM nccl_baseline.baseline_profile
         WHERE profile_id = NEW.profile_id;
        IF NEW.version_number = 1 THEN
            IF NEW.supersedes_version_id IS NOT NULL THEN
                RAISE EXCEPTION 'baseline version 1 cannot supersede another version';
            END IF;
        ELSE
            IF NEW.supersedes_version_id IS NULL THEN
                RAISE EXCEPTION 'new active baseline must supersede a prior version';
            END IF;
            SELECT profile_id, version_number, status
              INTO prior_profile_id, prior_version_number, prior_status
              FROM nccl_baseline.baseline_version
             WHERE baseline_version_id = NEW.supersedes_version_id;
            IF prior_profile_id IS DISTINCT FROM NEW.profile_id
               OR prior_version_number IS NULL
               OR prior_version_number >= NEW.version_number THEN
                RAISE EXCEPTION 'superseded baseline must belong to the same profile and be older';
            END IF;
            IF profile_active_version_id IS NOT NULL THEN
                IF profile_active_version_id IS DISTINCT FROM NEW.supersedes_version_id
                   OR prior_status IS DISTINCT FROM 'SUPERSEDED' THEN
                    RAISE EXCEPTION 'new active baseline must supersede the profile active pointer';
                END IF;
            ELSE
                SELECT max(version_number)
                  INTO latest_prior_version_number
                  FROM nccl_baseline.baseline_version
                 WHERE profile_id = NEW.profile_id
                   AND version_number < NEW.version_number;
                IF prior_status IS DISTINCT FROM 'FAILED'
                   OR prior_version_number IS DISTINCT FROM latest_prior_version_number THEN
                    RAISE EXCEPTION 'replacement baseline must supersede the latest failed version';
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'BUILDING' AND NEW.status = 'FAILED'
       AND NEW.activated_at IS NULL AND NEW.failure_reason IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.status = 'ACTIVE' AND NEW.status = 'SUPERSEDED'
       AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
       AND NEW.failure_reason IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.status = 'ACTIVE' AND NEW.status = 'FAILED'
       AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
       AND NEW.failure_reason IS NOT NULL THEN
        SELECT pg_get_userbyid(relowner)
          INTO version_owner
          FROM pg_class
         WHERE oid = TG_RELID;
        IF current_user::text IS DISTINCT FROM version_owner
           OR current_setting('cval.calibration_decision', TRUE)
              IS DISTINCT FROM 'revoke' THEN
            RAISE EXCEPTION 'ACTIVE baseline failure requires controlled calibration revocation';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.status IS NOT DISTINCT FROM OLD.status
       AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
       AND NEW.failure_reason IS NOT DISTINCT FROM OLD.failure_reason THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid immutable baseline status transition: % to %', OLD.status, NEW.status;
END
$$;
DROP TRIGGER IF EXISTS baseline_version_immutable ON nccl_baseline.baseline_version;
CREATE TRIGGER baseline_version_immutable
BEFORE INSERT OR UPDATE OR DELETE ON nccl_baseline.baseline_version
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.guard_baseline_version();

CREATE OR REPLACE FUNCTION nccl_baseline.guard_building_child_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    old_parent_status TEXT;
    new_parent_status TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT status INTO new_parent_status
        FROM nccl_baseline.baseline_version
        WHERE baseline_version_id = NEW.baseline_version_id;
        IF new_parent_status IS DISTINCT FROM 'BUILDING' THEN
            RAISE EXCEPTION 'baseline samples and thresholds mutate only while BUILDING';
        END IF;
        RETURN NEW;
    END IF;

    SELECT status INTO old_parent_status
    FROM nccl_baseline.baseline_version
    WHERE baseline_version_id = OLD.baseline_version_id;
    IF TG_OP = 'DELETE' THEN
        IF old_parent_status IS DISTINCT FROM 'BUILDING' THEN
            RAISE EXCEPTION 'baseline samples and thresholds mutate only while BUILDING';
        END IF;
        RETURN OLD;
    END IF;

    SELECT status INTO new_parent_status
    FROM nccl_baseline.baseline_version
    WHERE baseline_version_id = NEW.baseline_version_id;
    IF old_parent_status IS DISTINCT FROM 'BUILDING'
       OR new_parent_status IS DISTINCT FROM 'BUILDING' THEN
        RAISE EXCEPTION 'baseline samples and thresholds mutate only while BUILDING';
    END IF;

    IF TG_TABLE_NAME = 'baseline_version_sample'
       AND ROW(NEW.baseline_version_id, NEW.result_id) IS DISTINCT FROM
           ROW(OLD.baseline_version_id, OLD.result_id) THEN
        RAISE EXCEPTION 'baseline sample key identity is immutable';
    END IF;
    IF TG_TABLE_NAME = 'metric_threshold'
       AND ROW(NEW.baseline_version_id, NEW.metric_name, NEW.class_id) IS DISTINCT FROM
           ROW(OLD.baseline_version_id, OLD.metric_name, OLD.class_id) THEN
        RAISE EXCEPTION 'metric threshold key identity is immutable';
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS baseline_sample_building_only ON nccl_baseline.baseline_version_sample;
CREATE TRIGGER baseline_sample_building_only
BEFORE INSERT OR UPDATE OR DELETE ON nccl_baseline.baseline_version_sample
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.guard_building_child_mutation();
DROP TRIGGER IF EXISTS metric_threshold_building_only ON nccl_baseline.metric_threshold;
CREATE TRIGGER metric_threshold_building_only
BEFORE INSERT OR UPDATE OR DELETE ON nccl_baseline.metric_threshold
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.guard_building_child_mutation();

CREATE OR REPLACE FUNCTION nccl_baseline.validate_metric_threshold_set()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_version UUID;
    target_metric TEXT;
    row_count INTEGER;
    class_count INTEGER;
    first_lower DOUBLE PRECISION;
    valid_chain BOOLEAN;
    actual_order SMALLINT[];
    expected_order SMALLINT[];
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_version := OLD.baseline_version_id;
        target_metric := OLD.metric_name;
    ELSE
        target_version := NEW.baseline_version_id;
        target_metric := NEW.metric_name;
    END IF;
        SELECT count(*), count(DISTINCT class_id), min(lower_bound),
           bool_and(
               CASE WHEN next_lower IS NULL
                    THEN upper_bound IS NULL
                    ELSE upper_bound = next_lower
               END
             ),
             array_agg(class_id ORDER BY lower_bound)
         INTO row_count, class_count, first_lower, valid_chain, actual_order
      FROM (
          SELECT class_id, lower_bound, upper_bound,
                 lead(lower_bound) OVER (ORDER BY lower_bound) AS next_lower
          FROM nccl_baseline.metric_threshold
          WHERE baseline_version_id = target_version
            AND metric_name = target_metric
      ) AS ordered_ranges;
        expected_order := CASE target_metric
                WHEN 'BUS_BW' THEN ARRAY[5, 4, 3, 2, 1]::SMALLINT[]
                ELSE ARRAY[1, 2, 3, 4, 5]::SMALLINT[]
        END;
        IF row_count <> 5 OR class_count <> 5 OR first_lower <> 0
             OR valid_chain IS NOT TRUE OR actual_order IS DISTINCT FROM expected_order THEN
                RAISE EXCEPTION 'metric threshold set must contain five contiguous semantic ranges covering [0,infinity)';
    END IF;
    RETURN NULL;
END
$$;
DROP TRIGGER IF EXISTS metric_threshold_complete ON nccl_baseline.metric_threshold;
CREATE CONSTRAINT TRIGGER metric_threshold_complete
AFTER INSERT OR UPDATE OR DELETE ON nccl_baseline.metric_threshold
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.validate_metric_threshold_set();

CREATE OR REPLACE FUNCTION nccl_baseline.validate_profile_active_pointer()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_profile UUID;
    profile_status TEXT;
    active_pointer UUID;
    pointed_profile UUID;
    pointed_status TEXT;
    active_count INTEGER;
BEGIN
    target_profile := CASE TG_TABLE_NAME
        WHEN 'baseline_profile' THEN NEW.profile_id
        ELSE NEW.profile_id
    END;
    SELECT status, active_baseline_version_id
      INTO profile_status, active_pointer
      FROM nccl_baseline.baseline_profile
     WHERE profile_id = target_profile;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT count(*)
      INTO active_count
      FROM nccl_baseline.baseline_version
     WHERE profile_id = target_profile AND status = 'ACTIVE';
    IF profile_status = 'ACTIVE' THEN
        SELECT profile_id, status
          INTO pointed_profile, pointed_status
          FROM nccl_baseline.baseline_version
         WHERE baseline_version_id = active_pointer;
        IF active_pointer IS NULL OR active_count <> 1
           OR pointed_profile IS DISTINCT FROM target_profile
           OR pointed_status IS DISTINCT FROM 'ACTIVE' THEN
            RAISE EXCEPTION 'ACTIVE profile must point to its sole ACTIVE baseline version';
        END IF;
    ELSIF active_pointer IS NOT NULL OR active_count <> 0 THEN
        RAISE EXCEPTION 'non-ACTIVE profile cannot retain an active baseline pointer/version';
    END IF;
    RETURN NULL;
END
$$;
DROP TRIGGER IF EXISTS baseline_profile_active_pointer_consistent
ON nccl_baseline.baseline_profile;
CREATE CONSTRAINT TRIGGER baseline_profile_active_pointer_consistent
AFTER INSERT OR UPDATE ON nccl_baseline.baseline_profile
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.validate_profile_active_pointer();
DROP TRIGGER IF EXISTS baseline_version_profile_pointer_consistent
ON nccl_baseline.baseline_version;
CREATE CONSTRAINT TRIGGER baseline_version_profile_pointer_consistent
AFTER INSERT OR UPDATE ON nccl_baseline.baseline_version
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION nccl_baseline.validate_profile_active_pointer();

CREATE OR REPLACE FUNCTION nccl_baseline.apply_calibration_decision(
    p_decision_id UUID,
    p_result_id BIGINT,
    p_action TEXT,
    p_actor TEXT,
    p_reason TEXT,
    p_evidence JSONB
)
RETURNS TABLE (
    applied_decision_version BIGINT,
    requested_action TEXT,
    effective_action TEXT,
    created BOOLEAN,
    eligible_delta INTEGER,
    invalidated_baseline_version_id UUID,
    waiting_jobs_updated INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    existing nccl_baseline.calibration_decision%ROWTYPE;
    target_profile_id UUID;
    target_status TEXT;
    target_bus_bw DOUBLE PRECISION;
    target_latency DOUBLE PRECISION;
    target_error_code TEXT;
    latest_action TEXT;
    latest_version BIGINT;
    next_version BIGINT;
    raw_eligible BOOLEAN;
    old_eligible BOOLEAN;
    new_eligible BOOLEAN;
    delta INTEGER;
    active_version_id UUID;
    invalidated_version_id UUID;
    waiting_count INTEGER := 0;
    affected_count INTEGER;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('nccl-calibration:' || p_result_id::text, 0)
    );

    SELECT *
      INTO existing
      FROM nccl_baseline.calibration_decision
     WHERE decision_id = p_decision_id;
    IF FOUND THEN
        IF existing.result_id IS DISTINCT FROM p_result_id
           OR existing.action IS DISTINCT FROM p_action
           OR existing.actor IS DISTINCT FROM p_actor
           OR existing.reason IS DISTINCT FROM p_reason
           OR existing.evidence IS DISTINCT FROM p_evidence THEN
            RAISE EXCEPTION 'decision_id retry conflicts with immutable calibration event';
        END IF;
        SELECT action
          INTO latest_action
          FROM nccl_baseline.calibration_decision
         WHERE result_id = p_result_id
         ORDER BY decision_version DESC
         LIMIT 1;
        RETURN QUERY SELECT
            existing.decision_version,
            p_action,
            latest_action,
            FALSE,
            0,
            NULL::UUID,
            0;
        RETURN;
    END IF;

    SELECT job.profile_id, result.result_status, result.bus_bw_gbps,
           result.latency_us, result.error_code
      INTO target_profile_id, target_status, target_bus_bw,
           target_latency, target_error_code
      FROM nccl_raw.node_result AS result
      JOIN nccl_validation.evaluation_job AS job USING (result_id)
     WHERE result.result_id = p_result_id
     FOR UPDATE OF result;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'calibration result_id does not exist: %', p_result_id;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(target_profile_id::text, 0));
    SELECT active_baseline_version_id
      INTO active_version_id
      FROM nccl_baseline.baseline_profile
     WHERE profile_id = target_profile_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'calibration profile does not exist: %', target_profile_id;
    END IF;

    SELECT decision_version, action
      INTO latest_version, latest_action
      FROM nccl_baseline.calibration_decision
     WHERE result_id = p_result_id
     ORDER BY decision_version DESC
     LIMIT 1;
    IF latest_version IS NULL THEN
        IF p_action IS DISTINCT FROM 'APPROVE' THEN
            RAISE EXCEPTION 'first calibration decision must be APPROVE';
        END IF;
        next_version := 1;
    ELSE
        IF p_action IS NOT DISTINCT FROM latest_action THEN
            RAISE EXCEPTION 'calibration decisions must alternate effective action';
        END IF;
        next_version := latest_version + 1;
    END IF;

    INSERT INTO nccl_baseline.calibration_decision (
        decision_id, result_id, decision_version, action, actor, reason, evidence
    ) VALUES (
        p_decision_id, p_result_id, next_version, p_action,
        p_actor, p_reason, p_evidence
    );

    raw_eligible := target_status = 'SUCCESS'
        AND target_bus_bw IS NOT NULL
        AND target_bus_bw > 0
        AND target_bus_bw < 'Infinity'::DOUBLE PRECISION
        AND target_latency IS NOT NULL
        AND target_latency > 0
        AND target_latency < 'Infinity'::DOUBLE PRECISION
        AND target_error_code IS NULL;
    old_eligible := raw_eligible
        AND (latest_action IS NOT DISTINCT FROM 'APPROVE');
    new_eligible := raw_eligible
        AND (p_action IS NOT DISTINCT FROM 'APPROVE');
    delta := new_eligible::INTEGER - old_eligible::INTEGER;
    IF delta <> 0 THEN
        UPDATE nccl_baseline.baseline_profile
           SET eligible_result_count = eligible_result_count + delta,
               updated_at = now()
         WHERE profile_id = target_profile_id;
    END IF;

    IF p_action = 'REVOKE' AND active_version_id IS NOT NULL AND EXISTS (
        SELECT 1
          FROM nccl_baseline.baseline_version_sample
         WHERE baseline_version_id = active_version_id
           AND result_id = p_result_id
           AND included
    ) THEN
        PERFORM set_config('cval.calibration_decision', 'revoke', TRUE);
        UPDATE nccl_baseline.baseline_version
           SET status = 'FAILED',
               failure_reason = 'CALIBRATION_REVOKED_RESULT:' || p_result_id::text
                   || ':DECISION:' || p_decision_id::text
         WHERE baseline_version_id = active_version_id
           AND profile_id = target_profile_id
           AND status = 'ACTIVE';
        GET DIAGNOSTICS affected_count = ROW_COUNT;
        IF affected_count <> 1 THEN
            RAISE EXCEPTION 'active baseline changed during calibration revocation';
        END IF;
        invalidated_version_id := active_version_id;
        UPDATE nccl_baseline.baseline_profile
           SET status = 'COLLECTING',
               active_baseline_version_id = NULL,
               updated_at = now()
         WHERE profile_id = target_profile_id;
        UPDATE nccl_validation.evaluation_job
           SET status = 'WAITING_FOR_BASELINE',
               next_attempt_at = now(),
               last_error = 'ACTIVE_BASELINE_INVALIDATED_BY_CALIBRATION'
         WHERE profile_id = target_profile_id
           AND status IN ('PENDING', 'RETRY');
        GET DIAGNOSTICS waiting_count = ROW_COUNT;
    END IF;

    RETURN QUERY SELECT
        next_version,
        p_action,
        p_action,
        TRUE,
        delta,
        invalidated_version_id,
        waiting_count;
END
$$;
REVOKE ALL ON FUNCTION nccl_baseline.apply_calibration_decision(
    UUID, BIGINT, TEXT, TEXT, TEXT, JSONB
) FROM PUBLIC;

CREATE OR REPLACE FUNCTION nccl_validation.reject_health_class_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NCCL health classes are fixed';
END
$$;
DROP TRIGGER IF EXISTS health_class_immutable ON nccl_validation.health_class;
CREATE TRIGGER health_class_immutable
BEFORE UPDATE OR DELETE ON nccl_validation.health_class
FOR EACH ROW EXECUTE FUNCTION nccl_validation.reject_health_class_mutation();

CREATE OR REPLACE FUNCTION nccl_validation.reject_evaluation_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NCCL historical evaluations are immutable';
END
$$;
DROP TRIGGER IF EXISTS evaluation_immutable ON nccl_validation.evaluation;
CREATE TRIGGER evaluation_immutable
BEFORE UPDATE OR DELETE ON nccl_validation.evaluation
FOR EACH ROW EXECUTE FUNCTION nccl_validation.reject_evaluation_mutation();

CREATE OR REPLACE FUNCTION nccl_raw.reject_protected_truncate()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NCCL protected tables cannot be truncated';
END
$$;

DROP TRIGGER IF EXISTS schema_migration_no_truncate ON nccl_raw.schema_migration;
CREATE TRIGGER schema_migration_no_truncate BEFORE TRUNCATE ON nccl_raw.schema_migration
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS test_run_no_truncate ON nccl_raw.test_run;
CREATE TRIGGER test_run_no_truncate BEFORE TRUNCATE ON nccl_raw.test_run
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS outbox_receipt_no_truncate ON nccl_raw.outbox_receipt;
CREATE TRIGGER outbox_receipt_no_truncate BEFORE TRUNCATE ON nccl_raw.outbox_receipt
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS outbox_scan_cursor_no_truncate ON nccl_raw.outbox_scan_cursor;
CREATE TRIGGER outbox_scan_cursor_no_truncate BEFORE TRUNCATE ON nccl_raw.outbox_scan_cursor
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS calibration_decision_no_truncate ON nccl_baseline.calibration_decision;
CREATE TRIGGER calibration_decision_no_truncate BEFORE TRUNCATE ON nccl_baseline.calibration_decision
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS node_result_no_truncate ON nccl_raw.node_result;
CREATE TRIGGER node_result_no_truncate BEFORE TRUNCATE ON nccl_raw.node_result
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS nic_result_no_truncate ON nccl_raw.nic_result;
CREATE TRIGGER nic_result_no_truncate BEFORE TRUNCATE ON nccl_raw.nic_result
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS baseline_profile_no_truncate ON nccl_baseline.baseline_profile;
CREATE TRIGGER baseline_profile_no_truncate BEFORE TRUNCATE ON nccl_baseline.baseline_profile
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS baseline_version_no_truncate ON nccl_baseline.baseline_version;
CREATE TRIGGER baseline_version_no_truncate BEFORE TRUNCATE ON nccl_baseline.baseline_version
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS baseline_version_sample_no_truncate ON nccl_baseline.baseline_version_sample;
CREATE TRIGGER baseline_version_sample_no_truncate BEFORE TRUNCATE ON nccl_baseline.baseline_version_sample
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS metric_threshold_no_truncate ON nccl_baseline.metric_threshold;
CREATE TRIGGER metric_threshold_no_truncate BEFORE TRUNCATE ON nccl_baseline.metric_threshold
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS health_class_no_truncate ON nccl_validation.health_class;
CREATE TRIGGER health_class_no_truncate BEFORE TRUNCATE ON nccl_validation.health_class
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS evaluation_job_no_truncate ON nccl_validation.evaluation_job;
CREATE TRIGGER evaluation_job_no_truncate BEFORE TRUNCATE ON nccl_validation.evaluation_job
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();
DROP TRIGGER IF EXISTS evaluation_no_truncate ON nccl_validation.evaluation;
CREATE TRIGGER evaluation_no_truncate BEFORE TRUNCATE ON nccl_validation.evaluation
FOR EACH STATEMENT EXECUTE FUNCTION nccl_raw.reject_protected_truncate();

CREATE OR REPLACE VIEW nccl_validation.raw_result_status_view AS
SELECT
    result.result_id,
    result.run_id,
    result.node_name,
    result.test_timestamp,
    result.result_status,
    job.profile_id,
    job.status AS evaluation_job_status,
    (job.status = 'COMPLETED') AS classified,
    job.attempt_count,
    job.next_attempt_at,
    job.completed_at
FROM nccl_raw.node_result AS result
JOIN nccl_validation.evaluation_job AS job USING (result_id);

CREATE OR REPLACE VIEW nccl_validation.latest_result_view AS
WITH latest_evaluation AS (
    SELECT DISTINCT ON (evaluation.result_id) evaluation.*
    FROM nccl_validation.evaluation AS evaluation
    ORDER BY evaluation.result_id, evaluation.evaluated_at DESC, evaluation.evaluation_id DESC
)
SELECT
    result.node_name,
    result.test_timestamp,
    result.la_timestamp,
    run.run_id,
    run.test_name,
    run.test_definition_version,
    run.image_name,
    run.image_digest,
    run.cuda_version,
    run.pytorch_version,
    run.compiled_nccl_version,
    run.runtime_nccl_package_version,
    run.driver_version,
    run.driver_version_group,
    run.topology_class,
    run.gpu_model,
    run.gpus_per_node,
    run.iterations,
    run.samples,
    run.test_config,
    run.cval_run_id,
    run.cval_result_digest,
    run.summary_sha256,
    run.runtime_evidence_sha256,
    run.source_commit,
    run.implementation_identity,
    run.legacy_source,
    job.profile_id,
    profile.profile_key,
    evaluation.baseline_version_id,
    baseline.version_number AS baseline_version_number,
    baseline.activated_at AS baseline_activation_timestamp,
    result.bus_bw_gbps,
    evaluation.bus_bw_class,
    bus_class.class_label AS bus_bw_class_label,
    evaluation.bus_bw_severity_percentile,
    result.latency_us,
    evaluation.latency_class,
    latency_class.class_label AS latency_class_label,
    evaluation.latency_severity_percentile,
    evaluation.overall_health_class,
    overall_class.class_label AS overall_health_class_label,
    evaluation.overall_severity_percentile,
    evaluation.evaluation_scope,
    evaluation.evaluated_at,
    evaluation.failure_code,
    evaluation.explanation,
    job.status AS evaluation_job_status
FROM latest_evaluation AS evaluation
JOIN nccl_raw.node_result AS result USING (result_id)
JOIN nccl_raw.test_run AS run USING (run_id)
JOIN nccl_validation.evaluation_job AS job USING (result_id)
JOIN nccl_baseline.baseline_profile AS profile ON profile.profile_id = job.profile_id
JOIN nccl_baseline.baseline_version AS baseline
  ON baseline.baseline_version_id = evaluation.baseline_version_id
LEFT JOIN nccl_validation.health_class AS bus_class
  ON bus_class.class_id = evaluation.bus_bw_class
LEFT JOIN nccl_validation.health_class AS latency_class
  ON latency_class.class_id = evaluation.latency_class
LEFT JOIN nccl_validation.health_class AS overall_class
  ON overall_class.class_id = evaluation.overall_health_class;
