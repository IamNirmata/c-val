"""Explicit-transaction PostgreSQL repository for NCCL evaluation."""

from __future__ import annotations

import importlib
import json
import math
import re
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid4

from cval.nccl_eval.config import NcclEvaluationConfig
from cval.nccl_eval.models import (
    EvaluationScope,
    IngestionBatch,
    NodeResult,
    ResultStatus,
    TestRun,
    json_ready,
)
from cval.nccl_eval.profile import BaselineProfileIdentity, build_profile_identity
from cval.nccl_eval.thresholds import (
    DistributionSummary,
    MetricName,
    ThresholdRange,
    classify,
    derive_thresholds,
    overall_health,
    piecewise_severity,
)


_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_OUTBOX_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}\.json$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CALIBRATION_ACTOR = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$")
_CREDENTIAL_URL = re.compile(r"(postgres(?:ql)?://)[^\s/@:]+(?::[^\s/@]*)?@", re.IGNORECASE)


class RawConflictError(ValueError):
    """Raised when an idempotency key is retried with a different payload."""


@dataclass(frozen=True)
class EligibilityDecision:
    result_id: int
    included: bool
    exclusion_reason: str | None
    bus_bw_gbps: float | None
    latency_us: float | None


@dataclass(frozen=True)
class CalibrationDecision:
    decision_id: UUID
    result_id: int
    action: str
    actor: str
    reason: str
    evidence: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.decision_id, UUID):
            raise TypeError("decision_id must be a UUID")
        if (
            isinstance(self.result_id, bool)
            or not isinstance(self.result_id, int)
            or self.result_id <= 0
        ):
            raise ValueError("result_id must be a positive integer")
        if self.action not in {"APPROVE", "REVOKE"}:
            raise ValueError("calibration action must be APPROVE or REVOKE")
        if not isinstance(self.actor, str) or not _CALIBRATION_ACTOR.fullmatch(self.actor):
            raise ValueError("actor must be 1-128 bounded nonsecret characters")
        if (
            not isinstance(self.reason, str)
            or not self.reason.strip()
            or len(self.reason) > 1000
            or "\n" in self.reason
            or "\r" in self.reason
        ):
            raise ValueError(
                "reason must be a non-empty single-line string of at most 1000 characters"
            )
        if not isinstance(self.evidence, Mapping):
            raise ValueError("evidence must be a JSON object")
        try:
            json.dumps(
                self.evidence,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("evidence must contain only finite JSON values") from exc

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CalibrationDecision":
        if not isinstance(value, Mapping):
            raise ValueError("calibration decision must be an object")
        allowed = {"decision_id", "result_id", "action", "actor", "reason", "evidence"}
        unknown = sorted(set(value) - allowed)
        missing = sorted(allowed - set(value))
        if unknown or missing:
            detail = []
            if unknown:
                detail.append("unknown=" + ",".join(unknown))
            if missing:
                detail.append("missing=" + ",".join(missing))
            raise ValueError("invalid calibration decision fields: " + "; ".join(detail))
        try:
            decision_id = UUID(str(value["decision_id"]))
        except (TypeError, ValueError) as exc:
            raise ValueError("decision_id must be a UUID") from exc
        result_id = value["result_id"]
        return cls(
            decision_id=decision_id,
            result_id=result_id,  # type: ignore[arg-type]
            action=str(value["action"]),
            actor=str(value["actor"]),
            reason=str(value["reason"]),
            evidence=value["evidence"],  # type: ignore[arg-type]
        )


def parse_calibration_input(value: Mapping[str, Any]) -> tuple[CalibrationDecision, ...]:
    if not isinstance(value, Mapping) or set(value) != {"decisions"}:
        raise ValueError("calibration input must be exactly an object with decisions")
    raw = value["decisions"]
    if not isinstance(raw, list) or not raw or len(raw) > 5000:
        raise ValueError("calibration decisions must be a non-empty array of at most 5000")
    decisions = tuple(CalibrationDecision.from_dict(item) for item in raw)
    ids = [item.decision_id for item in decisions]
    if len(ids) != len(set(ids)):
        raise ValueError("calibration input contains duplicate decision_id values")
    results = [item.result_id for item in decisions]
    if len(results) != len(set(results)):
        raise ValueError("calibration input may contain only one decision per result_id")
    return decisions


@dataclass(frozen=True)
class ClaimReceipt:
    """Exact lease identity required for every claimed-job mutation."""

    result_id: int
    attempt_count: int
    claim_token: UUID
    claimed_by: str

    def __post_init__(self) -> None:
        if isinstance(self.result_id, bool) or not isinstance(self.result_id, int):
            raise TypeError("result_id must be an integer")
        if self.result_id <= 0:
            raise ValueError("result_id must be positive")
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise TypeError("attempt_count must be an integer")
        if self.attempt_count <= 0:
            raise ValueError("attempt_count must be positive")
        if not isinstance(self.claim_token, UUID):
            raise TypeError("claim_token must be a UUID")
        validate_worker_id(self.claimed_by)


@dataclass(frozen=True)
class EvaluationWork:
    result_id: int
    baseline_version_id: UUID
    bus_bw_gbps: float
    latency_us: float
    scope: EvaluationScope
    bus_summary: DistributionSummary
    latency_summary: DistributionSummary
    bus_ranges: tuple[ThresholdRange, ...]
    latency_ranges: tuple[ThresholdRange, ...]


CLAIM_JOBS_SQL = """
SELECT result_id
    FROM nccl_validation.evaluation_job
    WHERE status IN ('PENDING', 'RETRY')
      AND next_attempt_at <= now()
      AND attempt_count < %s
    ORDER BY created_at, result_id
    FOR UPDATE SKIP LOCKED
    LIMIT %s
"""

CLAIM_JOB_SQL = """
UPDATE nccl_validation.evaluation_job
SET status = 'PROCESSING',
    claimed_by = %s,
    claimed_at = now(),
        claim_token = %s,
    attempt_count = attempt_count + 1
WHERE result_id = %s
    AND status IN ('PENDING', 'RETRY')
RETURNING result_id, attempt_count, claim_token
"""


def create_pool(config: NcclEvaluationConfig) -> Any:
    """Lazily create, explicitly open, and verify a bounded Psycopg pool."""

    try:
        ConnectionPool = importlib.import_module("psycopg_pool").ConnectionPool
    except ImportError as exc:
        raise RuntimeError(
            "NCCL PostgreSQL commands require the optional cval[postgresql] dependencies"
        ) from exc

    pool = ConnectionPool(
        conninfo=config.require_database_url(),
        min_size=config.pool_min_size,
        max_size=config.pool_max_size,
        timeout=config.pool_timeout_seconds,
        max_waiting=max(8, config.pool_max_size * 8),
        open=False,
        kwargs={
            "autocommit": True,
            "connect_timeout": max(1, math.ceil(config.pool_timeout_seconds)),
            "application_name": "cval-nccl-eval",
        },
        name="cval-nccl-eval",
    )
    try:
        pool.open()
        pool.wait(timeout=config.pool_startup_timeout_seconds)
    except BaseException:
        pool.close()
        raise
    return pool


def validate_worker_id(worker_id: str) -> str:
    if not isinstance(worker_id, str) or not _WORKER_ID.fullmatch(worker_id):
        raise ValueError(
            "worker_id must be 1-64 nonsecret characters using letters, digits, '.', '_' or '-'"
        )
    return worker_id


def default_worker_id() -> str:
    """Return a bounded host/pod-derived identity with per-process uniqueness."""

    host = re.sub(r"[^A-Za-z0-9._-]+", "-", socket.gethostname()).strip("-._")
    return f"cval-nccl-{(host or 'worker')[:16]}-{uuid4()}"


def baseline_build_due(
    eligible_result_count: int,
    last_built_sample_count: int,
    *,
    has_active_baseline: bool,
    minimum_results: int = 40,
    update_increment: int = 10,
) -> bool:
    """Pure 40-initial/+10-refinement gate."""

    for name, value in (
        ("eligible_result_count", eligible_result_count),
        ("last_built_sample_count", last_built_sample_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    if minimum_results < 40 or update_increment < 10:
        raise ValueError("baseline gate requires at least 40 initial and +10 refinement")
    if not has_active_baseline:
        return eligible_result_count >= minimum_results
    return eligible_result_count >= last_built_sample_count + update_increment


def assess_eligibility(
    result_id: int,
    *,
    result_status: str,
    bus_bw_gbps: float | None,
    latency_us: float | None,
    error_code: str | None,
    effective_calibration_action: str | None,
) -> EligibilityDecision:
    """Apply the versioned explicit-calibration eligibility policy."""

    reason: str | None = None
    if result_status != ResultStatus.SUCCESS.value:
        reason = f"RESULT_STATUS_{result_status}"
    elif bus_bw_gbps is None:
        reason = "MISSING_BUS_BW"
    elif latency_us is None:
        reason = "MISSING_LATENCY"
    elif not _finite_positive(bus_bw_gbps):
        reason = "INVALID_BUS_BW"
    elif not _finite_positive(latency_us):
        reason = "INVALID_LATENCY"
    elif error_code:
        reason = "BLOCKING_ERROR"
    elif effective_calibration_action != "APPROVE":
        reason = "NOT_EXPLICITLY_APPROVED"
    return EligibilityDecision(
        result_id=int(result_id),
        included=reason is None,
        exclusion_reason=reason,
        bus_bw_gbps=None if bus_bw_gbps is None else float(bus_bw_gbps),
        latency_us=None if latency_us is None else float(latency_us),
    )


class NcclEvaluationRepository:
    """All NCCL database mutation boundaries and read models."""

    def __init__(self, pool: Any, config: NcclEvaluationConfig) -> None:
        self.pool = pool
        self.config = config

    def close(self) -> None:
        self.pool.close()

    def ingest_batch(self, batch: IngestionBatch) -> dict[str, object]:
        """Atomically ingest a test run, nodes, NIC rows, profile, and jobs."""

        if not isinstance(batch, IngestionBatch):
            raise TypeError("batch must be an IngestionBatch")
        with self.pool.connection() as connection:
            with connection.transaction():
                return self._ingest_batch(connection, batch)

    def ingest_outbox_batch(
        self,
        batch: IngestionBatch,
        *,
        outbox_name: str,
        content_sha256: str,
        observed_fingerprint: str,
    ) -> dict[str, object]:
        """Ingest one exact file and its immutable receipt in one transaction."""

        if not isinstance(batch, IngestionBatch):
            raise TypeError("batch must be an IngestionBatch")
        if not isinstance(outbox_name, str) or not _OUTBOX_NAME.fullmatch(outbox_name):
            raise ValueError("outbox_name must be a safe immediate JSON filename")
        if not isinstance(content_sha256, str) or not _SHA256.fullmatch(content_sha256):
            raise ValueError("content_sha256 must be a lowercase SHA-256 digest")
        if (
            not isinstance(observed_fingerprint, str)
            or not observed_fingerprint
            or len(observed_fingerprint) > 256
        ):
            raise ValueError("observed_fingerprint must be 1-256 characters")
        profile = build_profile_identity(batch.test_run)
        with self.pool.connection() as connection:
            with connection.transaction():
                existing = connection.execute(
                    """
                    SELECT status, content_sha256, run_id, profile_id, observed_fingerprint
                    FROM nccl_raw.outbox_receipt WHERE outbox_name = %s
                    """,
                    (outbox_name,),
                ).fetchone()
                if existing is not None:
                    expected = (
                        "INGESTED",
                        content_sha256,
                        batch.test_run.run_id,
                        profile.profile_id,
                        observed_fingerprint,
                    )
                    if tuple(existing) != expected:
                        raise RawConflictError(
                            "immutable outbox receipt differs for existing filename"
                        )
                    return {
                        "run_id": str(batch.test_run.run_id),
                        "profile_id": str(profile.profile_id),
                        "profile_key": profile.profile_key,
                        "node_count": len(batch.node_results),
                        "inserted_node_count": 0,
                        "inserted_eligible_count": 0,
                        "idempotent_node_count": len(batch.node_results),
                        "result_ids": [],
                        "outbox_name": outbox_name,
                        "content_sha256": content_sha256,
                        "receipt_created": False,
                    }
                receipt = self._ingest_batch(connection, batch)
                connection.execute(
                    """
                    INSERT INTO nccl_raw.outbox_receipt (
                        outbox_name, status, content_sha256, run_id, profile_id,
                        observed_fingerprint
                    ) VALUES (%s, 'INGESTED', %s, %s, %s, %s)
                    """,
                    (
                        outbox_name,
                        content_sha256,
                        batch.test_run.run_id,
                        profile.profile_id,
                        observed_fingerprint,
                    ),
                )
                return receipt | {
                    "outbox_name": outbox_name,
                    "content_sha256": content_sha256,
                    "receipt_created": True,
                }

    def outbox_terminal_states(self, names: Sequence[str]) -> dict[str, str]:
        safe_names = tuple(dict.fromkeys(names))
        if len(safe_names) > 5000 or any(
            not _scanned_outbox_name(name) for name in safe_names
        ):
            raise ValueError("outbox names must be at most 5000 bounded immediate JSON names")
        if not safe_names:
            return {}
        with self.pool.connection() as connection:
            with connection.transaction():
                rows = connection.execute(
                    "SELECT outbox_name, status FROM nccl_raw.outbox_receipt "
                    "WHERE outbox_name = ANY(%s)",
                    (list(safe_names),),
                ).fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def get_outbox_cursor(self) -> str | None:
        with self.pool.connection() as connection:
            with connection.transaction():
                row = connection.execute(
                    "SELECT outbox_name FROM nccl_raw.outbox_scan_cursor WHERE singleton"
                ).fetchone()
        if row is None:
            raise RuntimeError("outbox scan cursor singleton is absent")
        return row[0]

    def set_outbox_cursor(self, outbox_name: str) -> None:
        if not _scanned_outbox_name(outbox_name):
            raise ValueError("outbox cursor must be a bounded immediate JSON name")
        with self.pool.connection() as connection:
            with connection.transaction():
                updated = connection.execute(
                    "UPDATE nccl_raw.outbox_scan_cursor SET outbox_name = %s, "
                    "updated_at = now() WHERE singleton",
                    (outbox_name,),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("outbox scan cursor singleton is absent")

    def record_outbox_rejection(
        self,
        *,
        outbox_name: str,
        safe_error: str,
        observed_fingerprint: str,
        content_sha256: str | None = None,
    ) -> dict[str, object]:
        if not _scanned_outbox_name(outbox_name):
            raise ValueError("outbox_name must be a bounded immediate JSON name")
        if content_sha256 is not None and not _SHA256.fullmatch(content_sha256):
            raise ValueError("content_sha256 must be a lowercase SHA-256 digest")
        safe_error = self._safe_error(ValueError(safe_error))
        if not observed_fingerprint or len(observed_fingerprint) > 256:
            raise ValueError("observed_fingerprint must be 1-256 characters")
        expected = ("REJECTED", content_sha256, safe_error, observed_fingerprint)
        with self.pool.connection() as connection:
            with connection.transaction():
                existing = connection.execute(
                    "SELECT status, content_sha256, safe_error, observed_fingerprint "
                    "FROM nccl_raw.outbox_receipt WHERE outbox_name = %s",
                    (outbox_name,),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != expected:
                        raise RawConflictError(
                            "immutable outbox rejection differs for existing filename"
                        )
                    return {
                        "outbox_name": outbox_name,
                        "status": "REJECTED",
                        "receipt_created": False,
                    }
                connection.execute(
                    """
                    INSERT INTO nccl_raw.outbox_receipt (
                        outbox_name, status, content_sha256, safe_error,
                        observed_fingerprint
                    ) VALUES (%s, 'REJECTED', %s, %s, %s)
                    """,
                    (outbox_name, content_sha256, safe_error, observed_fingerprint),
                )
        return {
            "outbox_name": outbox_name,
            "status": "REJECTED",
            "receipt_created": True,
        }

    def _ingest_batch(
        self, connection: Any, batch: IngestionBatch
    ) -> dict[str, object]:
        profile = build_profile_identity(batch.test_run)
        inserted_nodes = 0
        inserted_eligible = 0
        result_ids: list[int] = []
        self._insert_or_match_run(
            connection,
            batch.test_run,
            profile.test_config_fingerprint,
        )
        profile_id = self._insert_or_match_profile(connection, profile)
        for node in batch.node_results:
            result_id, inserted = self._insert_or_match_node(
                connection, batch.test_run.run_id, node, profile_id
            )
            inserted_nodes += int(inserted)
            result_ids.append(result_id)
        connection.execute(
            """
            UPDATE nccl_baseline.baseline_profile
            SET eligible_result_count = eligible_result_count + %s,
                updated_at = now()
            WHERE profile_id = %s
            """,
            (inserted_eligible, profile_id),
        )
        return {
            "run_id": str(batch.test_run.run_id),
            "profile_id": str(profile.profile_id),
            "profile_key": profile.profile_key,
            "node_count": len(batch.node_results),
            "inserted_node_count": inserted_nodes,
            "inserted_eligible_count": inserted_eligible,
            "idempotent_node_count": len(batch.node_results) - inserted_nodes,
            "result_ids": result_ids,
        }

    def _insert_or_match_run(
        self,
        connection: Any,
        run: TestRun,
        config_fingerprint: str,
    ) -> None:
        values = (
            run.run_id,
            run.test_name,
            run.test_definition_version,
            run.started_at,
            run.completed_at,
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
            config_fingerprint,
            json.dumps(json_ready(run.test_config), sort_keys=True, separators=(",", ":")),
            run.cval_run_id,
            run.cval_result_digest,
            run.summary_sha256,
            run.runtime_evidence_sha256,
            run.source_commit,
            run.implementation_identity,
            run.legacy_source,
        )
        inserted = connection.execute(
            """
            INSERT INTO nccl_raw.test_run (
                run_id, test_name, test_definition_version, started_at, completed_at,
                image_name, image_digest, cuda_version, pytorch_version,
                compiled_nccl_version, runtime_nccl_package_version,
                driver_version, driver_version_group, topology_class, gpu_model,
                gpus_per_node, iterations, samples, test_config_fingerprint, test_config,
                cval_run_id, cval_result_digest, summary_sha256,
                runtime_evidence_sha256, source_commit, implementation_identity,
                legacy_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (run_id) DO NOTHING
            RETURNING run_id
            """,
            values,
        ).fetchone()
        if inserted is not None:
            return
        existing = connection.execute(
            """
            SELECT test_name, test_definition_version, started_at, completed_at,
                     image_name, image_digest, cuda_version, pytorch_version,
                     compiled_nccl_version, runtime_nccl_package_version,
                   driver_version, driver_version_group, topology_class, gpu_model,
                     gpus_per_node, iterations, samples, test_config_fingerprint,
                     test_config,
                     cval_run_id, cval_result_digest, summary_sha256,
                     runtime_evidence_sha256, source_commit, implementation_identity,
                     legacy_source
            FROM nccl_raw.test_run WHERE run_id = %s
            """,
            (run.run_id,),
        ).fetchone()
        expected = values[1:19] + (json_ready(run.test_config),) + values[20:]
        if existing is None or tuple(existing) != expected:
            raise RawConflictError("immutable test_run payload differs for existing run_id")

    def _insert_or_match_profile(
        self, connection: Any, profile: BaselineProfileIdentity
    ) -> UUID:
        profile_json = json.dumps(
            json_ready(profile.test_config), sort_keys=True, separators=(",", ":")
        )
        connection.execute(
            """
            INSERT INTO nccl_baseline.baseline_profile (
                profile_id, profile_key, test_name, test_definition_version,
                gpu_model, gpus_per_node, cuda_version, pytorch_version,
                compiled_nccl_version, runtime_nccl_package_version,
                driver_version_group, topology_class, source_commit, image_digest,
                implementation_identity, test_config_fingerprint,
                test_config, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s::jsonb, 'COLLECTING'
            )
            ON CONFLICT (profile_key) DO NOTHING
            """,
            (
                profile.profile_id,
                profile.profile_key,
                profile.test_name,
                profile.test_definition_version,
                profile.gpu_model,
                profile.gpus_per_node,
                profile.cuda_version,
                profile.pytorch_version,
                profile.compiled_nccl_version,
                profile.runtime_nccl_package_version,
                profile.driver_version_group,
                profile.topology_class,
                profile.source_commit,
                profile.image_digest,
                profile.implementation_identity,
                profile.test_config_fingerprint,
                profile_json,
            ),
        )
        existing = connection.execute(
            """
            SELECT profile_id, test_name, test_definition_version, gpu_model,
                     gpus_per_node, cuda_version, pytorch_version,
                     compiled_nccl_version, runtime_nccl_package_version,
                     driver_version_group, topology_class, source_commit, image_digest,
                       implementation_identity, test_config_fingerprint, test_config
            FROM nccl_baseline.baseline_profile WHERE profile_key = %s
            """,
            (profile.profile_key,),
        ).fetchone()
        expected = (
            profile.profile_id,
            profile.test_name,
            profile.test_definition_version,
            profile.gpu_model,
            profile.gpus_per_node,
            profile.cuda_version,
            profile.pytorch_version,
            profile.compiled_nccl_version,
            profile.runtime_nccl_package_version,
            profile.driver_version_group,
            profile.topology_class,
            profile.source_commit,
            profile.image_digest,
            profile.implementation_identity,
            profile.test_config_fingerprint,
            json_ready(profile.test_config),
        )
        if existing is None or tuple(existing) != expected:
            raise RawConflictError("profile_key collision has a different immutable payload")
        return profile.profile_id

    def _insert_or_match_node(
        self,
        connection: Any,
        run_id: UUID,
        node: NodeResult,
        profile_id: UUID,
    ) -> tuple[int, bool]:
        profile = connection.execute(
            """
            SELECT active_baseline_version_id
            FROM nccl_baseline.baseline_profile
            WHERE profile_id = %s
            FOR KEY SHARE
            """,
            (profile_id,),
        ).fetchone()
        if profile is None:
            raise RuntimeError("matched baseline profile disappeared before ingestion")
        values = (
            run_id,
            node.node_name,
            node.test_timestamp,
            node.la_timestamp,
            node.bus_bw_gbps,
            node.latency_us,
            node.result_status.value,
            node.error_code,
            node.error_message,
        )
        inserted = connection.execute(
            """
            INSERT INTO nccl_raw.node_result (
                run_id, node_name, test_timestamp, la_timestamp, bus_bw_gbps,
                latency_us, result_status, error_code, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, node_name) DO NOTHING
            RETURNING result_id
            """,
            values,
        ).fetchone()
        if inserted is None:
            existing = connection.execute(
                """
                SELECT result_id, test_timestamp, la_timestamp, bus_bw_gbps, latency_us,
                      result_status, error_code, error_message
                FROM nccl_raw.node_result
                WHERE run_id = %s AND node_name = %s
                """,
                (run_id, node.node_name),
            ).fetchone()
            expected = values[2:]
            if existing is None or tuple(existing[1:]) != expected:
                raise RawConflictError(
                    "immutable node_result payload differs for existing run_id/node_name"
                )
            result_id = int(existing[0])
            existing_nics = connection.execute(
                """
                SELECT device_name, max_bus_bw_gbps
                FROM nccl_raw.nic_result
                WHERE result_id = %s ORDER BY device_name
                """,
                (result_id,),
            ).fetchall()
            expected_nics = sorted(
                (nic.device_name, nic.max_bus_bw_gbps) for nic in node.nics
            )
            if list(existing_nics) != expected_nics:
                raise RawConflictError("immutable NIC payload differs for existing node_result")
            job = connection.execute(
                "SELECT profile_id FROM nccl_validation.evaluation_job WHERE result_id = %s",
                (result_id,),
            ).fetchone()
            if job is None or job[0] != profile_id:
                raise RawConflictError("existing node_result has a different evaluation profile")
            return result_id, False

        result_id = int(inserted[0])
        if node.nics:
            _executemany(
                connection,
                """
                INSERT INTO nccl_raw.nic_result
                    (result_id, device_name, max_bus_bw_gbps)
                VALUES (%s, %s, %s)
                """,
                [
                    (result_id, nic.device_name, nic.max_bus_bw_gbps)
                    for nic in node.nics
                ],
            )
        job_status = (
            "WAITING_FOR_BASELINE"
            if node.result_status is ResultStatus.SUCCESS and profile[0] is None
            else "PENDING"
        )
        job = connection.execute(
            """
            INSERT INTO nccl_validation.evaluation_job (result_id, profile_id, status)
            VALUES (%s, %s, %s)
            RETURNING result_id
            """,
            (result_id, profile_id, job_status),
        ).fetchone()
        if job is None or int(job[0]) != result_id:
            raise RuntimeError("evaluation job insertion did not return the ingested result")
        return result_id, True

    def calibration_plan(
        self, decisions: Sequence[CalibrationDecision]
    ) -> dict[str, object]:
        if not decisions:
            raise ValueError("calibration decisions must not be empty")
        planned: list[dict[str, object]] = []
        with self.pool.connection() as connection:
            with connection.transaction():
                for decision in decisions:
                    existing_id = self._calibration_by_id(connection, decision.decision_id)
                    current = self._calibration_target(connection, decision.result_id)
                    if existing_id is not None:
                        self._match_calibration_retry(existing_id, decision)
                        idempotent = True
                    else:
                        idempotent = False
                        _validate_calibration_transition(current[7], decision.action)
                    planned.append(
                        {
                            "decision_id": str(decision.decision_id),
                            "result_id": decision.result_id,
                            "profile_id": str(current[0]),
                            "current_action": current[7],
                            "requested_action": decision.action,
                            "effective_action": (
                                current[7] if idempotent else decision.action
                            ),
                            "idempotent_retry": idempotent,
                        }
                    )
        return {
            "mode": "dry-run",
            "decision_count": len(planned),
            "change_count": sum(not item["idempotent_retry"] for item in planned),
            "decisions": planned,
        }

    def apply_calibration(
        self, decisions: Sequence[CalibrationDecision]
    ) -> dict[str, object]:
        if not decisions:
            raise ValueError("calibration decisions must not be empty")
        receipts: list[dict[str, object]] = []
        with self.pool.connection() as connection:
            with connection.transaction():
                for decision in decisions:
                    existing_id = self._calibration_by_id(
                        connection, decision.decision_id
                    )
                    if existing_id is not None:
                        self._match_calibration_retry(existing_id, decision)
                    row = connection.execute(
                        """
                        SELECT applied_decision_version, requested_action,
                               effective_action, created, eligible_delta,
                               invalidated_baseline_version_id, waiting_jobs_updated
                        FROM nccl_baseline.apply_calibration_decision(
                            %s, %s, %s, %s, %s, %s::jsonb
                        )
                        """,
                        (
                            decision.decision_id,
                            decision.result_id,
                            decision.action,
                            decision.actor,
                            decision.reason,
                            json.dumps(
                                decision.evidence,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        ),
                    ).fetchone()
                    if row is None:
                        raise RuntimeError(
                            "calibration decision function returned no receipt"
                        )
                    receipts.append(
                        {
                            "decision_id": str(decision.decision_id),
                            "result_id": decision.result_id,
                            "decision_version": int(row[0]),
                            "requested_action": row[1],
                            "action": row[2],
                            "effective_action": row[2],
                            "created": bool(row[3]),
                            "eligible_delta": int(row[4]),
                            "invalidated_baseline_version_id": (
                                str(row[5]) if row[5] is not None else None
                            ),
                            "waiting_jobs_updated": int(row[6]),
                        }
                    )
        return {
            "mode": "apply",
            "decision_count": len(receipts),
            "created_count": sum(bool(item["created"]) for item in receipts),
            "decisions": receipts,
        }

    def calibration_report(self, *, limit: int = 100) -> dict[str, object]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
            raise ValueError("calibration limit must be between 1 and 5000")
        with self.pool.connection() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """
                    SELECT decision_id, result_id, decision_version, action,
                           actor, reason, evidence, created_at,
                           row_number() OVER (
                               PARTITION BY result_id ORDER BY decision_version DESC
                           ) = 1 AS effective
                    FROM nccl_baseline.calibration_decision
                    ORDER BY created_at DESC, decision_id
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
                counts = dict(
                    connection.execute(
                        """
                        WITH latest AS (
                            SELECT DISTINCT ON (result_id) result_id, action
                            FROM nccl_baseline.calibration_decision
                            ORDER BY result_id, decision_version DESC
                        )
                        SELECT action, count(*) FROM latest GROUP BY action ORDER BY action
                        """
                    ).fetchall()
                )
        return {
            "mode": "dry-run",
            "effective_counts": counts,
            "event_count": len(rows),
            "events": [
                {
                    "decision_id": str(row[0]),
                    "result_id": row[1],
                    "decision_version": row[2],
                    "action": row[3],
                    "actor": row[4],
                    "reason": row[5],
                    "evidence": row[6],
                    "created_at": _json_scalar(row[7]),
                    "effective": row[8],
                }
                for row in rows
            ],
        }

    @staticmethod
    def _calibration_by_id(connection: Any, decision_id: UUID) -> Any:
        return connection.execute(
            """
            SELECT decision_id, result_id, decision_version, action, actor, reason, evidence
            FROM nccl_baseline.calibration_decision WHERE decision_id = %s
            """,
            (decision_id,),
        ).fetchone()

    @staticmethod
    def _calibration_target(connection: Any, result_id: int) -> Any:
        row = connection.execute(
            """
            SELECT job.profile_id, result.result_id, result.result_status,
                   result.bus_bw_gbps, result.latency_us, result.error_code,
                   result.node_name, decision.action, decision.decision_version
            FROM nccl_raw.node_result AS result
            JOIN nccl_validation.evaluation_job AS job USING (result_id)
            LEFT JOIN LATERAL (
                SELECT action, decision_version
                FROM nccl_baseline.calibration_decision
                WHERE result_id = result.result_id
                ORDER BY decision_version DESC LIMIT 1
            ) AS decision ON TRUE
            WHERE result.result_id = %s
            """,
            (result_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"calibration result_id does not exist: {result_id}")
        return row

    @staticmethod
    def _match_calibration_retry(existing: Any, decision: CalibrationDecision) -> None:
        expected = (
            decision.decision_id,
            decision.result_id,
            existing[2],
            decision.action,
            decision.actor,
            decision.reason,
            dict(decision.evidence),
        )
        actual = tuple(existing[:6]) + (dict(existing[6]),)
        if actual != expected:
            raise RawConflictError(
                "decision_id retry conflicts with the immutable calibration event"
            )

    def baseline_eligibility_report(self) -> dict[str, object]:
        profiles: list[dict[str, object]] = []
        with self.pool.connection() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """
                    SELECT profile_id, profile_key, status, active_baseline_version_id,
                           last_built_sample_count
                    FROM nccl_baseline.baseline_profile
                    ORDER BY profile_key
                    """
                ).fetchall()
                for row in rows:
                    decisions = self._eligibility_decisions(connection, row[0])
                    eligible = sum(item.included for item in decisions)
                    exclusions = _exclusion_counts(decisions)
                    profiles.append(
                        {
                            "profile_id": str(row[0]),
                            "profile_key": row[1],
                            "profile_status": row[2],
                            "eligible_result_count": eligible,
                            "excluded_result_count": len(decisions) - eligible,
                            "exclusions": exclusions,
                            "last_built_sample_count": row[4],
                            "build_due": baseline_build_due(
                                eligible,
                                row[4],
                                has_active_baseline=row[3] is not None,
                                minimum_results=self.config.baseline_minimum_results,
                                update_increment=self.config.baseline_update_increment,
                            ),
                        }
                    )
        return {"mode": "dry-run", "profiles": profiles, "profile_count": len(profiles)}

    def build_baselines(self) -> dict[str, object]:
        with self.pool.connection() as connection:
            with connection.transaction():
                profile_ids = [
                    row[0]
                    for row in connection.execute(
                        """
                        SELECT profile_id FROM nccl_baseline.baseline_profile
                        WHERE status <> 'DISABLED' ORDER BY profile_key
                        """
                    ).fetchall()
                ]
        results = [self._build_profile(profile_id) for profile_id in profile_ids]
        return {
            "mode": "apply",
            "profile_count": len(results),
            "built_count": sum(bool(item.get("built")) for item in results),
            "profiles": results,
        }

    def _build_profile(self, profile_id: UUID) -> dict[str, object]:
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (str(profile_id),),
                )
                profile = connection.execute(
                    """
                    SELECT profile_key, status, active_baseline_version_id,
                           last_built_sample_count,
                           (
                               SELECT baseline_version_id
                               FROM nccl_baseline.baseline_version
                               WHERE profile_id = profile.profile_id
                               ORDER BY version_number DESC
                               LIMIT 1
                           ) AS latest_baseline_version_id
                    FROM nccl_baseline.baseline_profile AS profile
                    WHERE profile.profile_id = %s FOR UPDATE
                    """,
                    (profile_id,),
                ).fetchone()
                if profile is None:
                    raise ValueError("baseline profile no longer exists")
                if profile[1] == "DISABLED":
                    return {
                        "profile_id": str(profile_id),
                        "profile_key": profile[0],
                        "built": False,
                        "reason": "PROFILE_DISABLED",
                    }
                decisions = self._eligibility_decisions(connection, profile_id)
                included = [item for item in decisions if item.included]
                eligible_count = len(included)
                connection.execute(
                    """
                    UPDATE nccl_baseline.baseline_profile
                    SET eligible_result_count = %s, updated_at = now()
                    WHERE profile_id = %s
                    """,
                    (eligible_count, profile_id),
                )
                if not baseline_build_due(
                    eligible_count,
                    profile[3],
                    has_active_baseline=profile[2] is not None,
                    minimum_results=self.config.baseline_minimum_results,
                    update_increment=self.config.baseline_update_increment,
                ):
                    return {
                        "profile_id": str(profile_id),
                        "profile_key": profile[0],
                        "eligible_result_count": eligible_count,
                        "built": False,
                        "reason": "GATE_NOT_MET",
                    }

                bus_values = [float(item.bus_bw_gbps) for item in included]
                latency_values = [float(item.latency_us) for item in included]
                bus = derive_thresholds(
                    MetricName.BUS_BW,
                    bus_values,
                    derivation_method_version=self.config.derivation_method_version,
                )
                latency = derive_thresholds(
                    MetricName.LATENCY,
                    latency_values,
                    derivation_method_version=self.config.derivation_method_version,
                )
                version_number = connection.execute(
                    """
                    SELECT COALESCE(max(version_number), 0) + 1
                    FROM nccl_baseline.baseline_version WHERE profile_id = %s
                    """,
                    (profile_id,),
                ).fetchone()[0]
                version_id = uuid4()
                supersedes_version_id = profile[2] or profile[4]
                connection.execute(
                    """
                    INSERT INTO nccl_baseline.baseline_version (
                        baseline_version_id, profile_id, version_number, status,
                        sample_count, supersedes_version_id, derivation_method_version,
                        bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                        latency_mean, latency_p05, latency_p50, latency_p95
                    ) VALUES (
                        %s, %s, %s, 'BUILDING', %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        version_id,
                        profile_id,
                        version_number,
                        eligible_count,
                        supersedes_version_id,
                        self.config.derivation_method_version,
                        bus.summary.mean,
                        bus.summary.p05,
                        bus.summary.p50,
                        bus.summary.p95,
                        latency.summary.mean,
                        latency.summary.p05,
                        latency.summary.p50,
                        latency.summary.p95,
                    ),
                )
                _executemany(
                    connection,
                    """
                    INSERT INTO nccl_baseline.baseline_version_sample (
                        baseline_version_id, result_id, included, exclusion_reason
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (
                            version_id,
                            item.result_id,
                            item.included,
                            item.exclusion_reason,
                        )
                        for item in decisions
                    ],
                )
                _executemany(
                    connection,
                    """
                    INSERT INTO nccl_baseline.metric_threshold (
                        baseline_version_id, metric_name, class_id,
                        lower_bound, upper_bound, unit
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            version_id,
                            threshold.metric_name.value,
                            threshold.class_id,
                            threshold.lower_bound,
                            threshold.upper_bound,
                            threshold.unit,
                        )
                        for threshold in (*bus.ranges, *latency.ranges)
                    ],
                )
                if profile[2] is not None:
                    updated = connection.execute(
                        """
                        UPDATE nccl_baseline.baseline_version
                        SET status = 'SUPERSEDED'
                        WHERE baseline_version_id = %s AND status = 'ACTIVE'
                        """,
                        (profile[2],),
                    )
                    if updated.rowcount != 1:
                        raise RuntimeError("active baseline changed during locked build")
                activated = connection.execute(
                    """
                    UPDATE nccl_baseline.baseline_version
                    SET status = 'ACTIVE', activated_at = now()
                    WHERE baseline_version_id = %s AND status = 'BUILDING'
                    """,
                    (version_id,),
                )
                if activated.rowcount != 1:
                    raise RuntimeError("new baseline did not activate exactly once")
                profile_updated = connection.execute(
                    """
                    UPDATE nccl_baseline.baseline_profile
                    SET status = 'ACTIVE', active_baseline_version_id = %s,
                        last_built_sample_count = %s,
                        eligible_result_count = %s, updated_at = now()
                    WHERE profile_id = %s
                    """,
                    (version_id, eligible_count, eligible_count, profile_id),
                )
                if profile_updated.rowcount != 1:
                    raise RuntimeError("baseline profile pointer did not update exactly once")
                waiting = connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET status = 'PENDING', next_attempt_at = now(), last_error = NULL
                    WHERE profile_id = %s AND status = 'WAITING_FOR_BASELINE'
                    """,
                    (profile_id,),
                ).rowcount
                return {
                    "profile_id": str(profile_id),
                    "profile_key": profile[0],
                    "built": True,
                    "baseline_version_id": str(version_id),
                    "version_number": version_number,
                    "sample_count": eligible_count,
                    "excluded_result_count": len(decisions) - eligible_count,
                    "exclusions": _exclusion_counts(decisions),
                    "waiting_jobs_requeued": waiting,
                    "superseded_version_id": (
                        str(supersedes_version_id)
                        if supersedes_version_id is not None
                        else None
                    ),
                }

    def _eligibility_decisions(
        self, connection: Any, profile_id: UUID
    ) -> list[EligibilityDecision]:
        rows = connection.execute(
            """
            SELECT result.result_id, result.result_status, result.bus_bw_gbps,
                   result.latency_us, result.error_code, decision.action
            FROM nccl_validation.evaluation_job AS job
            JOIN nccl_raw.node_result AS result USING (result_id)
            LEFT JOIN LATERAL (
                SELECT action
                FROM nccl_baseline.calibration_decision
                WHERE result_id = result.result_id
                ORDER BY decision_version DESC
                LIMIT 1
            ) AS decision ON TRUE
            WHERE job.profile_id = %s
            ORDER BY result.result_id
            """,
            (profile_id,),
        ).fetchall()
        return [
            assess_eligibility(
                row[0],
                result_status=row[1],
                bus_bw_gbps=row[2],
                latency_us=row[3],
                error_code=row[4],
                effective_calibration_action=row[5],
            )
            for row in rows
        ]

    def queue_report(self) -> dict[str, object]:
        with self.pool.connection() as connection:
            with connection.transaction():
                counts = dict(
                    connection.execute(
                        """
                        SELECT status, count(*) FROM nccl_validation.evaluation_job
                        GROUP BY status ORDER BY status
                        """
                    ).fetchall()
                )
                ready = connection.execute(
                    """
                    SELECT count(*) FROM nccl_validation.evaluation_job
                    WHERE status IN ('PENDING', 'RETRY') AND next_attempt_at <= now()
                    """
                ).fetchone()[0]
        return {"mode": "dry-run", "ready": ready, "status_counts": counts}

    def claim_jobs(
        self, worker_id: str, *, batch_size: int | None = None
    ) -> list[ClaimReceipt]:
        worker_id = validate_worker_id(worker_id)
        limit = self.config.evaluator_batch_size if batch_size is None else batch_size
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("batch_size must be between 1 and 1000")
        with self.pool.connection() as connection:
            with connection.transaction():
                candidates = connection.execute(
                    CLAIM_JOBS_SQL,
                    (self.config.evaluator_max_attempts, limit),
                ).fetchall()
                claims: list[ClaimReceipt] = []
                for candidate in candidates:
                    token = uuid4()
                    row = connection.execute(
                        CLAIM_JOB_SQL,
                        (worker_id, token, candidate[0]),
                    ).fetchone()
                    if row is None:
                        raise RuntimeError("locked evaluation job could not be claimed")
                    claims.append(
                        ClaimReceipt(
                            result_id=int(row[0]),
                            attempt_count=int(row[1]),
                            claim_token=row[2],
                            claimed_by=worker_id,
                        )
                    )
        return claims

    def evaluate_claimed(self, claim: ClaimReceipt) -> dict[str, object]:
        if not isinstance(claim, ClaimReceipt):
            raise TypeError("claim must be a ClaimReceipt")
        work_or_receipt = self._load_evaluation_work(claim)
        if isinstance(work_or_receipt, dict):
            return work_or_receipt
        work = work_or_receipt
        bus_class = classify(work.bus_bw_gbps, work.bus_ranges)
        latency_class = classify(work.latency_us, work.latency_ranges)
        bus_severity = piecewise_severity(
            work.bus_bw_gbps, work.bus_summary, higher_is_better=True
        )
        latency_severity = piecewise_severity(
            work.latency_us, work.latency_summary, higher_is_better=False
        )
        overall_class, overall_severity = overall_health(
            bus_class, latency_class, bus_severity, latency_severity
        )
        explanation = (
            "BUS_BW and LATENCY classified against immutable median-centered bands; "
            "overall uses the worse metric."
        )
        expected = (
            work.scope.value,
            bus_class,
            bus_severity,
            latency_class,
            latency_severity,
            overall_class,
            overall_severity,
            self.config.evaluator_version,
            None,
            explanation,
        )
        with self.pool.connection() as connection:
            with connection.transaction():
                inserted = connection.execute(
                    """
                    INSERT INTO nccl_validation.evaluation (
                        result_id, baseline_version_id, evaluation_scope,
                        bus_bw_class, bus_bw_severity_percentile,
                        latency_class, latency_severity_percentile,
                        overall_health_class, overall_severity_percentile,
                        evaluator_version, failure_code, explanation
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (result_id, baseline_version_id) DO NOTHING
                    RETURNING evaluation_id
                    """,
                    (claim.result_id, work.baseline_version_id, *expected),
                ).fetchone()
                if inserted is None:
                    existing = connection.execute(
                        """
                        SELECT evaluation_scope, bus_bw_class, bus_bw_severity_percentile,
                               latency_class, latency_severity_percentile,
                               overall_health_class, overall_severity_percentile,
                               evaluator_version, failure_code, explanation
                        FROM nccl_validation.evaluation
                        WHERE result_id = %s AND baseline_version_id = %s
                        """,
                        (claim.result_id, work.baseline_version_id),
                    ).fetchone()
                    if existing is None or tuple(existing) != expected:
                        raise RawConflictError(
                            "existing evaluation differs for result/baseline idempotency key"
                        )
                completed = connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET status = 'COMPLETED', completed_at = now(),
                        claimed_by = NULL, claimed_at = NULL, claim_token = NULL,
                        last_error = NULL
                    WHERE result_id = %s AND status = 'PROCESSING'
                      AND claimed_by = %s AND attempt_count = %s AND claim_token = %s
                    """,
                    (
                        claim.result_id,
                        claim.claimed_by,
                        claim.attempt_count,
                        claim.claim_token,
                    ),
                )
                if completed.rowcount != 1:
                    raise RuntimeError("evaluation job claim changed before atomic completion")
        return {
            "result_id": claim.result_id,
            "job_status": "COMPLETED",
            "baseline_version_id": str(work.baseline_version_id),
            "evaluation_scope": work.scope.value,
            "bus_bw_class": bus_class,
            "latency_class": latency_class,
            "overall_health_class": overall_class,
            "overall_severity_percentile": overall_severity,
        }

    def _load_evaluation_work(
        self, claim: ClaimReceipt
    ) -> EvaluationWork | dict[str, object]:
        if not isinstance(claim, ClaimReceipt):
            raise TypeError("claim must be a ClaimReceipt")
        with self.pool.connection() as connection:
            with connection.transaction():
                row = connection.execute(
                    """
                    SELECT result.result_status, result.bus_bw_gbps, result.latency_us,
                           profile.active_baseline_version_id
                    FROM nccl_validation.evaluation_job AS job
                    JOIN nccl_raw.node_result AS result USING (result_id)
                    JOIN nccl_baseline.baseline_profile AS profile USING (profile_id)
                    WHERE job.result_id = %s AND job.status = 'PROCESSING'
                      AND job.claimed_by = %s AND job.attempt_count = %s
                      AND job.claim_token = %s
                    FOR UPDATE OF job
                    """,
                    (
                        claim.result_id,
                        claim.claimed_by,
                        claim.attempt_count,
                        claim.claim_token,
                    ),
                ).fetchone()
                if row is None:
                    raise RuntimeError("evaluation job claim receipt is stale or invalid")
                if row[0] != ResultStatus.SUCCESS.value or row[1] is None or row[2] is None:
                    failed = connection.execute(
                        """
                        UPDATE nccl_validation.evaluation_job
                        SET status = 'FAILED', completed_at = now(),
                            claimed_by = NULL, claimed_at = NULL, claim_token = NULL,
                            last_error = %s
                        WHERE result_id = %s AND status = 'PROCESSING'
                          AND claimed_by = %s AND attempt_count = %s AND claim_token = %s
                        """,
                        (
                            f"RAW_RESULT_{row[0]}",
                            claim.result_id,
                            claim.claimed_by,
                            claim.attempt_count,
                            claim.claim_token,
                        ),
                    )
                    if failed.rowcount != 1:
                        raise RuntimeError("raw-failure claim changed before completion")
                    return {
                        "result_id": claim.result_id,
                        "job_status": "FAILED",
                        "failure_code": f"RAW_RESULT_{row[0]}",
                    }
                baseline_id = row[3]
                if baseline_id is None:
                    waiting = connection.execute(
                        """
                        UPDATE nccl_validation.evaluation_job
                        SET status = 'WAITING_FOR_BASELINE', claimed_by = NULL,
                            claimed_at = NULL, claim_token = NULL,
                            next_attempt_at = now(), last_error = NULL
                        WHERE result_id = %s AND status = 'PROCESSING'
                          AND claimed_by = %s AND attempt_count = %s AND claim_token = %s
                        """,
                        (
                            claim.result_id,
                            claim.claimed_by,
                            claim.attempt_count,
                            claim.claim_token,
                        ),
                    )
                    if waiting.rowcount != 1:
                        raise RuntimeError("waiting claim changed before transition")
                    return {
                        "result_id": claim.result_id,
                        "job_status": "WAITING_FOR_BASELINE",
                    }
                baseline = connection.execute(
                    """
                    SELECT baseline_version_id, bus_bw_mean, bus_bw_p05, bus_bw_p50,
                           bus_bw_p95, latency_mean, latency_p05, latency_p50, latency_p95
                    FROM nccl_baseline.baseline_version
                    WHERE baseline_version_id = %s AND status = 'ACTIVE'
                    """,
                    (baseline_id,),
                ).fetchone()
                if baseline is None:
                    raise RuntimeError("profile active baseline pointer is invalid")
                threshold_rows = connection.execute(
                    """
                    SELECT metric_name, class_id, lower_bound, upper_bound, unit
                    FROM nccl_baseline.metric_threshold
                    WHERE baseline_version_id = %s
                    ORDER BY metric_name, lower_bound
                    """,
                    (baseline_id,),
                ).fetchall()
                in_sample = connection.execute(
                    """
                    SELECT included FROM nccl_baseline.baseline_version_sample
                    WHERE baseline_version_id = %s AND result_id = %s
                    """,
                    (baseline_id, claim.result_id),
                ).fetchone()
        ranges = {
            metric: tuple(
                ThresholdRange(MetricName(item[0]), item[1], item[2], item[3], item[4])
                for item in threshold_rows
                if item[0] == metric.value
            )
            for metric in (MetricName.BUS_BW, MetricName.LATENCY)
        }
        return EvaluationWork(
            result_id=claim.result_id,
            baseline_version_id=baseline[0],
            bus_bw_gbps=float(row[1]),
            latency_us=float(row[2]),
            scope=(
                EvaluationScope.IN_SAMPLE
                if in_sample is not None and in_sample[0]
                else EvaluationScope.OUT_OF_SAMPLE
            ),
            bus_summary=DistributionSummary(0, baseline[1], baseline[2], baseline[3], baseline[4]),
            latency_summary=DistributionSummary(
                0, baseline[5], baseline[6], baseline[7], baseline[8]
            ),
            bus_ranges=ranges[MetricName.BUS_BW],
            latency_ranges=ranges[MetricName.LATENCY],
        )

    def schedule_retry(
        self, claim: ClaimReceipt, error: BaseException
    ) -> dict[str, object]:
        if not isinstance(claim, ClaimReceipt):
            raise TypeError("claim must be a ClaimReceipt")
        safe_error = self._safe_error(error)
        with self.pool.connection() as connection:
            with connection.transaction():
                row = connection.execute(
                    """
                    SELECT attempt_count FROM nccl_validation.evaluation_job
                    WHERE result_id = %s AND status = 'PROCESSING'
                      AND claimed_by = %s AND attempt_count = %s AND claim_token = %s
                    FOR UPDATE
                    """,
                    (
                        claim.result_id,
                        claim.claimed_by,
                        claim.attempt_count,
                        claim.claim_token,
                    ),
                ).fetchone()
                if row is None:
                    raise RuntimeError("cannot retry a job not claimed by this worker")
                attempts = int(row[0])
                if attempts >= self.config.evaluator_max_attempts:
                    status = "FAILED"
                    delay = 0.0
                    completed_at = "now()"
                else:
                    status = "RETRY"
                    delay = min(
                        self.config.evaluator_retry_max_seconds,
                        self.config.evaluator_retry_base_seconds * (2 ** max(0, attempts - 1)),
                    )
                    completed_at = "NULL"
                updated = connection.execute(
                    f"""
                    UPDATE nccl_validation.evaluation_job
                    SET status = %s, claimed_by = NULL, claimed_at = NULL,
                        claim_token = NULL,
                        next_attempt_at = now() + make_interval(secs => %s),
                        completed_at = {completed_at}, last_error = %s
                    WHERE result_id = %s AND status = 'PROCESSING'
                      AND claimed_by = %s AND attempt_count = %s AND claim_token = %s
                    """,
                    (
                        status,
                        delay,
                        safe_error,
                        claim.result_id,
                        claim.claimed_by,
                        claim.attempt_count,
                        claim.claim_token,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("evaluation job claim changed before retry")
        return {
            "result_id": claim.result_id,
            "job_status": status,
            "attempt_count": attempts,
            "retry_delay_seconds": delay,
            "error_code": type(error).__name__,
        }

    def stale_claim_report(self) -> dict[str, object]:
        with self.pool.connection() as connection:
            with connection.transaction():
                count = connection.execute(
                    """
                    SELECT count(*) FROM nccl_validation.evaluation_job
                    WHERE status = 'PROCESSING'
                      AND claimed_at < now() - make_interval(secs => %s)
                    """,
                    (self.config.evaluator_stale_claim_seconds,),
                ).fetchone()[0]
        return {"mode": "dry-run", "stale_claim_count": count}

    def recover_stale_claims(self) -> dict[str, object]:
        with self.pool.connection() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET status = CASE WHEN attempt_count >= %s THEN 'FAILED' ELSE 'RETRY' END,
                        claimed_by = NULL, claimed_at = NULL, claim_token = NULL,
                        next_attempt_at = now(),
                        completed_at = CASE WHEN attempt_count >= %s THEN now() ELSE NULL END,
                        last_error = 'Worker claim expired'
                    WHERE status = 'PROCESSING'
                      AND claimed_at < now() - make_interval(secs => %s)
                    RETURNING result_id, status, attempt_count
                    """,
                    (
                        self.config.evaluator_max_attempts,
                        self.config.evaluator_max_attempts,
                        self.config.evaluator_stale_claim_seconds,
                    ),
                ).fetchall()
        return {
            "mode": "apply",
            "recovered_count": len(rows),
            "jobs": [
                {"result_id": row[0], "job_status": row[1], "attempt_count": row[2]}
                for row in rows
            ],
        }

    def status(self, *, latest_limit: int = 20) -> dict[str, object]:
        if not 1 <= latest_limit <= 1000:
            raise ValueError("latest_limit must be between 1 and 1000")
        with self.pool.connection() as connection:
            with connection.transaction():
                job_counts = dict(
                    connection.execute(
                        "SELECT status, count(*) FROM nccl_validation.evaluation_job GROUP BY status"
                    ).fetchall()
                )
                profile_counts = dict(
                    connection.execute(
                        "SELECT status, count(*) FROM nccl_baseline.baseline_profile GROUP BY status"
                    ).fetchall()
                )
                profiles = connection.execute(
                    """
                    SELECT profile_id, profile_key, status, eligible_result_count,
                           last_built_sample_count, active_baseline_version_id
                    FROM nccl_baseline.baseline_profile ORDER BY profile_key
                    """
                ).fetchall()
                latest = connection.execute(
                    """
                    SELECT node_name, test_timestamp, profile_key,
                           baseline_version_number, overall_health_class,
                           overall_severity_percentile, evaluation_scope, evaluated_at
                    FROM nccl_validation.latest_result_view
                    ORDER BY evaluated_at DESC, node_name LIMIT %s
                    """,
                    (latest_limit,),
                ).fetchall()
        return {
            "job_counts": job_counts,
            "profile_counts": profile_counts,
            "profiles": [
                {
                    "profile_id": str(row[0]),
                    "profile_key": row[1],
                    "status": row[2],
                    "eligible_result_count": row[3],
                    "last_built_sample_count": row[4],
                    "active_baseline_version_id": str(row[5]) if row[5] else None,
                }
                for row in profiles
            ],
            "latest": [
                {
                    "node_name": row[0],
                    "test_timestamp": _json_scalar(row[1]),
                    "profile_key": row[2],
                    "baseline_version_number": row[3],
                    "overall_health_class": row[4],
                    "overall_severity_percentile": row[5],
                    "evaluation_scope": row[6],
                    "evaluated_at": _json_scalar(row[7]),
                }
                for row in latest
            ],
        }

    def _safe_error(self, error: BaseException) -> str:
        message = str(error).replace("\n", " ").replace("\r", " ")
        if self.config.database_url:
            message = message.replace(self.config.database_url, "[DATABASE_URL REDACTED]")
        message = _CREDENTIAL_URL.sub(r"\1[REDACTED]@", message)
        message = re.sub(
            r"\b(user|password|passfile|sslkey)=('(?:[^']|'')*'|\S+)",
            r"\1=[REDACTED]",
            message,
            flags=re.IGNORECASE,
        )
        return message[:1000] or type(error).__name__


def _executemany(connection: Any, sql: str, params: Sequence[Sequence[Any]]) -> None:
    """Run a parameterized Psycopg batch through the documented cursor API."""

    with connection.cursor() as cursor:
        cursor.executemany(sql, params)


def _exclusion_counts(decisions: Iterable[EligibilityDecision]) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in decisions:
        if item.exclusion_reason:
            result[item.exclusion_reason] = result.get(item.exclusion_reason, 0) + 1
    return dict(sorted(result.items()))


def _finite_positive(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(float(value))
        and float(value) > 0.0
    )


def _scanned_outbox_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 255
        and value.endswith(".json")
        and "\x00" not in value
        and "/" not in value
    )


def _validate_calibration_transition(
    current_action: str | None, requested_action: str
) -> None:
    if requested_action == "APPROVE" and current_action == "APPROVE":
        raise ValueError("result is already effectively approved")
    if requested_action == "REVOKE" and current_action != "APPROVE":
        raise ValueError("REVOKE requires an effective APPROVE decision")


def _json_scalar(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    return value
