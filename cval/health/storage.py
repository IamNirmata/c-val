"""Exact, versioned SQLite persistence for U8 health-class candidates."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sqlite3
import stat
import time
from contextlib import closing
from functools import lru_cache
from pathlib import Path
from typing import Any

from cval.health.combination import validate_environment_combination
from cval.health.engine import (
    assert_candidate_integrity,
    build_candidate_from_plugin,
    _candidate_payload,
    evaluate_build_trigger,
    validate_candidate,
    validate_stored_baseline,
)
from cval.health.models import (
    BaselineLifecycle,
    EnvironmentCombination,
    HealthBuildState,
    HealthCandidate,
    HealthContext,
    HEALTH_CLASS_DEFINITIONS,
    HealthClassCode,
    MetricBaseline,
    MetricObservation,
    QualityGate,
    QualityReport,
    ResultSampleCoverage,
    SourceCoverage,
    SourceResult,
    SourceSnapshot,
    StoredHealthBaseline,
    ThresholdBand,
)
from cval.storage.paths import safe_writable_file_path
from cval.validation.registry import ValidationTestDefinition


HEALTH_SCHEMA_VERSION = 1
_MIGRATION_NAME = "initial-versioned-health-engine"
_ACTIVATION_KEY_BYTES = 32


def _activation_key_path(db_path: Path) -> Path:
    return safe_writable_file_path(
        db_path.with_name(f"{db_path.name}.activation.key"),
        allowed_root=db_path.parent,
        description="health activation key",
    )


def _load_activation_key(db_path: Path, *, create: bool = False) -> bytes:
    key_path = _activation_key_path(db_path)
    if create and not key_path.exists():
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(key_path, flags, 0o600)
        except FileExistsError:
            pass
        else:
            try:
                key = secrets.token_bytes(_ACTIVATION_KEY_BYTES)
                written = 0
                while written < len(key):
                    written += os.write(descriptor, key[written:])
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            directory_descriptor = os.open(key_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    if not key_path.is_file() or key_path.is_symlink():
        raise RuntimeError("Health activation key is missing or unsafe")
    mode = stat.S_IMODE(key_path.stat().st_mode)
    if mode & 0o077:
        raise RuntimeError("Health activation key permissions must be owner-only")
    key = key_path.read_bytes()
    if len(key) != _ACTIVATION_KEY_BYTES:
        raise RuntimeError("Health activation key has an invalid length")
    return key


def _activation_key_digest(key: bytes) -> str:
    return f"sha256:{hashlib.sha256(key).hexdigest()}"


def _connection_database_path(connection: sqlite3.Connection) -> Path:
    rows = connection.execute("PRAGMA database_list").fetchall()
    main = next((row for row in rows if row[1] == "main"), None)
    if main is None or not isinstance(main[2], str) or not main[2]:
        raise RuntimeError("Health database has no filesystem-backed main path")
    return safe_writable_file_path(main[2], description="health database")


def _activation_signature(
    candidate: HealthCandidate,
    activated_at: int | None,
    quality: QualityReport,
    key: bytes,
) -> str:
    if activated_at is None:
        raise RuntimeError("Activation signature requires an activation timestamp")
    payload = json.dumps(
        {
            "baseline_id": candidate.baseline_id,
            "parent_baseline_id": candidate.parent_baseline_id,
            "test_id": candidate.test_id,
            "combination_key": candidate.combination.key,
            "test_config_digest": candidate.test_config_digest,
            "health_policy_version": candidate.health_policy_version,
            "adapter_schema_version": candidate.adapter_schema_version,
            "evaluator_version": candidate.evaluator_version,
            "activated_at": activated_at,
            "quality_json": _quality_json(quality),
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "hmac-sha256:" + hmac.new(key, payload, hashlib.sha256).hexdigest()


def store_candidate_from_plugin(
    plugin: Any,
    context: HealthContext,
    *,
    db_path: str | Path,
    now: int | None = None,
) -> HealthCandidate:
    """Load canonical adapter observations, build, and persist one candidate."""

    candidate = build_candidate_from_plugin(plugin, context)
    _store_candidate(
        candidate,
        context.definition,
        db_path=db_path,
        now=now,
        robust_z_threshold=context.robust_z_threshold,
    )
    return candidate


def _store_candidate(
    candidate: HealthCandidate,
    definition: ValidationTestDefinition,
    *,
    db_path: str | Path,
    now: int | None = None,
    robust_z_threshold: float | None = None,
) -> bool:
    """Store one immutable candidate; return false for an exact content retry."""

    assert_candidate_integrity(candidate)
    quality = validate_candidate(
        candidate,
        definition,
        robust_z_threshold=robust_z_threshold,
    )
    if candidate.test_id != definition.metadata.id:
        raise ValueError("Candidate owner does not match the health DB definition")
    health = definition.health
    assert health is not None
    if len(candidate.source_snapshot.results) < health.min_samples:
        raise ValueError(
            "Health candidate build trigger is not satisfied: insufficient_samples"
        )
    timestamp = _non_negative_int(
        int(time.time()) if now is None else now,
        "health candidate storage timestamp",
    )
    if timestamp < candidate.created_at:
        raise ValueError("Health candidate storage timestamp precedes candidate creation")
    path = safe_writable_file_path(db_path, description="health database")
    if not path.exists():
        if candidate.parent_baseline_id is not None:
            raise ValueError(
                "Initial health candidate cannot declare a lifecycle parent"
            )
        initial_decision = evaluate_build_trigger(
            candidate.source_snapshot.result_ids,
            (),
            min_samples=health.min_samples,
            min_new_results=health.min_new_results,
        )
        if not initial_decision.eligible:
            raise ValueError(
                "Health candidate build trigger is not satisfied: "
                + ", ".join(initial_decision.reasons)
            )
        if _activation_key_path(path).exists():
            raise RuntimeError(
                "Health activation key exists without its database"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path = safe_writable_file_path(path, description="health database")

    with closing(sqlite3.connect(f"file:{path}?mode=rwc", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA recursive_triggers=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        database_was_empty = _database_is_empty(connection)
        activation_key = _load_activation_key(path, create=database_was_empty)
        activation_key_digest = _activation_key_digest(activation_key)
        if not database_was_empty:
            _validate_schema(connection)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        try:
            _prepare_schema(connection)
            _validate_schema(connection)
            owner = connection.execute(
                "SELECT test_id, activation_key_digest "
                "FROM health_database_owner WHERE id=1"
            ).fetchone()
            if owner is None:
                if not database_was_empty:
                    raise RuntimeError(
                        "Existing health database lacks its immutable owner"
                    )
                connection.execute(
                    "INSERT INTO health_database_owner("
                    "id, test_id, activation_key_digest) VALUES (1, ?, ?)",
                    (candidate.test_id, activation_key_digest),
                )
            elif (
                _nonempty_db_string(owner[0], "health database owner")
                != candidate.test_id
            ):
                raise ValueError("Health database is owned by a different validation test")
            elif _nonempty_db_string(
                owner[1],
                "health activation key digest",
            ) != activation_key_digest:
                raise ValueError(
                    "Health database activation key binding is invalid"
                )
            existing = _load_stored_from_connection(connection, candidate.baseline_id)
            if existing is not None:
                if _candidate_payload(existing.candidate) != _candidate_payload(candidate):
                    raise RuntimeError(
                        "Health candidate identity collides with different persisted content"
                    )
                validate_stored_baseline(
                    existing,
                    definition,
                    robust_z_threshold=robust_z_threshold,
                )
                _validate_candidate_trigger_state(connection, existing, definition)
                connection.rollback()
                return False
            digest_collision = connection.execute(
                "SELECT baseline_id FROM health_baselines WHERE payload_digest=?",
                (candidate.payload_digest,),
            ).fetchone()
            if digest_collision is not None and _nonempty_db_string(
                digest_collision[0], "payload digest baseline_id"
            ) != candidate.baseline_id:
                raise RuntimeError("Health candidate payload digest is already bound elsewhere")
            active_row = connection.execute(
                """
                SELECT baseline_id FROM health_baselines
                WHERE test_id=? AND combination_key=? AND lifecycle_state='active'
                """,
                (candidate.test_id, candidate.combination.key),
            ).fetchone()
            current_active_id = (
                _nonempty_db_string(active_row[0], "active baseline_id")
                if active_row is not None
                else None
            )
            if candidate.parent_baseline_id != current_active_id:
                raise ValueError(
                    "Health candidate lifecycle parent does not match current active baseline"
                )
            if current_active_id is not None:
                active_stored = _load_stored_from_connection(
                    connection,
                    current_active_id,
                )
                if active_stored is None:
                    raise RuntimeError("Current active health baseline is missing")
                validate_stored_baseline(
                    active_stored,
                    definition,
                    robust_z_threshold=robust_z_threshold,
                )
                if (
                    active_stored.activated_at is None
                    or active_stored.activated_at > candidate.created_at
                    or active_stored.updated_at > candidate.created_at
                ):
                    raise ValueError(
                        "Health candidate creation/storage precedes its active parent"
                    )
            previous_candidate_id = _latest_candidate_id_from_chain(
                connection,
                candidate.test_id,
                candidate.combination.key,
            )
            previous_ids = _baseline_source_ids(
                connection,
                previous_candidate_id,
                expected_test_id=candidate.test_id,
                expected_combination_key=candidate.combination.key,
            )
            build_decision = evaluate_build_trigger(
                candidate.source_snapshot.result_ids,
                previous_ids,
                min_samples=health.min_samples,
                min_new_results=health.min_new_results,
            )
            if not build_decision.eligible:
                raise ValueError(
                    "Health candidate build trigger is not satisfied: "
                    + ", ".join(build_decision.reasons)
                )
            quality_json = _quality_json(quality)
            connection.execute(
                """
                INSERT INTO health_baselines (
                    baseline_id, payload_digest, test_id, combination_key,
                    combination_factors_json, lifecycle_state, method,
                    robust_z_threshold, observation_digest,
                    source_result_count, excluded_result_count,
                    source_first_timestamp, source_last_timestamp,
                    source_max_result_id, test_config_digest,
                    health_policy_version, adapter_schema_version,
                    evaluator_version,
                    parent_baseline_id, created_at, updated_at,
                    activated_at, superseded_at, quality_json
                ) VALUES (?, ?, ?, ?, ?, 'candidate', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
                """,
                (
                    candidate.baseline_id,
                    candidate.payload_digest,
                    candidate.test_id,
                    candidate.combination.key,
                    candidate.combination.factors_json,
                    candidate.method,
                    candidate.robust_z_threshold,
                    candidate.observation_digest,
                    len(candidate.source_snapshot.results),
                    candidate.excluded_result_count,
                    candidate.source_snapshot.first_timestamp,
                    candidate.source_snapshot.last_timestamp,
                    candidate.source_snapshot.max_result_id,
                    candidate.test_config_digest,
                    candidate.health_policy_version,
                    candidate.adapter_schema_version,
                    candidate.evaluator_version,
                    candidate.parent_baseline_id,
                    candidate.created_at,
                    timestamp,
                    quality_json,
                ),
            )
            connection.execute(
                """
                INSERT INTO health_candidate_triggers (
                    baseline_id, previous_candidate_id,
                    min_samples, min_new_results,
                    qualifying_result_count, new_result_count
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.baseline_id,
                    previous_candidate_id,
                    health.min_samples,
                    health.min_new_results,
                    build_decision.qualifying_count,
                    build_decision.new_result_count,
                ),
            )
            connection.executemany(
                """
                INSERT INTO health_baseline_sources (
                    baseline_id, result_id, run_id, completed_timestamp,
                    result_digest, raw_result_digest,
                    test_config_digest, combination_key,
                    adapter_schema_version, receipt_evidence_digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        candidate.baseline_id,
                        result.result_id,
                        result.run_id,
                        result.completed_timestamp,
                        result.result_digest,
                        result.raw_result_digest,
                        result.test_config_digest,
                        result.combination_key,
                        result.adapter_schema_version,
                        result.receipt_evidence_digest,
                    )
                    for result in candidate.source_snapshot.results
                ],
            )
            connection.executemany(
                """
                INSERT INTO health_observations (
                    baseline_id, result_id, run_id, completed_timestamp,
                    source, metric_name, sample_key, value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        candidate.baseline_id,
                        observation.result_id,
                        observation.run_id,
                        observation.completed_timestamp,
                        observation.source,
                        observation.metric_name,
                        observation.sample_key,
                        observation.value,
                    )
                    for observation in candidate.observations
                ],
            )
            connection.executemany(
                """
                INSERT INTO health_source_coverage (
                    baseline_id, source, metric_name, result_id, sample_key
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        candidate.baseline_id,
                        coverage.source,
                        coverage.metric_name,
                        result.result_id,
                        sample_key,
                    )
                    for coverage in candidate.source_coverage
                    for result in coverage.results
                    for sample_key in result.sample_keys
                ],
            )
            for metric in candidate.metrics:
                connection.execute(
                    """
                    INSERT INTO health_metric_statistics (
                        baseline_id, spec_name, source, metric_name, direction,
                        units, weight, tolerance_pct, center, mad, mad_sigma,
                        delta, p05, p95,
                        sample_count, excluded_count, statistics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.baseline_id,
                        metric.spec_name,
                        metric.source,
                        metric.metric_name,
                        metric.direction,
                        metric.units,
                        metric.weight,
                        metric.tolerance_pct,
                        metric.center,
                        metric.mad,
                        metric.mad_sigma,
                        metric.delta,
                        metric.p05,
                        metric.p95,
                        metric.sample_count,
                        metric.excluded_count,
                        metric.statistics_json,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO health_thresholds (
                        baseline_id, spec_name, source, metric_name,
                        class_code, band_index, lower_bound, upper_bound,
                        lower_inclusive, upper_inclusive
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            candidate.baseline_id,
                            metric.spec_name,
                            metric.source,
                            metric.metric_name,
                            int(band.class_code),
                            band.band_index,
                            band.lower_bound,
                            band.upper_bound,
                            int(band.lower_inclusive),
                            int(band.upper_inclusive),
                        )
                        for band in metric.thresholds
                    ],
                )
            current_ids = set(candidate.source_snapshot.result_ids)
            connection.execute(
                """
                INSERT INTO health_build_state (
                    test_id, combination_key, last_seen_result_id,
                    last_candidate_id, qualifying_result_count, new_result_count,
                    last_checked_at, last_built_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '')
                ON CONFLICT(test_id, combination_key) DO UPDATE SET
                    last_seen_result_id=excluded.last_seen_result_id,
                    last_candidate_id=excluded.last_candidate_id,
                    qualifying_result_count=excluded.qualifying_result_count,
                    new_result_count=excluded.new_result_count,
                    last_checked_at=excluded.last_checked_at,
                    last_built_at=excluded.last_built_at,
                    last_error=''
                """,
                (
                    candidate.test_id,
                    candidate.combination.key,
                    candidate.source_snapshot.max_result_id,
                    candidate.baseline_id,
                    len(current_ids),
                    build_decision.new_result_count,
                    timestamp,
                    timestamp,
                ),
            )
            reloaded = _load_stored_from_connection(connection, candidate.baseline_id)
            if (
                reloaded is None
                or _candidate_payload(reloaded.candidate) != _candidate_payload(candidate)
                or reloaded.candidate.created_at != candidate.created_at
                or _quality_json(reloaded.quality) != quality_json
                or reloaded.updated_at != timestamp
            ):
                raise RuntimeError("Stored health candidate failed durable round-trip validation")
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise


def activate_candidate(
    baseline_id: str,
    definition: ValidationTestDefinition,
    *,
    db_path: str | Path,
    now: int | None = None,
    robust_z_threshold: float | None = None,
) -> bool:
    """Activate a quality-approved candidate and atomically supersede its parent."""

    _require_baseline_id(baseline_id)
    timestamp = _non_negative_int(
        int(time.time()) if now is None else now,
        "health activation timestamp",
    )
    path = safe_writable_file_path(db_path, description="health database")
    if not path.is_file():
        raise FileNotFoundError(f"Health database not found: {path}")
    with closing(sqlite3.connect(f"file:{path}?mode=rw", uri=True, timeout=30)) as connection:
        activation_key = _load_activation_key(path)
        connection.create_function("cval_activation_authorized", 0, lambda: 1)
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA recursive_triggers=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        _validate_schema(connection)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        try:
            _validate_schema(connection)
            stored = _load_stored_from_connection(connection, baseline_id)
            if stored is None:
                raise KeyError(f"Health candidate not found: {baseline_id}")
            report = validate_stored_baseline(
                stored,
                definition,
                robust_z_threshold=robust_z_threshold,
            )
            if stored.lifecycle is BaselineLifecycle.ACTIVE:
                _validate_candidate_trigger_state(connection, stored, definition)
                connection.rollback()
                return False
            if stored.lifecycle is not BaselineLifecycle.CANDIDATE:
                raise ValueError("Only candidate health baselines may be activated")
            _validate_candidate_trigger_state(connection, stored, definition)
            if timestamp < stored.updated_at:
                raise ValueError("Health activation timestamp precedes candidate storage")
            if not report.activation_ready:
                failed = [gate.name for gate in report.gates if not gate.passed]
                raise ValueError(
                    "Health candidate failed activation quality gates: "
                    + ", ".join(failed)
                )
            if _quality_json(report) != _quality_json(stored.quality):
                raise RuntimeError("Stored health candidate quality report is stale or corrupt")
            health = definition.health
            assert health is not None
            active = connection.execute(
                """
                SELECT baseline_id FROM health_baselines
                WHERE test_id=? AND combination_key=? AND lifecycle_state='active'
                """,
                (stored.candidate.test_id, stored.candidate.combination.key),
            ).fetchone()
            current_active = (
                _nonempty_db_string(active[0], "active baseline_id")
                if active
                else None
            )
            if stored.candidate.parent_baseline_id != current_active:
                raise ValueError(
                    "Health candidate lifecycle parent does not match the current active baseline"
                )
            connection.execute(
                """
                INSERT INTO health_activation_evidence (
                    baseline_id, test_id, combination_key,
                    test_config_digest, health_policy_version,
                    adapter_schema_version, evaluator_version,
                    activated_at, quality_json
                    , signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    baseline_id,
                    stored.candidate.test_id,
                    stored.candidate.combination.key,
                    stored.candidate.test_config_digest,
                    stored.candidate.health_policy_version,
                    stored.candidate.adapter_schema_version,
                    stored.candidate.evaluator_version,
                    timestamp,
                    _quality_json(report),
                    _activation_signature(
                        stored.candidate,
                        timestamp,
                        report,
                        activation_key,
                    ),
                ),
            )
            if current_active is not None:
                active_stored = _load_stored_from_connection(connection, current_active)
                if active_stored is None:
                    raise RuntimeError("Current active health baseline is missing")
                validate_stored_baseline(
                    active_stored,
                    definition,
                    robust_z_threshold=robust_z_threshold,
                )
                if (
                    active_stored.activated_at is None
                    or timestamp < active_stored.activated_at
                    or active_stored.activated_at > stored.candidate.created_at
                    or active_stored.updated_at > stored.candidate.created_at
                ):
                    raise ValueError(
                        "Health activation timestamp precedes the active parent"
                    )
                connection.execute(
                    """
                    UPDATE health_baselines
                    SET lifecycle_state='superseded', updated_at=?, superseded_at=?
                    WHERE baseline_id=? AND lifecycle_state='active'
                    """,
                    (timestamp, timestamp, current_active),
                )
                if connection.execute("SELECT changes()").fetchone()[0] != 1:
                    raise RuntimeError("Could not atomically supersede the active baseline")
            connection.execute(
                """
                UPDATE health_baselines
                SET lifecycle_state='active', updated_at=?, activated_at=?
                WHERE baseline_id=? AND lifecycle_state='candidate'
                """,
                (timestamp, timestamp, baseline_id),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise RuntimeError("Could not atomically activate the health candidate")
            count = connection.execute(
                """
                SELECT COUNT(*) FROM health_baselines
                WHERE test_id=? AND combination_key=? AND lifecycle_state='active'
                """,
                (stored.candidate.test_id, stored.candidate.combination.key),
            ).fetchone()[0]
            if count != 1:
                raise RuntimeError("Health activation violated the one-active invariant")
            _validate_schema(connection)
            activated = _load_stored_from_connection(connection, baseline_id)
            if activated is None:
                raise RuntimeError("Activated health baseline disappeared")
            validate_stored_baseline(
                activated,
                definition,
                robust_z_threshold=robust_z_threshold,
            )
            if current_active is not None:
                superseded = _load_stored_from_connection(connection, current_active)
                if superseded is None:
                    raise RuntimeError("Superseded health baseline disappeared")
                validate_stored_baseline(
                    superseded,
                    definition,
                    robust_z_threshold=robust_z_threshold,
                )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise


def load_baseline(
    baseline_id: str,
    *,
    db_path: str | Path,
) -> StoredHealthBaseline | None:
    _require_baseline_id(baseline_id)
    path = safe_writable_file_path(db_path, description="health database")
    if not path.is_file():
        return None
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        _begin_read_snapshot(connection)
        _validate_schema(connection)
        _require_health_db_owner(connection)
        return _load_stored_from_connection(connection, baseline_id)


def get_active_baseline(
    test_id: str,
    combination_key: str,
    *,
    db_path: str | Path,
) -> StoredHealthBaseline | None:
    _require_test_id(test_id)
    _require_digest(combination_key, "combination_key")
    path = safe_writable_file_path(db_path, description="health database")
    if not path.is_file():
        return None
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        _begin_read_snapshot(connection)
        _validate_schema(connection)
        _require_health_db_owner(connection, test_id)
        row = connection.execute(
            """
            SELECT baseline_id FROM health_baselines
            WHERE test_id=? AND combination_key=? AND lifecycle_state='active'
            """,
            (test_id, combination_key),
        ).fetchone()
        stored = (
            _load_stored_from_connection(
                connection,
                _nonempty_db_string(row[0], "active baseline_id"),
            )
            if row
            else None
        )
        if stored is not None and stored.lifecycle is not BaselineLifecycle.ACTIVE:
            raise RuntimeError("Selected active health baseline changed lifecycle")
        return stored


def list_baselines(
    *,
    db_path: str | Path,
    test_id: str | None = None,
    combination_key: str | None = None,
) -> list[StoredHealthBaseline]:
    if test_id is not None:
        _require_test_id(test_id)
    if combination_key is not None:
        _require_digest(combination_key, "combination_key")
    path = safe_writable_file_path(db_path, description="health database")
    if not path.is_file():
        return []
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        _begin_read_snapshot(connection)
        _validate_schema(connection)
        owner = _require_health_db_owner(connection)
        if test_id is not None and test_id != owner:
            raise ValueError("Health database selector does not match its owner")
        clauses: list[str] = []
        params: list[str] = []
        if test_id is not None:
            clauses.append("test_id=?")
            params.append(test_id)
        if combination_key is not None:
            clauses.append("combination_key=?")
            params.append(combination_key)
        query = "SELECT baseline_id FROM health_baselines"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC, baseline_id"
        return [
            stored
            for row in connection.execute(query, params)
            if (
                stored := _load_stored_from_connection(
                    connection,
                    _nonempty_db_string(row[0], "listed baseline_id"),
                )
            )
            is not None
        ]


def load_build_state(
    test_id: str,
    combination_key: str,
    *,
    db_path: str | Path,
) -> HealthBuildState | None:
    _require_test_id(test_id)
    _require_digest(combination_key, "combination_key")
    path = safe_writable_file_path(db_path, description="health database")
    if not path.is_file():
        return None
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        _begin_read_snapshot(connection)
        _validate_schema(connection)
        _require_health_db_owner(connection, test_id)
        row = connection.execute(
            """
            SELECT last_seen_result_id, last_candidate_id,
                   qualifying_result_count, new_result_count,
                   last_checked_at, last_built_at, last_error
            FROM health_build_state WHERE test_id=? AND combination_key=?
            """,
            (test_id, combination_key),
        ).fetchone()
        if row is None:
            return None
        candidate_id = (
            _nonempty_db_string(row[1], "last_candidate_id")
            if row[1] is not None
            else None
        )
        return HealthBuildState(
            test_id=test_id,
            combination_key=combination_key,
            last_seen_result_id=(
                _non_negative_int(row[0], "last_seen_result_id")
                if row[0] is not None
                else None
            ),
            last_candidate_id=candidate_id,
            qualifying_result_count=_non_negative_int(
                row[2], "qualifying_result_count"
            ),
            new_result_count=_non_negative_int(row[3], "new_result_count"),
            last_checked_at=(
                _non_negative_int(row[4], "last_checked_at")
                if row[4] is not None
                else None
            ),
            last_built_at=(
                _non_negative_int(row[5], "last_built_at")
                if row[5] is not None
                else None
            ),
            last_error=_db_string(row[6], "last_error"),
            candidate_source_result_ids=tuple(
                sorted(
                    _baseline_source_ids(
                        connection,
                        candidate_id,
                        expected_test_id=test_id,
                        expected_combination_key=combination_key,
                    )
                )
            ),
        )


def _load_stored_from_connection(
    connection: sqlite3.Connection,
    baseline_id: str,
) -> StoredHealthBaseline | None:
    row = connection.execute(
        """
        SELECT payload_digest, test_id, combination_key,
               combination_factors_json, lifecycle_state, method,
                             robust_z_threshold, observation_digest,
             source_result_count, excluded_result_count,
             source_first_timestamp, source_last_timestamp,
                         source_max_result_id, test_config_digest,
                         health_policy_version, adapter_schema_version, evaluator_version,
             parent_baseline_id, created_at, updated_at,
             activated_at, superseded_at, quality_json
        FROM health_baselines WHERE baseline_id=?
        """,
        (baseline_id,),
    ).fetchone()
    if row is None:
        return None
    combination = EnvironmentCombination(
        _nonempty_db_string(row[2], "combination_key"),
        _nonempty_db_string(row[3], "combination_factors_json"),
    )
    validate_environment_combination(combination)
    sources = tuple(
        SourceResult(
            _positive_int(source[0], "source result_id"),
            _nonempty_db_string(source[1], "source run_id"),
            _non_negative_int(source[2], "source completed_timestamp"),
            _nonempty_db_string(source[3], "source result_digest"),
            _nonempty_db_string(source[4], "source raw_result_digest"),
            _nonempty_db_string(source[5], "source test_config_digest"),
            _nonempty_db_string(source[6], "source combination_key"),
            _positive_int(source[7], "source adapter_schema_version"),
            _nonempty_db_string(source[8], "source receipt_evidence_digest"),
        )
        for source in connection.execute(
            """
                 SELECT result_id, run_id, completed_timestamp,
                     result_digest, raw_result_digest,
                     test_config_digest, combination_key,
                     adapter_schema_version, receipt_evidence_digest
            FROM health_baseline_sources WHERE baseline_id=? ORDER BY result_id
            """,
            (baseline_id,),
        )
    )
    snapshot = SourceSnapshot(sources)
    observations = tuple(
        MetricObservation(
            result_id=_positive_int(observation[0], "observation result_id"),
            run_id=_nonempty_db_string(observation[1], "observation run_id"),
            completed_timestamp=_non_negative_int(
                observation[2],
                "observation completed_timestamp",
            ),
            source=_nonempty_db_string(observation[3], "observation source"),
            metric_name=_nonempty_db_string(
                observation[4],
                "observation metric_name",
            ),
            sample_key=_nonempty_db_string(
                observation[5],
                "observation sample_key",
            ),
            value=_finite_db_number(observation[6], "observation value"),
        )
        for observation in connection.execute(
            """
            SELECT result_id, run_id, completed_timestamp, source,
                   metric_name, sample_key, value
            FROM health_observations WHERE baseline_id=?
            ORDER BY result_id, source, metric_name, sample_key
            """,
            (baseline_id,),
        )
    )
    persisted_source_identity = (
        _non_negative_int(row[8], "source_result_count"),
        _non_negative_int(row[10], "source_first_timestamp")
        if row[10] is not None
        else None,
        _non_negative_int(row[11], "source_last_timestamp")
        if row[11] is not None
        else None,
        _positive_int(row[12], "source_max_result_id")
        if row[12] is not None
        else None,
    )
    if persisted_source_identity != (
        len(sources),
        snapshot.first_timestamp,
        snapshot.last_timestamp,
        snapshot.max_result_id,
    ):
        raise RuntimeError("Health baseline source metadata does not match source rows")
    metrics: list[MetricBaseline] = []
    source_coverage_rows = connection.execute(
        """
        SELECT source, metric_name, result_id, sample_key
        FROM health_source_coverage
        WHERE baseline_id=? ORDER BY source, metric_name, result_id, sample_key
        """,
        (baseline_id,),
    ).fetchall()
    coverage_map: dict[tuple[str, str], dict[int, list[str]]] = {}
    for source, metric_name, result_id, sample_key in source_coverage_rows:
        coverage_map.setdefault(
            (
                _nonempty_db_string(source, "coverage source"),
                _nonempty_db_string(metric_name, "coverage metric_name"),
            ),
            {},
        ).setdefault(
            _positive_int(result_id, "coverage result_id"),
            [],
        ).append(_nonempty_db_string(sample_key, "coverage sample_key"))
    source_coverage = tuple(
        SourceCoverage(
            source,
            metric_name,
            tuple(
                ResultSampleCoverage(result_id, tuple(sample_keys))
                for result_id, sample_keys in sorted(results.items())
            ),
        )
        for (source, metric_name), results in sorted(coverage_map.items())
    )
    metric_rows = connection.execute(
        """
        SELECT spec_name, source, metric_name, direction, units, weight,
             tolerance_pct, center, mad, mad_sigma, delta, p05, p95,
               sample_count, excluded_count, statistics_json
        FROM health_metric_statistics WHERE baseline_id=?
        ORDER BY source, metric_name
        """,
        (baseline_id,),
    ).fetchall()
    for metric_row in metric_rows:
        metric_source = _nonempty_db_string(metric_row[1], "metric source")
        metric_name = _nonempty_db_string(metric_row[2], "metric_name")
        threshold_rows = connection.execute(
                """
                SELECT spec_name, class_code, band_index, lower_bound, upper_bound,
                       lower_inclusive, upper_inclusive
                FROM health_thresholds
                WHERE baseline_id=? AND source=? AND metric_name=?
                ORDER BY class_code, band_index
                """,
                (baseline_id, metric_source, metric_name),
            ).fetchall()
        metric_spec_name = _nonempty_db_string(metric_row[0], "metric spec_name")
        if any(
            _nonempty_db_string(band[0], "threshold spec_name") != metric_spec_name
            for band in threshold_rows
        ):
            raise RuntimeError("Health threshold spec ownership is invalid")
        thresholds = tuple(
            ThresholdBand(
                class_code=HealthClassCode(
                    _non_negative_int(band[1], "threshold class_code")
                ),
                band_index=_non_negative_int(band[2], "threshold band_index"),
                lower_bound=(
                    _finite_db_number(band[3], "threshold lower_bound")
                    if band[3] is not None
                    else None
                ),
                upper_bound=(
                    _finite_db_number(band[4], "threshold upper_bound")
                    if band[4] is not None
                    else None
                ),
                lower_inclusive=bool(
                    _binary_int(band[5], "threshold lower_inclusive")
                ),
                upper_inclusive=bool(
                    _binary_int(band[6], "threshold upper_inclusive")
                ),
            )
            for band in threshold_rows
        )
        metrics.append(
            MetricBaseline(
                spec_name=metric_spec_name,
                source=metric_source,
                metric_name=metric_name,
                direction=_nonempty_db_string(metric_row[3], "metric direction"),
                units=_db_string(metric_row[4], "metric units"),
                weight=_finite_db_number(metric_row[5], "metric weight"),
                tolerance_pct=_finite_db_number(
                    metric_row[6], "metric tolerance_pct"
                ),
                center=_finite_db_number(metric_row[7], "metric center"),
                mad=_finite_db_number(metric_row[8], "metric mad"),
                mad_sigma=_finite_db_number(metric_row[9], "metric mad_sigma"),
                delta=_finite_db_number(metric_row[10], "metric delta"),
                p05=_finite_db_number(metric_row[11], "metric p05"),
                p95=_finite_db_number(metric_row[12], "metric p95"),
                sample_count=_positive_int(metric_row[13], "metric sample_count"),
                excluded_count=_non_negative_int(
                    metric_row[14], "metric excluded_count"
                ),
                statistics_json=_nonempty_db_string(
                    metric_row[15], "metric statistics_json"
                ),
                thresholds=thresholds,
            )
        )
    candidate = HealthCandidate(
        baseline_id=baseline_id,
        payload_digest=_nonempty_db_string(row[0], "payload_digest"),
        test_id=_nonempty_db_string(row[1], "test_id"),
        combination=combination,
        test_config_digest=_nonempty_db_string(row[13], "test_config_digest"),
        health_policy_version=_nonempty_db_string(
            row[14], "health_policy_version"
        ),
        adapter_schema_version=_positive_int(
            row[15], "adapter_schema_version"
        ),
        evaluator_version=_nonempty_db_string(row[16], "evaluator_version"),
        method=_nonempty_db_string(row[5], "health method"),
        robust_z_threshold=_finite_db_number(row[6], "robust_z_threshold"),
        observation_digest=_nonempty_db_string(row[7], "observation_digest"),
        source_snapshot=snapshot,
        observations=observations,
        source_coverage=source_coverage,
        metrics=tuple(metrics),
        parent_baseline_id=(
            _nonempty_db_string(row[17], "parent_baseline_id")
            if row[17] is not None
            else None
        ),
        created_at=_non_negative_int(row[18], "baseline created_at"),
        excluded_result_count=_non_negative_int(
            row[9], "baseline excluded_result_count"
        ),
    )
    assert_candidate_integrity(candidate)
    quality = _parse_quality_json(
        _nonempty_db_string(row[22], "quality_json")
    )
    lifecycle = BaselineLifecycle(
        _nonempty_db_string(row[4], "lifecycle_state")
    )
    activated_at = (
        _non_negative_int(row[20], "baseline activated_at")
        if row[20] is not None
        else None
    )
    superseded_at = (
        _non_negative_int(row[21], "baseline superseded_at")
        if row[21] is not None
        else None
    )
    lifecycle_timestamps_valid = (
        lifecycle is BaselineLifecycle.CANDIDATE
        and activated_at is None
        and superseded_at is None
    ) or (
        lifecycle is BaselineLifecycle.ACTIVE
        and activated_at is not None
        and superseded_at is None
    ) or (
        lifecycle is BaselineLifecycle.SUPERSEDED
        and activated_at is not None
        and superseded_at is not None
    )
    if not lifecycle_timestamps_valid:
        raise RuntimeError("Health baseline lifecycle timestamps are inconsistent")
    stored = StoredHealthBaseline(
        candidate=candidate,
        lifecycle=lifecycle,
        quality=quality,
        updated_at=_non_negative_int(row[19], "baseline updated_at"),
        activated_at=activated_at,
        superseded_at=superseded_at,
    )
    try:
        validate_stored_baseline(stored)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Stored health baseline metadata is invalid: {exc}") from exc
    _validate_candidate_trigger_integrity(connection, stored)
    _validate_activation_evidence(connection, stored)
    return stored


def _validate_activation_evidence(
    connection: sqlite3.Connection,
    stored: StoredHealthBaseline,
) -> None:
    rows = connection.execute(
        """
        SELECT test_id, combination_key, test_config_digest,
               health_policy_version, adapter_schema_version,
             evaluator_version, activated_at, quality_json, signature
        FROM health_activation_evidence WHERE baseline_id=?
        """,
        (stored.candidate.baseline_id,),
    ).fetchall()
    if stored.lifecycle is BaselineLifecycle.CANDIDATE:
        if rows:
            raise RuntimeError("Candidate health baseline has activation evidence")
        return
    if len(rows) != 1:
        raise RuntimeError("Active/superseded baseline lacks activation evidence")
    row = rows[0]
    expected = (
        stored.candidate.test_id,
        stored.candidate.combination.key,
        stored.candidate.test_config_digest,
        stored.candidate.health_policy_version,
        stored.candidate.adapter_schema_version,
        stored.candidate.evaluator_version,
        stored.activated_at,
        _quality_json(stored.quality),
        _activation_signature(
            stored.candidate,
            stored.activated_at,
            stored.quality,
            _load_activation_key(_connection_database_path(connection)),
        ),
    )
    actual = (
        _nonempty_db_string(row[0], "activation test_id"),
        _nonempty_db_string(row[1], "activation combination_key"),
        _nonempty_db_string(row[2], "activation test_config_digest"),
        _nonempty_db_string(row[3], "activation health_policy_version"),
        _positive_int(row[4], "activation adapter_schema_version"),
        _nonempty_db_string(row[5], "activation evaluator_version"),
        _non_negative_int(row[6], "activation activated_at"),
        _nonempty_db_string(row[7], "activation quality_json"),
        _nonempty_db_string(row[8], "activation signature"),
    )
    if actual[:-1] != expected[:-1] or not hmac.compare_digest(
        actual[-1],
        expected[-1],
    ):
        raise RuntimeError("Health activation evidence does not match its baseline")


def _begin_read_snapshot(connection: sqlite3.Connection) -> None:
    """Start one query-only snapshot before any schema or content reads."""

    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")


def _quality_json(report: QualityReport) -> str:
    return json.dumps(
        {
            "activation_ready": report.activation_ready,
            "gates": [
                {"name": gate.name, "passed": gate.passed, "message": gate.message}
                for gate in report.gates
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _parse_quality_json(value: str) -> QualityReport:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Health quality_json is invalid") from exc
    if not isinstance(payload, dict) or json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) != value:
        raise RuntimeError("Health quality_json is not canonical")
    gates_raw = payload.get("gates")
    if not isinstance(gates_raw, list):
        raise RuntimeError("Health quality_json lacks gates")
    gates = tuple(
        QualityGate(gate["name"], gate["passed"], gate["message"])
        for gate in gates_raw
        if isinstance(gate, dict)
        and set(gate) == {"name", "passed", "message"}
        and isinstance(gate.get("name"), str)
        and bool(gate.get("name"))
        and isinstance(gate.get("passed"), bool)
        and isinstance(gate.get("message"), str)
    )
    if len(gates) != len(gates_raw):
        raise RuntimeError("Health quality_json contains malformed gates")
    report = QualityReport(gates)
    if payload.get("activation_ready") is not report.activation_ready:
        raise RuntimeError("Health quality_json readiness does not match its gates")
    return report


def _baseline_source_ids(
    connection: sqlite3.Connection,
    baseline_id: str | None,
    *,
    expected_test_id: str | None = None,
    expected_combination_key: str | None = None,
) -> set[int]:
    if baseline_id is None:
        return set()
    stored = _load_stored_from_connection(connection, baseline_id)
    if stored is None:
        raise RuntimeError(f"Health baseline source owner is missing: {baseline_id}")
    if (
        expected_test_id is not None
        and stored.candidate.test_id != expected_test_id
    ) or (
        expected_combination_key is not None
        and stored.candidate.combination.key != expected_combination_key
    ):
        raise RuntimeError("Health baseline source owner/combination is invalid")
    return set(stored.candidate.source_snapshot.result_ids)


def _latest_candidate_id_from_chain(
    connection: sqlite3.Connection,
    test_id: str,
    combination_key: str,
) -> str | None:
    rows = connection.execute(
        """
        SELECT b.baseline_id, t.previous_candidate_id
        FROM health_baselines b
        LEFT JOIN health_candidate_triggers t ON t.baseline_id=b.baseline_id
        WHERE b.test_id=? AND b.combination_key=?
        ORDER BY b.baseline_id
        """,
        (test_id, combination_key),
    ).fetchall()
    if not rows:
        return None
    baseline_ids = {
        _nonempty_db_string(row[0], "candidate chain baseline_id") for row in rows
    }
    predecessors = {
        _nonempty_db_string(row[0], "candidate chain baseline_id"): (
            _nonempty_db_string(row[1], "candidate chain previous_candidate_id")
            if row[1] is not None
            else None
        )
        for row in rows
    }
    roots = [baseline_id for baseline_id, previous in predecessors.items() if previous is None]
    if len(roots) != 1:
        raise RuntimeError("Health immutable candidate chain has invalid roots")
    previous_ids = {previous for previous in predecessors.values() if previous is not None}
    if not previous_ids.issubset(baseline_ids):
        raise RuntimeError("Health immutable candidate chain escapes its combination")
    children: dict[str, str] = {}
    for child, previous in predecessors.items():
        if previous is None:
            continue
        if previous in children:
            raise RuntimeError("Health immutable candidate chain is branched or cyclic")
        children[previous] = child
    visited: set[str] = set()
    current: str | None = roots[0]
    tail: str | None = None
    while current is not None:
        if current in visited:
            raise RuntimeError("Health immutable candidate chain is branched or cyclic")
        visited.add(current)
        tail = current
        current = children.get(current)
    if visited != baseline_ids or tail is None:
        raise RuntimeError("Health immutable candidate chain is branched or cyclic")
    return tail


def _validate_candidate_trigger_state(
    connection: sqlite3.Connection,
    stored: StoredHealthBaseline,
    definition: ValidationTestDefinition,
) -> None:
    health = definition.health
    if health is None or not health.enabled:
        raise ValueError("Health trigger definition is not enabled")
    stored_min_samples, stored_min_new = _validate_candidate_trigger_integrity(
        connection,
        stored,
    )
    if (
        stored_min_samples != health.min_samples
        or stored_min_new != health.min_new_results
    ):
        raise ValueError("Health candidate immutable build trigger is invalid")


def _validate_candidate_trigger_integrity(
    connection: sqlite3.Connection,
    stored: StoredHealthBaseline,
) -> tuple[int, int]:
    """Validate persisted build evidence without consulting advisory state/config."""

    candidate = stored.candidate
    triggers = connection.execute(
        """
        SELECT previous_candidate_id, min_samples, min_new_results,
               qualifying_result_count, new_result_count
        FROM health_candidate_triggers WHERE baseline_id=?
        """,
        (candidate.baseline_id,),
    ).fetchall()
    if len(triggers) != 1:
        raise RuntimeError("Health candidate lacks immutable trigger evidence")
    trigger = triggers[0]
    previous_candidate_id = (
        _nonempty_db_string(trigger[0], "previous_candidate_id")
        if trigger[0] is not None
        else None
    )
    if previous_candidate_id == candidate.baseline_id:
        raise RuntimeError("Health candidate trigger cannot reference itself")
    previous_ids = _raw_baseline_source_ids(
        connection,
        previous_candidate_id,
        expected_test_id=candidate.test_id,
        expected_combination_key=candidate.combination.key,
    )
    stored_min_samples = _positive_int(trigger[1], "trigger min_samples")
    stored_min_new = _positive_int(trigger[2], "trigger min_new_results")
    expected = evaluate_build_trigger(
        candidate.source_snapshot.result_ids,
        previous_ids,
        min_samples=stored_min_samples,
        min_new_results=stored_min_new,
    )
    if (
        _non_negative_int(trigger[3], "trigger qualifying_result_count")
        != expected.qualifying_count
        or _non_negative_int(trigger[4], "trigger new_result_count")
        != expected.new_result_count
        or not expected.eligible
    ):
        raise ValueError("Health candidate immutable build trigger is invalid")
    return stored_min_samples, stored_min_new


def _raw_baseline_source_ids(
    connection: sqlite3.Connection,
    baseline_id: str | None,
    *,
    expected_test_id: str,
    expected_combination_key: str,
) -> set[int]:
    if baseline_id is None:
        return set()
    owner = connection.execute(
        "SELECT test_id, combination_key FROM health_baselines WHERE baseline_id=?",
        (baseline_id,),
    ).fetchone()
    if owner is None:
        raise RuntimeError("Health candidate trigger predecessor is missing")
    if (
        _nonempty_db_string(owner[0], "trigger predecessor test_id")
        != expected_test_id
        or _nonempty_db_string(owner[1], "trigger predecessor combination_key")
        != expected_combination_key
    ):
        raise RuntimeError("Health candidate trigger predecessor has a different owner")
    values = [
        _positive_int(row[0], "trigger predecessor result_id")
        for row in connection.execute(
            "SELECT result_id FROM health_baseline_sources "
            "WHERE baseline_id=? ORDER BY result_id",
            (baseline_id,),
        )
    ]
    if len(values) != len(set(values)):
        raise RuntimeError("Health candidate trigger predecessor sources are duplicated")
    return set(values)



def _database_is_empty(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type IN ('table','index','view','trigger') LIMIT 1"
    ).fetchone() is None


def _require_health_db_owner(
    connection: sqlite3.Connection,
    expected_test_id: str | None = None,
) -> str:
    row = connection.execute(
        "SELECT test_id FROM health_database_owner WHERE id=1"
    ).fetchone()
    if row is None:
        raise RuntimeError("Health database lacks its immutable owner")
    owner = _nonempty_db_string(row[0], "health database owner")
    if expected_test_id is not None and owner != expected_test_id:
        raise ValueError("Health database selector does not match its owner")
    return owner


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_int(value: Any, field_name: str) -> int:
    result = _non_negative_int(value, field_name)
    if result == 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _binary_int(value: Any, field_name: str) -> int:
    result = _non_negative_int(value, field_name)
    if result not in {0, 1}:
        raise ValueError(f"{field_name} must be 0 or 1")
    return result


def _nonempty_db_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _db_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    return value


def _finite_db_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _require_baseline_id(value: Any) -> str:
    if not isinstance(value, str) or not value.startswith("hb1:") or len(value) != 68:
        raise ValueError("baseline_id must be a U8 content-addressed ID")
    try:
        int(value[4:], 16)
    except ValueError as exc:
        raise ValueError("baseline_id must be a U8 content-addressed ID") from exc
    return value


def _require_test_id(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("test_id must be a non-empty string")
    return value


def _require_digest(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a SHA-256 digest") from exc
    return value


def _prepare_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_database_owner (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            test_id TEXT NOT NULL UNIQUE,
            activation_key_digest TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_class_definitions (
            class_code INTEGER PRIMARY KEY CHECK (class_code BETWEEN 0 AND 5),
            class_name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_baselines (
            baseline_id TEXT PRIMARY KEY,
            payload_digest TEXT NOT NULL UNIQUE,
            test_id TEXT NOT NULL,
            combination_key TEXT NOT NULL,
            combination_factors_json TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL
                CHECK (lifecycle_state IN ('candidate','active','superseded')),
            method TEXT NOT NULL,
            robust_z_threshold REAL NOT NULL CHECK (robust_z_threshold > 0),
            observation_digest TEXT NOT NULL,
            source_result_count INTEGER NOT NULL CHECK (source_result_count >= 0),
            excluded_result_count INTEGER NOT NULL DEFAULT 0
                CHECK (excluded_result_count >= 0),
            source_first_timestamp INTEGER,
            source_last_timestamp INTEGER,
            source_max_result_id INTEGER,
            test_config_digest TEXT NOT NULL,
            health_policy_version TEXT NOT NULL,
            adapter_schema_version INTEGER NOT NULL
                CHECK (adapter_schema_version > 0),
            evaluator_version TEXT NOT NULL,
            parent_baseline_id TEXT,
            created_at INTEGER NOT NULL CHECK (created_at >= 0),
            updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
            activated_at INTEGER CHECK (activated_at IS NULL OR activated_at >= 0),
            superseded_at INTEGER CHECK (superseded_at IS NULL OR superseded_at >= 0),
            quality_json TEXT NOT NULL,
            FOREIGN KEY (parent_baseline_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT,
            CHECK (source_last_timestamp IS NULL OR source_first_timestamp IS NULL
                   OR source_last_timestamp >= source_first_timestamp),
            CHECK (activated_at IS NULL OR lifecycle_state IN ('active','superseded')),
            CHECK (superseded_at IS NULL OR lifecycle_state = 'superseded')
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_baseline_sources (
            baseline_id TEXT NOT NULL,
            result_id INTEGER NOT NULL CHECK (result_id > 0),
            run_id TEXT NOT NULL,
            completed_timestamp INTEGER NOT NULL CHECK (completed_timestamp >= 0),
            result_digest TEXT NOT NULL,
            raw_result_digest TEXT NOT NULL,
            test_config_digest TEXT NOT NULL,
            combination_key TEXT NOT NULL,
            adapter_schema_version INTEGER NOT NULL CHECK (adapter_schema_version > 0),
            receipt_evidence_digest TEXT NOT NULL,
            PRIMARY KEY (baseline_id, result_id),
            UNIQUE (baseline_id, run_id),
            FOREIGN KEY (baseline_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_observations (
            baseline_id TEXT NOT NULL,
            result_id INTEGER NOT NULL CHECK (result_id > 0),
            run_id TEXT NOT NULL,
            completed_timestamp INTEGER NOT NULL CHECK (completed_timestamp >= 0),
            source TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            sample_key TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (
                baseline_id, result_id, source, metric_name, sample_key
            ),
            FOREIGN KEY (baseline_id, result_id)
                REFERENCES health_baseline_sources(baseline_id, result_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_metric_statistics (
            baseline_id TEXT NOT NULL,
            spec_name TEXT NOT NULL,
            source TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            direction TEXT NOT NULL
                CHECK (direction IN ('low_bad','high_bad','two_sided','absolute')),
            units TEXT NOT NULL DEFAULT '',
            weight REAL NOT NULL CHECK (weight > 0),
            tolerance_pct REAL NOT NULL CHECK (tolerance_pct >= 0),
            center REAL NOT NULL,
            mad REAL NOT NULL CHECK (mad >= 0),
            mad_sigma REAL NOT NULL CHECK (mad_sigma >= 0),
            delta REAL NOT NULL CHECK (delta >= 0),
            p05 REAL NOT NULL,
            p95 REAL NOT NULL,
            sample_count INTEGER NOT NULL CHECK (sample_count > 0),
            excluded_count INTEGER NOT NULL CHECK (excluded_count >= 0),
            statistics_json TEXT NOT NULL,
            PRIMARY KEY (baseline_id, source, metric_name),
            FOREIGN KEY (baseline_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_source_coverage (
            baseline_id TEXT NOT NULL,
            source TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            result_id INTEGER NOT NULL CHECK (result_id > 0),
            sample_key TEXT NOT NULL,
            PRIMARY KEY (
                baseline_id, source, metric_name, result_id, sample_key
            ),
            FOREIGN KEY (baseline_id, result_id)
                REFERENCES health_baseline_sources(baseline_id, result_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_thresholds (
            baseline_id TEXT NOT NULL,
            spec_name TEXT NOT NULL,
            source TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            class_code INTEGER NOT NULL CHECK (class_code BETWEEN 0 AND 4),
            band_index INTEGER NOT NULL CHECK (band_index >= 0),
            lower_bound REAL,
            upper_bound REAL,
            lower_inclusive INTEGER NOT NULL CHECK (lower_inclusive IN (0,1)),
            upper_inclusive INTEGER NOT NULL CHECK (upper_inclusive IN (0,1)),
            PRIMARY KEY (
                baseline_id, source, metric_name, class_code, band_index
            ),
            FOREIGN KEY (baseline_id, source, metric_name)
                REFERENCES health_metric_statistics(baseline_id, source, metric_name)
                ON DELETE RESTRICT,
            CHECK (lower_bound IS NULL OR upper_bound IS NULL
                   OR lower_bound <= upper_bound)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_candidate_triggers (
            baseline_id TEXT PRIMARY KEY,
            previous_candidate_id TEXT,
            min_samples INTEGER NOT NULL CHECK (min_samples > 0),
            min_new_results INTEGER NOT NULL CHECK (min_new_results > 0),
            qualifying_result_count INTEGER NOT NULL
                CHECK (qualifying_result_count >= 0),
            new_result_count INTEGER NOT NULL CHECK (new_result_count >= 0),
            FOREIGN KEY (baseline_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (previous_candidate_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_activation_evidence (
            baseline_id TEXT PRIMARY KEY,
            test_id TEXT NOT NULL,
            combination_key TEXT NOT NULL,
            test_config_digest TEXT NOT NULL,
            health_policy_version TEXT NOT NULL,
            adapter_schema_version INTEGER NOT NULL
                CHECK (adapter_schema_version > 0),
            evaluator_version TEXT NOT NULL,
            activated_at INTEGER NOT NULL CHECK (activated_at >= 0),
            quality_json TEXT NOT NULL,
            signature TEXT NOT NULL,
            FOREIGN KEY (baseline_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS health_build_state (
            test_id TEXT NOT NULL,
            combination_key TEXT NOT NULL,
            last_seen_result_id INTEGER,
            last_candidate_id TEXT,
            qualifying_result_count INTEGER NOT NULL DEFAULT 0
                CHECK (qualifying_result_count >= 0),
            new_result_count INTEGER NOT NULL DEFAULT 0
                CHECK (new_result_count >= 0),
            last_checked_at INTEGER,
            last_built_at INTEGER,
            last_error TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (test_id, combination_key),
            FOREIGN KEY (last_candidate_id) REFERENCES health_baselines(baseline_id)
                ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_health_baselines_test_state "
        "ON health_baselines(test_id, combination_key, lifecycle_state, created_at DESC)"
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_health_baselines_one_active "
        "ON health_baselines(test_id, combination_key) "
        "WHERE lifecycle_state='active'"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_health_thresholds_metric "
        "ON health_thresholds(source, metric_name, class_code)"
    )
    if connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?",
        (HEALTH_SCHEMA_VERSION,),
    ).fetchone() is None:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
            (HEALTH_SCHEMA_VERSION, _MIGRATION_NAME, int(time.time())),
        )
    for definition in HEALTH_CLASS_DEFINITIONS:
        if connection.execute(
            "SELECT 1 FROM health_class_definitions WHERE class_code=?",
            (definition[0],),
        ).fetchone() is None:
            connection.execute(
                """
                INSERT INTO health_class_definitions(
                    class_code, class_name, description
                ) VALUES (?, ?, ?)
                """,
                definition,
            )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_health_baselines_no_replace
        BEFORE INSERT ON health_baselines
                WHEN NEW.lifecycle_state != 'candidate'
                    OR NEW.activated_at IS NOT NULL
                    OR NEW.superseded_at IS NOT NULL
                    OR (NEW.rowid != -1 AND NEW.rowid <= 0)
                    OR (NEW.rowid != -1 AND EXISTS (
                        SELECT 1 FROM health_baselines existing
                        WHERE existing.rowid IS NEW.rowid
                    ))
                    OR EXISTS (
            SELECT 1 FROM health_baselines existing
            WHERE existing.baseline_id IS NEW.baseline_id
               OR existing.payload_digest IS NEW.payload_digest
        )
        BEGIN
            SELECT RAISE(ABORT, 'health baseline replacement inserts are forbidden');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_health_baselines_no_delete
        BEFORE DELETE ON health_baselines
        BEGIN
            SELECT RAISE(ABORT, 'health baselines are immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_health_baselines_legal_update
        BEFORE UPDATE ON health_baselines
        WHEN NOT (
            NEW.baseline_id IS OLD.baseline_id
            AND NEW.payload_digest IS OLD.payload_digest
            AND NEW.test_id IS OLD.test_id
            AND NEW.combination_key IS OLD.combination_key
            AND NEW.combination_factors_json IS OLD.combination_factors_json
            AND NEW.method IS OLD.method
            AND NEW.robust_z_threshold IS OLD.robust_z_threshold
            AND NEW.observation_digest IS OLD.observation_digest
            AND NEW.source_result_count IS OLD.source_result_count
            AND NEW.excluded_result_count IS OLD.excluded_result_count
            AND NEW.source_first_timestamp IS OLD.source_first_timestamp
            AND NEW.source_last_timestamp IS OLD.source_last_timestamp
            AND NEW.source_max_result_id IS OLD.source_max_result_id
            AND NEW.test_config_digest IS OLD.test_config_digest
            AND NEW.health_policy_version IS OLD.health_policy_version
            AND NEW.adapter_schema_version IS OLD.adapter_schema_version
            AND NEW.evaluator_version IS OLD.evaluator_version
            AND NEW.parent_baseline_id IS OLD.parent_baseline_id
            AND NEW.created_at IS OLD.created_at
            AND NEW.quality_json IS OLD.quality_json
            AND (
                (
                    OLD.lifecycle_state = 'candidate'
                    AND NEW.lifecycle_state = 'active'
                    AND OLD.activated_at IS NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.activated_at IS NOT NULL
                    AND NEW.superseded_at IS NULL
                    AND NEW.updated_at = NEW.activated_at
                    AND NEW.updated_at >= OLD.updated_at
                    AND json_extract(NEW.quality_json, '$.activation_ready') = 1
                    AND EXISTS (
                        SELECT 1 FROM health_candidate_triggers t
                        WHERE t.baseline_id = OLD.baseline_id
                    )
                    AND EXISTS (
                        SELECT 1 FROM health_activation_evidence a
                        WHERE a.baseline_id = OLD.baseline_id
                          AND a.test_id = OLD.test_id
                          AND a.combination_key = OLD.combination_key
                          AND a.test_config_digest = OLD.test_config_digest
                          AND a.health_policy_version = OLD.health_policy_version
                          AND a.adapter_schema_version = OLD.adapter_schema_version
                          AND a.evaluator_version = OLD.evaluator_version
                          AND a.activated_at = NEW.activated_at
                          AND a.quality_json = OLD.quality_json
                    )
                    AND (
                        (
                            NEW.parent_baseline_id IS NULL
                            AND NOT EXISTS (
                                SELECT 1 FROM health_baselines other
                                WHERE other.baseline_id != OLD.baseline_id
                                  AND other.test_id = OLD.test_id
                                  AND other.combination_key = OLD.combination_key
                                  AND other.lifecycle_state IN ('active','superseded')
                            )
                        )
                        OR EXISTS (
                            SELECT 1 FROM health_baselines parent
                            WHERE parent.baseline_id = NEW.parent_baseline_id
                              AND parent.test_id = NEW.test_id
                              AND parent.combination_key = NEW.combination_key
                              AND parent.lifecycle_state = 'superseded'
                              AND parent.superseded_at = NEW.activated_at
                        )
                    )
                )
                OR (
                    OLD.lifecycle_state = 'active'
                    AND NEW.lifecycle_state = 'superseded'
                    AND cval_activation_authorized() = 1
                    AND OLD.activated_at IS NOT NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.activated_at IS OLD.activated_at
                    AND NEW.superseded_at IS NOT NULL
                    AND NEW.updated_at = NEW.superseded_at
                    AND NEW.updated_at >= OLD.updated_at
                    AND EXISTS (
                        SELECT 1 FROM health_baselines child
                                                JOIN health_activation_evidence evidence
                                                    ON evidence.baseline_id = child.baseline_id
                        WHERE child.parent_baseline_id = OLD.baseline_id
                          AND child.test_id = OLD.test_id
                          AND child.combination_key = OLD.combination_key
                          AND child.lifecycle_state = 'candidate'
                                                    AND evidence.test_id = child.test_id
                                                    AND evidence.combination_key = child.combination_key
                                                    AND evidence.test_config_digest = child.test_config_digest
                                                    AND evidence.health_policy_version = child.health_policy_version
                                                    AND evidence.adapter_schema_version = child.adapter_schema_version
                                                    AND evidence.evaluator_version = child.evaluator_version
                                                    AND evidence.activated_at = NEW.superseded_at
                                                    AND evidence.quality_json = child.quality_json
                    )
                )
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'illegal health baseline mutation');
        END
        """
    )
    for table in (
        "schema_migrations",
        "health_database_owner",
        "health_class_definitions",
        "health_baseline_sources",
        "health_observations",
        "health_metric_statistics",
        "health_source_coverage",
        "health_thresholds",
        "health_candidate_triggers",
        "health_activation_evidence",
    ):
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_update
            BEFORE UPDATE ON {table}
            BEGIN
                SELECT RAISE(ABORT, '{table} rows are immutable');
            END
            """
        )
    immutable_keys = {
        "schema_migrations": (("version",),),
        "health_database_owner": (("id",), ("test_id",)),
        "health_class_definitions": (("class_code",), ("class_name",)),
        "health_baseline_sources": (
            ("baseline_id", "result_id"),
            ("baseline_id", "run_id"),
        ),
        "health_observations": (
            ("baseline_id", "result_id", "source", "metric_name", "sample_key"),
        ),
        "health_metric_statistics": (("baseline_id", "source", "metric_name"),),
        "health_source_coverage": (
            ("baseline_id", "source", "metric_name", "result_id", "sample_key"),
        ),
        "health_thresholds": (
            ("baseline_id", "source", "metric_name", "class_code", "band_index"),
        ),
        "health_candidate_triggers": (("baseline_id",),),
        "health_activation_evidence": (("baseline_id",),),
    }
    for table, groups in immutable_keys.items():
        conflicts = [
            f"(NEW.rowid != -1 AND EXISTS (SELECT 1 FROM {table} existing "
            "WHERE existing.rowid IS NEW.rowid))",
            "(NEW.rowid != -1 AND NEW.rowid <= 0)",
        ]
        for group in groups:
            nonnull = " AND ".join(f"NEW.{column} IS NOT NULL" for column in group)
            equality = " AND ".join(
                f"existing.{column} IS NEW.{column}" for column in group
            )
            conflicts.append(
                f"(({nonnull}) AND EXISTS (SELECT 1 FROM {table} existing "
                f"WHERE {equality}))"
            )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_replace
            BEFORE INSERT ON {table}
            WHEN {' OR '.join(conflicts)}
            BEGIN
                SELECT RAISE(ABORT, '{table} replacement inserts are forbidden');
            END
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_delete
            BEFORE DELETE ON {table}
            BEGIN
                SELECT RAISE(ABORT, '{table} rows are immutable');
            END
            """
        )
    for table in (
        "health_baseline_sources",
        "health_observations",
        "health_metric_statistics",
        "health_source_coverage",
        "health_thresholds",
        "health_candidate_triggers",
    ):
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_candidate_insert
            BEFORE INSERT ON {table}
            WHEN COALESCE(
                (SELECT lifecycle_state FROM health_baselines
                 WHERE baseline_id = NEW.baseline_id),
                ''
            ) != 'candidate'
            BEGIN
                SELECT RAISE(ABORT, '{table} requires a candidate owner');
            END
            """
        )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_health_activation_evidence_candidate_insert
        BEFORE INSERT ON health_activation_evidence
        WHEN cval_activation_authorized() != 1
          OR COALESCE(
                (SELECT lifecycle_state FROM health_baselines
                 WHERE baseline_id = NEW.baseline_id),
                ''
             ) != 'candidate'
        BEGIN
            SELECT RAISE(ABORT, 'activation evidence requires framework authorization');
        END
        """
    )


@lru_cache(maxsize=1)
def _schema_manifest() -> dict[str, str]:
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        _prepare_schema(connection)
        return {
            f"{row[0]}:{row[1]}": str(row[2] or "")
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE type IN ('table','index','view','trigger')"
            )
        }


def _validate_schema(connection: sqlite3.Connection) -> None:
    actual = {
        f"{row[0]}:{row[1]}": str(row[2] or "")
        for row in connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger')"
        )
    }
    if actual != _schema_manifest():
        raise RuntimeError("Health database schema manifest does not match this engine")
    immutable_rowid_tables = (
        "schema_migrations",
        "health_database_owner",
        "health_class_definitions",
        "health_baselines",
        "health_baseline_sources",
        "health_observations",
        "health_metric_statistics",
        "health_source_coverage",
        "health_thresholds",
        "health_candidate_triggers",
        "health_activation_evidence",
    )
    for table_name in immutable_rowid_tables:
        minimum_rowid = 0 if table_name == "health_class_definitions" else 1
        if connection.execute(
            f"SELECT 1 FROM {table_name} WHERE rowid < ? LIMIT 1",
            (minimum_rowid,),
        ).fetchone() is not None:
            raise RuntimeError(
                f"Health immutable table {table_name} has an invalid hidden rowid"
            )
    migrations = connection.execute(
        "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
    ).fetchall()
    if (
        len(migrations) != 1
        or migrations[0][:2] != (HEALTH_SCHEMA_VERSION, _MIGRATION_NAME)
        or isinstance(migrations[0][2], bool)
        or not isinstance(migrations[0][2], int)
        or migrations[0][2] < 0
    ):
        raise RuntimeError(f"Unsupported health schema migration manifest: {migrations}")
    definitions = connection.execute(
        """
        SELECT class_code, class_name, description
        FROM health_class_definitions ORDER BY class_code
        """
    ).fetchall()
    if definitions != list(HEALTH_CLASS_DEFINITIONS):
        raise RuntimeError("Health class definitions do not match stable codes 0-5")
    owners = connection.execute(
        "SELECT id, test_id, activation_key_digest "
        "FROM health_database_owner ORDER BY id"
    ).fetchall()
    if len(owners) > 1 or (
        owners
        and (
            owners[0][0] != 1
            or not isinstance(owners[0][1], str)
            or not owners[0][1]
            or not isinstance(owners[0][2], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", owners[0][2])
        )
    ):
        raise RuntimeError("Health database owner manifest is invalid")
    baseline_owners = {
        row[0]
        for row in connection.execute("SELECT DISTINCT test_id FROM health_baselines")
    }
    build_owners = {
        row[0]
        for row in connection.execute("SELECT DISTINCT test_id FROM health_build_state")
    }
    if (baseline_owners or build_owners) and not owners:
        raise RuntimeError("Nonempty health database lacks its owner row")
    if owners and (
        baseline_owners - {owners[0][1]} or build_owners - {owners[0][1]}
    ):
        raise RuntimeError("Health database contains cross-owner baselines")
    if owners:
        key = _load_activation_key(_connection_database_path(connection))
        if not hmac.compare_digest(
            owners[0][2],
            _activation_key_digest(key),
        ):
            raise RuntimeError("Health activation key does not match its owner")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise RuntimeError("Health database foreign-key manifest is invalid")
    missing_triggers = connection.execute(
        """
        SELECT b.baseline_id
        FROM health_baselines b
        LEFT JOIN health_candidate_triggers t ON t.baseline_id=b.baseline_id
        WHERE t.baseline_id IS NULL
        ORDER BY b.baseline_id
        """
    ).fetchall()
    if missing_triggers:
        raise RuntimeError("Health database contains baseline(s) without trigger evidence")
    combinations = connection.execute(
        "SELECT DISTINCT test_id, combination_key FROM health_baselines "
        "ORDER BY test_id, combination_key"
    ).fetchall()
    for test_id, combination_key in combinations:
        _latest_candidate_id_from_chain(
            connection,
            _nonempty_db_string(test_id, "candidate chain test_id"),
            _nonempty_db_string(
                combination_key,
                "candidate chain combination_key",
            ),
        )
    invalid_parent_owner = connection.execute(
        """
        SELECT child.baseline_id
        FROM health_baselines child
        JOIN health_baselines parent ON parent.baseline_id=child.parent_baseline_id
        WHERE parent.test_id != child.test_id
           OR parent.combination_key != child.combination_key
        LIMIT 1
        """
    ).fetchone()
    if invalid_parent_owner is not None:
        raise RuntimeError("Health lifecycle parent has a different owner/combination")
    invalid_superseded = connection.execute(
        """
        SELECT parent.baseline_id
        FROM health_baselines parent
        WHERE parent.lifecycle_state='superseded'
          AND (
            SELECT COUNT(*)
            FROM health_baselines child
            JOIN health_activation_evidence evidence
              ON evidence.baseline_id=child.baseline_id
            WHERE child.parent_baseline_id=parent.baseline_id
              AND child.test_id=parent.test_id
              AND child.combination_key=parent.combination_key
              AND child.lifecycle_state IN ('active','superseded')
              AND child.activated_at=parent.superseded_at
              AND evidence.activated_at=child.activated_at
          ) != 1
        LIMIT 1
        """
    ).fetchone()
    if invalid_superseded is not None:
        raise RuntimeError("Superseded health baseline lacks one activated child")
    invalid_active = connection.execute(
        """
        SELECT active.baseline_id
        FROM health_baselines active
        WHERE active.lifecycle_state='active'
          AND EXISTS (
            SELECT 1 FROM health_baselines child
            WHERE child.parent_baseline_id=active.baseline_id
              AND child.lifecycle_state IN ('active','superseded')
          )
        LIMIT 1
        """
    ).fetchone()
    if invalid_active is not None:
        raise RuntimeError("Active health baseline is not the activated chain tail")
    for row in connection.execute(
        "SELECT baseline_id FROM health_baselines ORDER BY baseline_id"
    ):
        baseline_id = _nonempty_db_string(row[0], "schema baseline_id")
        if _load_stored_from_connection(connection, baseline_id) is None:
            raise RuntimeError("Health schema baseline disappeared during validation")
