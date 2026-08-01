"""Dry-run-first, registry-driven U9 health evaluator.

This module is deliberately local/PVC-only.  It imports no Kubernetes code,
uses only canonical per-test U7/U8 paths, and keeps every test failure isolated.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from contextlib import ExitStack, closing, nullcontext
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Callable

from cval.config import CvalConfig
from cval.evaluator.state import (
    StateLockError,
    bind_state_directory,
    bind_state_target,
    open_state_root,
    state_test_lock,
)
from cval.health.combination import resolve_environment_combination
from cval.health.engine import (
    HEALTH_ENGINE_VERSION,
    build_candidate_from_plugin,
    classify_from_plugin,
    evaluate_build_trigger,
)
from cval.health.models import (
    BaselineLifecycle,
    CandidateStoreStatus,
    ClassificationHistoryRecord,
    EnvironmentCombination,
    HealthCandidate,
    HealthContext,
    HealthVerdict,
    SourceResult,
    SourceSnapshot,
)
from cval.health.storage import (
    _activate_candidate,
    assert_health_database_generation,
    get_chain_cursor,
    load_baselines_generation,
    _persist_candidate,
    preflight_activation,
    resolve_health_db_path,
)
from cval.storage.paths import safe_writable_file_path
from cval.storage.sqlite_snapshot import (
    immutable_sqlite_snapshot,
    is_snapshot_uri,
    sqlite_connection_projection,
)
from cval.storage.sqlite_uri import assert_sqlite_file_identity, connect_sqlite_file
from cval.storage.per_test_results import (
    PLUGIN_API_VERSION,
    SelectedResultEvidence,
    common_result_schema_version,
    migrate_per_test_results_to_v2,
    resolve_test_results_db_path,
    selected_result_evidence_guard,
    store_classification_history,
    validate_common_result_connection,
)
from cval.validation.plugins import load_registered_plugin
from cval.validation.plugins import IngestionConflictError
from cval.validation.registry import (
    RegisteredValidationTest,
    validation_test_config_digest,
)


EVALUATE_CONFIRMATION = "evaluate"
ACTIVATE_CONFIRMATION = "activate"
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_BASELINE_PATTERN = re.compile(r"^hb1:[0-9a-f]{64}$")
_CATALOG_QUERY_PAGE_SIZE = 256


class HealthEvaluatorError(RuntimeError):
    """Base error for expected evaluator/operator failures."""


class HealthEvaluatorPolicyError(HealthEvaluatorError):
    """Raised before any write when an apply gate is not satisfied."""


class HealthEvaluatorLockError(HealthEvaluatorError):
    """Raised when a bounded owner-only per-test lock cannot be acquired."""


@dataclass(frozen=True)
class CatalogResult:
    result_id: int
    run_id: str
    status: str
    combination: EnvironmentCombination | None
    source_result: SourceResult | None
    defer_reason: str | None
    result_digest: str = ""
    raw_result_digest: str = ""
    test_config_digest: str = ""
    adapter_schema_version: int = 0
    receipt_evidence_digest: str = ""
    target: ClassificationTarget | None = None
    existing_evidence_digest: str | None = None
    selected_evidence: SelectedResultEvidence | None = None


@dataclass(frozen=True)
class ClassificationTarget:
    baseline_identity: str
    target_digest: str
    baseline_id: str | None
    category: str


@dataclass(frozen=True)
class ResultCatalog:
    schema_version: int
    result_count: int
    candidate_results: tuple[CatalogResult, ...]
    classification_results: tuple[CatalogResult, ...]
    classification_backlog: int
    adapter_schema_initialized: bool
    deferred_results: tuple[CatalogResult, ...] = ()
    deferred_count: int = 0

    @property
    def classification_truncated(self) -> bool:
        return (
            self.classification_backlog > len(self.classification_results)
            or self.deferred_count > len(self.deferred_results)
        )


@dataclass(frozen=True)
class CandidateAction:
    combination_key: str
    action: str
    baseline_id: str | None
    qualifying_count: int
    new_result_count: int
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClassificationAction:
    run_id: str
    action: str
    class_code: int | None
    class_name: str | None
    baseline_id: str | None
    dnr_reason: str | None
    reason: str = ""


@dataclass(frozen=True)
class TestEvaluationReport:
    test_id: str
    status: str
    result_db_path: str
    health_db_path: str
    source_schema_version: int | None
    result_count: int
    candidate_source_count: int = 0
    classification_selected_count: int = 0
    deferred_count: int = 0
    classification_backlog: int = 0
    classification_remaining: int = 0
    classification_truncated: bool = False
    adapter_schema_initialized: bool | None = None
    candidates: tuple[CandidateAction, ...] = ()
    classifications: tuple[ClassificationAction, ...] = ()
    history_inserted: int = 0
    history_idempotent: int = 0
    migrated_to_v2: bool = False
    candidates_inserted: int = 0
    candidates_idempotent: int = 0
    health_db_present: bool = False
    activation_key_present: bool = False
    creation_cleanup_completed: bool | None = None
    partial_writes: bool = False
    error_stage: str = ""
    write_stages_completed: tuple[str, ...] = ()
    write_atomicity: str = "no-writes"
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvaluationCycleReport:
    mode: str
    evaluator_version: str
    write_enabled: bool
    started_at: int
    tests: tuple[TestEvaluationReport, ...]

    @property
    def ok(self) -> bool:
        return all(report.status != "error" for report in self.tests)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "evaluator_version": self.evaluator_version,
            "write_enabled": self.write_enabled,
            "started_at": self.started_at,
            "ok": self.ok,
            "summary": {
                "tests": len(self.tests),
                "processed": sum(report.status == "processed" for report in self.tests),
                "skipped": sum(report.status == "skipped" for report in self.tests),
                "errors": sum(report.status == "error" for report in self.tests),
                "history_inserted": sum(report.history_inserted for report in self.tests),
                "deferred_count": sum(report.deferred_count for report in self.tests),
                "classification_backlog": sum(
                    report.classification_backlog for report in self.tests
                ),
                "classification_remaining": sum(
                    report.classification_remaining for report in self.tests
                ),
                "truncated_tests": sum(
                    report.classification_truncated for report in self.tests
                ),
                "partial_write_tests": sum(report.partial_writes for report in self.tests),
            },
            "tests": [report.to_dict() for report in self.tests],
        }


@dataclass(frozen=True)
class ActivationReport:
    mode: str
    test_id: str
    baseline_id: str
    combination_key: str
    lifecycle: str
    activation_ready: bool
    already_active: bool
    activated: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_health_cycle(
    config: CvalConfig,
    *,
    apply: bool = False,
    confirmation: str | None = None,
    now: int | None = None,
) -> EvaluationCycleReport:
    """Evaluate all enabled registry tests with health+ingest capabilities.

    Dry-run is the default and never creates locks, databases, key sidecars, or
    migration rows.  Apply is independently double-gated.
    """

    timestamp = _non_negative_timestamp(now)
    _require_apply_gate(
        config,
        apply=apply,
        confirmation=confirmation,
        expected=EVALUATE_CONFIRMATION,
    )
    if apply:
        with open_state_root(config, require_writable=True):
            pass
    eligible = tuple(
        test
        for test in config.tests.registry.enabled
        if _has_health_and_ingest_capabilities(test)
    )
    reports: list[TestEvaluationReport] = []
    for registered in eligible:
        try:
            reports.append(
                _evaluate_registered_test(
                    config,
                    registered,
                    apply=apply,
                    now=timestamp,
                )
            )
        except Exception as exc:  # noqa: BLE001 - per-test isolation boundary
            reports.append(
                _error_report(config, registered, _safe_error(exc))
            )
    return EvaluationCycleReport(
        mode="apply" if apply else "dry-run",
        evaluator_version=HEALTH_ENGINE_VERSION,
        write_enabled=config.health_evaluator.write_enabled,
        started_at=timestamp,
        tests=tuple(reports),
    )


def activate_health_candidate(
    config: CvalConfig,
    test_id: str,
    baseline_id: str,
    *,
    apply: bool = False,
    confirmation: str | None = None,
    now: int | None = None,
) -> ActivationReport:
    """Preflight or deliberately activate one named U8 candidate."""

    timestamp = _non_negative_timestamp(now)
    _require_apply_gate(
        config,
        apply=apply,
        confirmation=confirmation,
        expected=ACTIVATE_CONFIRMATION,
    )
    registered = config.tests.registry.require(test_id)
    if not registered.enabled or not _has_health_and_ingest_capabilities(registered):
        raise HealthEvaluatorError(
            f"Validation test {test_id!r} is not enabled for health evaluation"
        )
    if not isinstance(baseline_id, str) or not _BASELINE_PATTERN.fullmatch(baseline_id):
        raise HealthEvaluatorError("baseline_id must be a U8 content-addressed ID")
    result_path = resolve_test_results_db_path(
        config.health_evaluator.state_root,
        registered,
    )
    health_path = resolve_health_db_path(
        config.health_evaluator.state_root,
        registered,
    )
    def operation(
        lock_guard: Callable[[], None] | None = None,
        health_binding: Any | None = None,
        key_binding: Any | None = None,
    ) -> ActivationReport:
        preflight = preflight_activation(
            baseline_id,
            registered.definition,
            db_path=health_path,
            robust_z_threshold=config.baseline.robust_z_threshold,
            state_binding=health_binding,
            key_binding=key_binding,
        )
        activated = False
        if apply:
            if not preflight.activation_ready:
                raise HealthEvaluatorError("Health candidate is not activation-ready")
            activated = _activate_candidate(
                baseline_id,
                registered.definition,
                db_path=health_path,
                now=timestamp,
                robust_z_threshold=config.baseline.robust_z_threshold,
                lock_guard=lock_guard,
                state_binding=health_binding,
                key_binding=key_binding,
            )
            preflight = preflight_activation(
                baseline_id,
                registered.definition,
                db_path=health_path,
                robust_z_threshold=config.baseline.robust_z_threshold,
                state_binding=health_binding,
                key_binding=key_binding,
            )
        return ActivationReport(
            mode="apply" if apply else "dry-run",
            test_id=test_id,
            baseline_id=baseline_id,
            combination_key=preflight.combination_key,
            lifecycle=preflight.lifecycle.value,
            activation_ready=preflight.activation_ready,
            already_active=preflight.already_active,
            activated=activated,
        )

    if not apply:
        return operation()
    try:
        with state_test_lock(config, result_path) as shared_guard:
            with bind_state_target(
                config,
                result_path,
                create=False,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ) as result_binding, bind_state_target(
                config,
                health_path,
                create=False,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ) as health_binding, bind_state_target(
                config,
                health_path.with_name(f"{health_path.name}.activation.key"),
                create=False,
                allow_missing=False,
                writable=False,
                require_writable=True,
            ) as key_binding:
                def guard() -> None:
                    shared_guard()
                    result_binding.assert_path_binding()
                    health_binding.assert_path_binding()
                    key_binding.assert_path_binding()

                guard.path = shared_guard.path  # type: ignore[attr-defined]

                return operation(guard, health_binding, key_binding)
    except StateLockError as exc:
        raise HealthEvaluatorLockError(str(exc)) from exc


def _evaluate_registered_test(
    config: CvalConfig,
    registered: RegisteredValidationTest,
    *,
    apply: bool,
    now: int,
) -> TestEvaluationReport:
    result_path = resolve_test_results_db_path(
        config.health_evaluator.state_root,
        registered,
    )
    health_path = resolve_health_db_path(
        config.health_evaluator.state_root,
        registered,
    )
    if not result_path.is_file():
        return TestEvaluationReport(
            test_id=registered.id,
            status="skipped",
            result_db_path=str(result_path),
            health_db_path=str(health_path),
            source_schema_version=None,
            result_count=0,
            error="canonical result database is missing",
        )

    def operation(
        lock_guard: Callable[[], None] | None = None,
        source_identity: Any | None = None,
        health_binding: Any | None = None,
        key_binding: Any | None = None,
        source_binding: Any | None = None,
    ) -> TestEvaluationReport:
        stage = "read-preflight"
        activation_key_path = health_path.with_name(
            f"{health_path.name}.activation.key"
        )
        health_existed_before = (
            health_binding.identity is not None
            if health_binding is not None
            else health_path.is_file()
        )
        key_existed_before = (
            key_binding.identity is not None
            if key_binding is not None
            else activation_key_path.is_file()
        )
        try:
            plugin = load_registered_plugin(registered)
            if plugin is None:
                raise HealthEvaluatorError("Health-capable test has no plugin")
            active, initial_health_generation = _active_baselines_generation(
                registered.id,
                health_path,
                health_binding=health_binding,
                key_binding=key_binding,
            )
            with immutable_sqlite_snapshot(
                result_path,
                expected_identity=source_identity,
                source_fd=(source_binding.descriptor if source_binding is not None else None),
                source_parent_fd=(
                    source_binding.directory.parent_fd
                    if source_binding is not None
                    else None
                ),
                source_name=(source_binding.name if source_binding is not None else None),
                binding_guard=(
                    source_binding.assert_path_binding
                    if source_binding is not None
                    else None
                ),
            ) as snapshot:
                catalog = _load_result_catalog(
                    snapshot.uri,
                    registered,
                    plugin,
                    active_baseline_ids={
                        key: value.candidate.baseline_id for key, value in active.items()
                    },
                    limit=config.health_evaluator.max_classifications_per_test,
                )
                candidates, candidate_plans = _plan_candidates(
                    config,
                    registered,
                    plugin,
                    catalog,
                    health_path=health_path,
                    result_path=snapshot.uri,
                    now=now,
                    health_binding=health_binding,
                    key_binding=key_binding,
                )
                classifications, records, verified_idempotent = _evaluate_classifications(
                    config,
                    registered,
                    plugin,
                    catalog,
                    health_path=health_path,
                    result_path=snapshot.uri,
                    now=now,
                    active=active,
                )
                selected_evidence = _selected_evidence(catalog)
                source_identity = snapshot.source_identity
        except Exception as exc:  # noqa: BLE001 - preflight isolation and stage report
            return TestEvaluationReport(
                test_id=registered.id,
                status="error",
                result_db_path=str(result_path),
                health_db_path=str(health_path),
                source_schema_version=None,
                result_count=0,
                error_stage=stage,
                write_atomicity="no-writes",
                error=_safe_error(exc),
            )

        common = dict(
            test_id=registered.id,
            result_db_path=str(result_path),
            health_db_path=str(health_path),
            source_schema_version=catalog.schema_version,
            result_count=catalog.result_count,
            candidate_source_count=len(catalog.candidate_results),
            classification_selected_count=len(catalog.classification_results),
            deferred_count=catalog.deferred_count,
            classification_backlog=catalog.classification_backlog,
            classification_remaining=(
                catalog.classification_backlog + catalog.deferred_count
            ),
            classification_truncated=catalog.classification_truncated,
            adapter_schema_initialized=catalog.adapter_schema_initialized,
            candidates=candidates,
            classifications=classifications,
            history_idempotent=verified_idempotent,
            health_db_present=(
                health_binding.identity is not None
                if health_binding is not None
                else health_path.is_file()
            ),
            activation_key_present=(
                key_binding.identity is not None
                if key_binding is not None
                else activation_key_path.is_file()
            ),
        )
        if not apply:
            return TestEvaluationReport(status="processed", **common)

        migrated = False
        candidate_inserted = 0
        candidate_idempotent = 0
        history_inserted = 0
        history_idempotent = verified_idempotent
        completed: list[str] = []
        candidate_statuses: dict[str, str] = {}
        durable_effect = False
        candidate_persistence_started = False
        try:
            if catalog.schema_version == 1 and (candidate_plans or records):
                stage = "result-db-migration"
                migrated = migrate_per_test_results_to_v2(
                    result_path,
                    expected_identity=source_identity,
                    expected_results=selected_evidence,
                    pre_write_check=lambda connection: _assert_adapter_schema_state(
                        connection,
                        plugin,
                        catalog,
                    ),
                    lock_guard=lock_guard,
                )
                durable_effect = durable_effect or migrated
                completed.append(stage)
            for candidate in candidate_plans:
                stage = f"health-candidate:{candidate.combination.key}"
                with selected_result_evidence_guard(
                    result_path,
                    expected_identity=source_identity,
                    expected_results=_candidate_selected_evidence(candidate, catalog),
                ) as result_connection:
                    _assert_candidate_rebuild(
                        plugin,
                        candidate,
                        registered,
                        result_connection,
                        catalog,
                        active,
                        config.health_evaluator.max_classifications_per_test,
                    )
                    candidate_persistence_started = True
                    outcome = _persist_candidate(
                        candidate,
                        registered.definition,
                        db_path=health_path,
                        now=max(now, candidate.created_at),
                        robust_z_threshold=config.baseline.robust_z_threshold,
                        pre_commit=lambda _connection: assert_sqlite_file_identity(
                            source_identity
                        ),
                        lock_guard=lock_guard,
                        state_binding=health_binding,
                        key_binding=key_binding,
                    )
                if outcome.status is CandidateStoreStatus.STORED:
                    candidate_inserted += 1
                    durable_effect = True
                else:
                    candidate_idempotent += 1
                candidate_statuses[candidate.baseline_id] = outcome.status.value
                completed.append(stage)
            if records:
                stage = "health-generation-revalidation"
                _current_active, current_health_generation = (
                    _active_baselines_generation(
                        registered.id,
                        health_path,
                        health_binding=health_binding,
                        key_binding=key_binding,
                    )
                )
                if (
                    current_health_generation.active_digest
                    != initial_health_generation.active_digest
                ):
                    raise RuntimeError(
                        "Health active generation changed after classification preflight"
                    )
                stage = "classification-history"

                def pre_history_check(connection: sqlite3.Connection) -> None:
                    nonlocal stage
                    stage = "health-generation-revalidation"
                    assert_health_database_generation(
                        current_health_generation,
                        state_binding=health_binding,
                        key_binding=key_binding,
                    )
                    stage = "classification-revalidation"
                    current_catalog = _load_result_catalog(
                        result_path,
                        registered,
                        plugin,
                        active_baseline_ids={
                            key: value.candidate.baseline_id
                            for key, value in _current_active.items()
                        },
                        limit=config.health_evaluator.max_classifications_per_test,
                        connection=connection,
                    )
                    _assert_catalog_source_generation(catalog, current_catalog)
                    with sqlite_connection_projection(connection) as result_projection:
                        current_actions, current_records, current_idempotent = (
                            _evaluate_classifications(
                                config,
                                registered,
                                plugin,
                                catalog,
                                health_path=health_path,
                                result_path=result_projection,
                                now=now,
                                active=_current_active,
                            )
                        )
                    if (
                        current_actions != classifications
                        or current_records != records
                        or current_idempotent != verified_idempotent
                    ):
                        raise IngestionConflictError(
                            "Classification evidence changed before history commit"
                        )
                    stage = "classification-history"

                history_outcome = store_classification_history(
                    records,
                    db_path=result_path,
                    expected_identity=source_identity,
                    expected_results=selected_evidence,
                    pre_write_check=pre_history_check,
                    lock_guard=lock_guard,
                )
                history_inserted = history_outcome.inserted
                history_idempotent += history_outcome.idempotent
                durable_effect = durable_effect or history_inserted > 0
                completed.append(stage)
                classifications = _applied_classification_actions(
                    classifications,
                    tuple(outcome.value for outcome in history_outcome.outcomes),
                )
            candidates = _applied_candidate_actions(
                candidates,
                candidate_statuses,
            )
        except Exception as exc:  # noqa: BLE001 - preserve durable-stage facts
            candidates = _applied_candidate_actions(candidates, candidate_statuses)
            health_db_present = (
                health_binding.identity is not None
                if health_binding is not None
                else health_path.is_file()
            )
            activation_key_present = (
                key_binding.identity is not None
                if key_binding is not None
                else activation_key_path.is_file()
            )
            creation_artifact_effect = (
                (not health_existed_before and health_db_present)
                or (not key_existed_before and activation_key_present)
            )
            if health_binding is not None:
                health_binding.directory.assert_path_binding()
                staging_present = any(
                    name.startswith(f".{health_path.name}.")
                    and ".staging" in name
                    for name in os.listdir(health_binding.directory.parent_fd)
                )
            else:
                staging_present = any(
                    health_path.parent.glob(f".{health_path.name}.*.staging*")
                )
            return TestEvaluationReport(
                status="error",
                **{
                    **common,
                    "candidates": candidates,
                    "classifications": classifications,
                    "history_inserted": history_inserted,
                    "history_idempotent": history_idempotent,
                    "migrated_to_v2": migrated,
                    "candidates_inserted": candidate_inserted,
                    "candidates_idempotent": candidate_idempotent,
                    "health_db_present": health_db_present,
                    "activation_key_present": activation_key_present,
                    "creation_cleanup_completed": (
                        not health_existed_before
                        and not key_existed_before
                        and not health_db_present
                        and not activation_key_present
                        and not staging_present
                        if stage.startswith("health-candidate:")
                        and candidate_persistence_started
                        else None
                    ),
                    "partial_writes": (
                        durable_effect or creation_artifact_effect or staging_present
                    ),
                    "error_stage": stage,
                    "write_stages_completed": tuple(completed),
                    "write_atomicity": "per-database transactions; cross-database non-atomic",
                    "error": _safe_error(exc),
                },
            )
        remaining = max(
            0,
            catalog.classification_backlog
            - history_inserted
            - (history_idempotent - verified_idempotent),
        ) + catalog.deferred_count
        return TestEvaluationReport(
            status="processed",
            **{
                **common,
                "candidates": candidates,
                "classifications": classifications,
                "classification_remaining": remaining,
                "history_inserted": history_inserted,
                "history_idempotent": history_idempotent,
                "migrated_to_v2": migrated,
                "candidates_inserted": candidate_inserted,
                "candidates_idempotent": candidate_idempotent,
                "health_db_present": (
                    health_binding.identity is not None
                    if health_binding is not None
                    else health_path.is_file()
                ),
                "activation_key_present": (
                    key_binding.identity is not None
                    if key_binding is not None
                    else activation_key_path.is_file()
                ),
                "write_stages_completed": tuple(completed),
                "write_atomicity": "per-database transactions; cross-database non-atomic",
            },
        )

    if not apply:
        return operation()
    report: TestEvaluationReport | None = None
    try:
        with state_test_lock(config, result_path) as shared_guard:
            with ExitStack() as bindings:
                result_binding = bindings.enter_context(
                    bind_state_target(
                        config,
                        result_path,
                        create=False,
                        allow_missing=False,
                        writable=True,
                        require_writable=True,
                    )
                )
                health_directory = bindings.enter_context(
                    bind_state_directory(
                        config,
                        health_path,
                        create=True,
                        allow_missing=False,
                        require_writable=True,
                    )
                )
                health_binding = bindings.enter_context(
                    bind_state_target(
                        config,
                        health_path,
                        create=False,
                        allow_missing=True,
                        writable=True,
                        require_writable=True,
                    )
                )
                key_binding = bindings.enter_context(
                    bind_state_target(
                        config,
                        health_path.with_name(
                            f"{health_path.name}.activation.key"
                        ),
                        create=False,
                        allow_missing=True,
                        writable=False,
                        require_writable=True,
                    )
                )

                def lock_guard() -> None:
                    shared_guard()
                    result_binding.assert_path_binding()
                    health_directory.assert_path_binding()
                    if health_binding.identity is not None:
                        health_binding.assert_path_binding()
                    if key_binding.identity is not None:
                        key_binding.assert_path_binding()

                lock_guard.path = shared_guard.path  # type: ignore[attr-defined]

                report = operation(
                    lock_guard,
                    result_binding.sqlite_identity,
                    health_binding,
                    key_binding,
                    result_binding,
                )
    except (
        StateLockError,
        HealthEvaluatorLockError,
        RuntimeError,
        ValueError,
        OSError,
    ) as exc:
        if report is None:
            if isinstance(exc, HealthEvaluatorLockError):
                raise
            raise HealthEvaluatorLockError(str(exc)) from exc
        durable_stages = (
            report.migrated_to_v2
            or report.candidates_inserted > 0
            or report.history_inserted > 0
        )
        partial_writes = report.partial_writes or durable_stages
        finalization_error = (
            "Health evaluator lock finalization failed: " + _safe_error(exc)
        )
        if report.error:
            finalization_error = f"{report.error}; {finalization_error}"
        return replace(
            report,
            status="error",
            partial_writes=partial_writes,
            error_stage=report.error_stage or "lock-finalization",
            write_atomicity=(
                "per-database transactions; cross-database non-atomic"
                if partial_writes
                else "no-writes"
            ),
            error=finalization_error,
        )
    if report is None:
        raise RuntimeError("Health evaluator apply completed without a test report")
    return report


def _applied_candidate_actions(
    actions: tuple[CandidateAction, ...],
    statuses: dict[str, str],
) -> tuple[CandidateAction, ...]:
    return tuple(
        CandidateAction(
            combination_key=action.combination_key,
            action=(statuses.get(action.baseline_id, action.action) if action.baseline_id else action.action),
            baseline_id=action.baseline_id,
            qualifying_count=action.qualifying_count,
            new_result_count=action.new_result_count,
            reasons=action.reasons,
        )
        for action in actions
    )


def _applied_classification_actions(
    actions: tuple[ClassificationAction, ...],
    outcomes: tuple[str, ...],
) -> tuple[ClassificationAction, ...]:
    applied: list[ClassificationAction] = []
    outcome_index = 0
    for action in actions:
        label = action.action
        if label == "would-store":
            if outcome_index >= len(outcomes):
                raise RuntimeError("Classification history outcome count is incomplete")
            label = outcomes[outcome_index]
            outcome_index += 1
        applied.append(
            ClassificationAction(
                run_id=action.run_id,
                action=label,
                class_code=action.class_code,
                class_name=action.class_name,
                baseline_id=action.baseline_id,
                dnr_reason=action.dnr_reason,
                reason=action.reason,
            )
        )
    if outcome_index != len(outcomes):
        raise RuntimeError("Classification history outcome count exceeds planned work")
    return tuple(applied)


def _selected_evidence(catalog: ResultCatalog) -> tuple[SelectedResultEvidence, ...]:
    by_result: dict[int, SelectedResultEvidence] = {}
    for result in (*catalog.candidate_results, *catalog.classification_results):
        evidence = result.selected_evidence
        if evidence is None:
            raise RuntimeError("Evaluator catalog result lacks selected U7 evidence")
        existing = by_result.setdefault(evidence.result_id, evidence)
        if existing != evidence:
            raise RuntimeError("Evaluator catalog selected U7 evidence is inconsistent")
    return tuple(by_result[key] for key in sorted(by_result))


def _candidate_selected_evidence(
    candidate: HealthCandidate,
    catalog: ResultCatalog,
) -> tuple[SelectedResultEvidence, ...]:
    by_result = {
        result.result_id: result.selected_evidence
        for result in catalog.candidate_results
    }
    selected = tuple(by_result.get(source.result_id) for source in candidate.source_snapshot.results)
    if any(evidence is None for evidence in selected):
        raise RuntimeError("Candidate source lacks selected U7 evidence")
    return tuple(evidence for evidence in selected if evidence is not None)


def _assert_candidate_rebuild(
    plugin: Any,
    candidate: HealthCandidate,
    registered: RegisteredValidationTest,
    result_connection: sqlite3.Connection,
    catalog: ResultCatalog,
    active: dict[str, Any],
    limit: int,
) -> None:
    """Rebuild exact candidate content while U7 selected evidence is reserved."""

    with sqlite_connection_projection(result_connection) as result_projection:
        current_catalog = _load_result_catalog(
            result_projection,
            registered,
            plugin,
            active_baseline_ids={
                key: value.candidate.baseline_id for key, value in active.items()
            },
            limit=limit,
        )
        _assert_catalog_source_generation(catalog, current_catalog)
        projected_sources = SourceSnapshot(
            tuple(
                sorted(
                    (
                        result.source_result
                        for result in current_catalog.candidate_results
                        if result.combination == candidate.combination
                        and result.source_result is not None
                    ),
                    key=lambda result: result.result_id,
                )
            )
        )
        if projected_sources != candidate.source_snapshot:
            raise IngestionConflictError(
                "Candidate source snapshot changed before candidate store"
            )
        context = HealthContext(
            definition=registered.definition,
            result_db_path=result_projection,
            combination=candidate.combination,
            source_snapshot=projected_sources,
            parent_baseline_id=candidate.parent_baseline_id,
            evaluator_version=candidate.evaluator_version,
            robust_z_threshold=candidate.robust_z_threshold,
            created_at=candidate.created_at,
        )
        if build_candidate_from_plugin(plugin, context) != candidate:
            raise IngestionConflictError(
                "Candidate adapter evidence changed before candidate store"
            )


def _assert_catalog_source_generation(
    expected: ResultCatalog,
    current: ResultCatalog,
) -> None:
    """Require the same complete candidate/adapter source generation.

    Classification-history state is intentionally excluded. A concurrent exact
    history append is resolved by the atomic store and its per-record outcome.
    """

    if (
        current.result_count != expected.result_count
        or current.candidate_results != expected.candidate_results
        or current.adapter_schema_initialized != expected.adapter_schema_initialized
    ):
        raise IngestionConflictError(
            "U7 result/adapter catalog changed after evaluator preflight"
        )


def _assert_adapter_schema_state(
    connection: sqlite3.Connection,
    plugin: Any,
    catalog: ResultCatalog,
) -> None:
    present = plugin.validate_schema(connection, True)
    if present != catalog.adapter_schema_initialized:
        raise IngestionConflictError(
            "U7 adapter schema state changed before evaluator migration"
        )
    if present and not plugin.validate_schema(connection, False):
        raise RuntimeError("Health evaluator requires the exact adapter schema")


def _evaluate_candidates(
    config: CvalConfig,
    registered: RegisteredValidationTest,
    plugin: Any,
    catalog: ResultCatalog,
    *,
    health_path: Path,
    result_path: Path,
    apply: bool,
    now: int,
) -> tuple[CandidateAction, ...]:
    if apply:
        raise ValueError("Candidate writes require the preflight-then-persist cycle")
    actions, _ = _plan_candidates(
        config,
        registered,
        plugin,
        catalog,
        health_path=health_path,
        result_path=result_path,
        now=now,
    )
    return actions


def _plan_candidates(
    config: CvalConfig,
    registered: RegisteredValidationTest,
    plugin: Any,
    catalog: ResultCatalog,
    *,
    health_path: Path,
    result_path: str | Path,
    now: int,
    health_binding: Any | None = None,
    key_binding: Any | None = None,
) -> tuple[tuple[CandidateAction, ...], tuple[HealthCandidate, ...]]:
    health = registered.definition.health
    assert health is not None and health.enabled
    grouped: dict[str, tuple[EnvironmentCombination, list[SourceResult]]] = {}
    for result in catalog.candidate_results:
        if result.source_result is None or result.combination is None:
            continue
        entry = grouped.setdefault(
            result.combination.key,
            (result.combination, []),
        )
        entry[1].append(result.source_result)

    actions: list[CandidateAction] = []
    candidates: list[HealthCandidate] = []
    for combination_key in sorted(grouped):
        combination, sources = grouped[combination_key]
        snapshot = SourceSnapshot(tuple(sorted(sources, key=lambda item: item.result_id)))
        cursor = get_chain_cursor(
            registered.id,
            combination.key,
            db_path=health_path,
            state_binding=health_binding,
            key_binding=key_binding,
        )
        decision = evaluate_build_trigger(
            snapshot.result_ids,
            cursor.latest_source_result_ids,
            min_samples=health.min_samples,
            min_new_results=health.min_new_results,
        )
        if not decision.eligible:
            actions.append(
                CandidateAction(
                    combination.key,
                    "not-eligible",
                    None,
                    decision.qualifying_count,
                    decision.new_result_count,
                    decision.reasons,
                )
            )
            continue
        context = HealthContext(
            definition=registered.definition,
            result_db_path=result_path,
            combination=combination,
            source_snapshot=snapshot,
            parent_baseline_id=cursor.active_baseline_id,
            evaluator_version=HEALTH_ENGINE_VERSION,
            robust_z_threshold=config.baseline.robust_z_threshold,
            created_at=max(now, snapshot.last_timestamp or now),
        )
        candidate = build_candidate_from_plugin(plugin, context)
        candidates.append(candidate)
        action = "would-store"
        baseline_id = candidate.baseline_id
        actions.append(
            CandidateAction(
                combination.key,
                action,
                baseline_id,
                decision.qualifying_count,
                decision.new_result_count,
            )
        )
    return tuple(actions), tuple(candidates)


def _evaluate_classifications(
    config: CvalConfig,
    registered: RegisteredValidationTest,
    plugin: Any,
    catalog: ResultCatalog,
    *,
    health_path: Path,
    result_path: str | Path,
    now: int,
    active: dict[str, Any] | None = None,
) -> tuple[
    tuple[ClassificationAction, ...],
    tuple[ClassificationHistoryRecord, ...],
    int,
]:
    active = _active_baselines(registered.id, health_path) if active is None else active
    actions: list[ClassificationAction] = []
    records: list[ClassificationHistoryRecord] = []
    idempotent = 0
    selected = sorted(
        (*catalog.classification_results, *catalog.deferred_results),
        key=lambda result: result.result_id,
    )
    for result in selected:
        if result.defer_reason is not None:
            actions.append(
                ClassificationAction(
                    result.run_id,
                    "deferred",
                    None,
                    None,
                    None,
                    None,
                    result.defer_reason,
                )
            )
            continue
        baseline = active.get(result.combination.key) if result.combination else None
        source_snapshot = (
            SourceSnapshot((result.source_result,))
            if result.source_result is not None
            else SourceSnapshot(())
        )
        context = HealthContext(
            definition=registered.definition,
            result_db_path=result_path,
            combination=result.combination,
            source_snapshot=source_snapshot,
            evaluator_version=HEALTH_ENGINE_VERSION,
            robust_z_threshold=config.baseline.robust_z_threshold,
            raw_status=result.status,
            created_at=now,
        )
        verdict = classify_from_plugin(plugin, context, baseline)
        record = _history_record(result, verdict, registered=registered, now=now)
        if result.existing_evidence_digest is not None:
            if record.evidence_digest != result.existing_evidence_digest:
                raise IngestionConflictError(
                    "Existing classification target has different verdict evidence"
                )
            idempotent += 1
            actions.append(_classification_action(result.run_id, verdict, "idempotent"))
            continue
        records.append(record)
        actions.append(_classification_action(result.run_id, verdict, "would-store"))
    return tuple(actions), tuple(records), idempotent


def _active_baselines(test_id: str, health_path: Path) -> dict[str, Any]:
    active, _generation = _active_baselines_generation(test_id, health_path)
    return active


def _active_baselines_generation(
    test_id: str,
    health_path: Path,
    *,
    health_binding: Any | None = None,
    key_binding: Any | None = None,
) -> tuple[dict[str, Any], Any]:
    stored_items, generation = load_baselines_generation(
        db_path=health_path,
        test_id=test_id,
        state_binding=health_binding,
        key_binding=key_binding,
    )
    active = {
        stored.candidate.combination.key: stored
        for stored in stored_items
        if stored.lifecycle is BaselineLifecycle.ACTIVE
    }
    return active, generation


def _classification_action(
    run_id: str,
    verdict: HealthVerdict,
    action: str,
) -> ClassificationAction:
    return ClassificationAction(
        run_id=run_id,
        action=action,
        class_code=int(verdict.class_code),
        class_name=verdict.class_name,
        baseline_id=verdict.baseline_id,
        dnr_reason=(verdict.dnr_reason.value if verdict.dnr_reason is not None else None),
    )


def _history_record(
    result: CatalogResult,
    verdict: HealthVerdict,
    *,
    registered: RegisteredValidationTest,
    now: int,
) -> ClassificationHistoryRecord:
    dnr_reason = verdict.dnr_reason.value if verdict.dnr_reason is not None else None
    target = result.target or _classification_target(
        result,
        registered,
        baseline_id=verdict.baseline_id,
    )
    if target is None:
        raise RuntimeError("Deferred classification cannot produce history")
    key_payload = json.dumps(
        {"run_id": result.run_id, "baseline_identity": target.baseline_identity},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    metric_json = json.dumps(
        [
            {
                "class_code": int(metric.class_code),
                "class_name": metric.class_name,
                "metric_name": metric.metric_name,
                "pct_diff": metric.pct_diff,
                "severity_pct": metric.severity_pct,
                "source": metric.source,
                "value": metric.value,
            }
            for metric in verdict.metrics
        ],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    evidence_digest = _canonical_digest(
        {
            "target_digest": target.target_digest,
            "baseline_id": verdict.baseline_id,
            "combination_key": verdict.combination_key,
            "health_class_name": verdict.class_name,
            "health_class_numerical": int(verdict.class_code),
            "dnr_reason": dnr_reason,
            "evaluator_version": HEALTH_ENGINE_VERSION,
            "metric_verdicts_json": metric_json,
            "details_json": verdict.details_json,
        }
    )
    return ClassificationHistoryRecord(
        classification_key="sha256:" + hashlib.sha256(key_payload).hexdigest(),
        result_id=result.result_id,
        run_id=result.run_id,
        baseline_id=verdict.baseline_id,
        baseline_identity=target.baseline_identity,
        target_digest=target.target_digest,
        evidence_digest=evidence_digest,
        combination_key=verdict.combination_key,
        health_class_name=verdict.class_name,
        health_class_numerical=int(verdict.class_code),
        dnr_reason=dnr_reason,
        classified_at=now,
        evaluator_version=HEALTH_ENGINE_VERSION,
        metric_verdicts_json=metric_json,
        details_json=verdict.details_json,
    )


def _classification_target(
    result: CatalogResult,
    registered: RegisteredValidationTest,
    *,
    baseline_id: str | None,
) -> ClassificationTarget | None:
    if result.defer_reason is not None:
        return None
    if result.status == "fail":
        category = "raw_failed"
    elif result.status == "incomplete":
        category = "raw_incomplete"
    elif result.combination is None:
        category = "missing_combination"
    elif baseline_id is None:
        category = "no_active_baseline"
    else:
        category = "active_baseline"
    health = registered.definition.health
    if health is None or not health.enabled:
        raise RuntimeError("Classification target requires enabled health configuration")
    identity_payload = {
        "test_id": registered.id,
        "test_config_digest": validation_test_config_digest(registered),
        "health_policy_version": health.policy_version,
        "evaluator_version": HEALTH_ENGINE_VERSION,
        "adapter_schema_version": result.adapter_schema_version,
        "combination_key": result.combination.key if result.combination else "",
        "baseline_id": baseline_id,
        "category": category,
    }
    baseline_identity = "ht1:" + hashlib.sha256(
        json.dumps(
            identity_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    target_digest = _canonical_digest(
        {
            "identity": identity_payload,
            "result_id": result.result_id,
            "run_id": result.run_id,
            "raw_status": result.status,
            "result_digest": result.result_digest,
            "raw_result_digest": result.raw_result_digest,
            "stored_test_config_digest": result.test_config_digest,
            "receipt_evidence_digest": result.receipt_evidence_digest,
        }
    )
    return ClassificationTarget(
        baseline_identity=baseline_identity,
        target_digest=target_digest,
        baseline_id=baseline_id,
        category=category,
    )


def _canonical_digest(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _load_result_catalog(
    db_path: str | Path,
    registered: RegisteredValidationTest,
    plugin: Any,
    *,
    active_baseline_ids: dict[str, str],
    limit: int,
    connection: sqlite3.Connection | None = None,
) -> ResultCatalog:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("max_classifications_per_test must be a positive integer")
    if not isinstance(active_baseline_ids, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in active_baseline_ids.items()
    ):
        raise TypeError("active_baseline_ids must be a string mapping")
    owns_connection = connection is None
    if owns_connection:
        connection = (
            sqlite3.connect(str(db_path), uri=True, timeout=30)
            if is_snapshot_uri(db_path)
            else connect_sqlite_file(db_path, mode="ro", timeout=30)
        )
    assert connection is not None
    scope = closing(connection) if owns_connection else nullcontext(connection)
    with scope as connection:
        if owns_connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN")
        elif not connection.in_transaction:
            raise RuntimeError(
                "Connection-based result catalog requires an active transaction"
            )
        validate_common_result_connection(connection)
        schema_version = common_result_schema_version(connection)
        adapter_present = plugin.validate_schema(connection, True)
        version_rows = connection.execute(
            "SELECT test_id, version, applied_at FROM adapter_schema_versions ORDER BY test_id"
        ).fetchall()
        receipt_count = _non_negative_int(
            connection.execute(
                "SELECT COUNT(*) FROM metric_ingestion_receipts"
            ).fetchone()[0],
            "adapter receipt count",
        )
        if not adapter_present and not version_rows and receipt_count == 0:
            adapter_version: int | None = None
        elif not adapter_present or not version_rows or (
            len(version_rows) != 1
            or version_rows[0][0] != registered.id
            or isinstance(version_rows[0][1], bool)
            or not isinstance(version_rows[0][1], int)
            or version_rows[0][1] <= 0
            or isinstance(version_rows[0][2], bool)
            or not isinstance(version_rows[0][2], int)
            or version_rows[0][2] < 0
        ):
            raise RuntimeError(
                "Health evaluator adapter schema/version/receipt state is partial"
            )
        else:
            adapter_version = version_rows[0][1]
            if not plugin.validate_schema(connection, False):
                raise RuntimeError("Health evaluator requires the exact adapter schema")
        select_rows = (
            "SELECT tr.result_id, tr.run_id, tr.test_id, tr.node, tr.run_timestamp, "
            "tr.started_timestamp, tr.completed_timestamp, tr.status, tr.image_name, "
            "tr.pytorch_version, tr.cuda_version, tr.test_config_digest, "
            "tr.combination_key, tr.raw_result_json, tr.result_digest, "
            "mr.test_id, mr.adapter_api_version, mr.evidence_digest, "
            "mr.inserted_count, mr.updated_count, mr.metric_names_json, mr.created_at "
            "FROM test_results tr LEFT JOIN metric_ingestion_receipts mr "
            "ON mr.run_id=tr.run_id "
        )
        expected_config = validation_test_config_digest(registered)
        candidate_results: list[CatalogResult] = []
        classification_results: list[CatalogResult] = []
        verification_results: list[CatalogResult] = []
        deferred_results: list[CatalogResult] = []
        deferred_count = 0
        backlog = 0
        validated_receipt_count = 0
        result_count = _non_negative_int(
            connection.execute("SELECT COUNT(*) FROM test_results").fetchone()[0],
            "result count",
        )
        last_result_id = 0
        while True:
            rows = connection.execute(
                select_rows
                + "WHERE tr.result_id > ? ORDER BY tr.result_id ASC LIMIT ?",
                (last_result_id, _CATALOG_QUERY_PAGE_SIZE),
            ).fetchall()
            if not rows:
                break
            if len(rows) > _CATALOG_QUERY_PAGE_SIZE:
                raise RuntimeError("Health evaluator result page exceeded its bound")
            page_targets: list[CatalogResult] = []
            for row in rows:
                result = _catalog_result_from_row(
                    row,
                    registered,
                    adapter_version=adapter_version,
                    expected_config_digest=expected_config,
                )
                if row[15] is not None:
                    validated_receipt_count += 1
                last_result_id = result.result_id
                if result.source_result is not None:
                    candidate_results.append(result)
                baseline_id = (
                    active_baseline_ids.get(result.combination.key)
                    if result.combination is not None
                    else None
                )
                target = _classification_target(
                    result,
                    registered,
                    baseline_id=baseline_id,
                )
                if target is None:
                    deferred_count += 1
                    if len(deferred_results) < limit:
                        deferred_results.append(result)
                    continue
                result = CatalogResult(
                    result_id=result.result_id,
                    run_id=result.run_id,
                    status=result.status,
                    combination=result.combination,
                    source_result=result.source_result,
                    defer_reason=result.defer_reason,
                    result_digest=result.result_digest,
                    raw_result_digest=result.raw_result_digest,
                    test_config_digest=result.test_config_digest,
                    adapter_schema_version=result.adapter_schema_version,
                    receipt_evidence_digest=result.receipt_evidence_digest,
                    target=target,
                    selected_evidence=result.selected_evidence,
                )
                page_targets.append(result)
            existing_targets = (
                _load_existing_classification_targets(connection, page_targets)
                if schema_version == 2
                else {}
            )
            for result in page_targets:
                assert result.target is not None
                existing = existing_targets.get(
                    (result.run_id, result.target.baseline_identity)
                )
                if existing is not None:
                    if (
                        not isinstance(existing[0], str)
                        or existing[0] != result.target.target_digest
                        or not isinstance(existing[1], str)
                        or not _DIGEST_PATTERN.fullmatch(existing[1])
                    ):
                        raise IngestionConflictError(
                            "Existing classification target has different canonical evidence"
                        )
                    if len(verification_results) < limit:
                        verification_results.append(
                            CatalogResult(
                                result_id=result.result_id,
                                run_id=result.run_id,
                                status=result.status,
                                combination=result.combination,
                                source_result=result.source_result,
                                defer_reason=result.defer_reason,
                                result_digest=result.result_digest,
                                raw_result_digest=result.raw_result_digest,
                                test_config_digest=result.test_config_digest,
                                adapter_schema_version=result.adapter_schema_version,
                                receipt_evidence_digest=result.receipt_evidence_digest,
                                target=result.target,
                                existing_evidence_digest=existing[1],
                                selected_evidence=result.selected_evidence,
                            )
                        )
                    continue
                backlog += 1
                if len(classification_results) < limit:
                    classification_results.append(result)
        if validated_receipt_count != receipt_count:
            raise RuntimeError(
                "Health evaluator validated receipt count does not match receipts "
                "joined to exact parent test_results"
            )
    return ResultCatalog(
        schema_version=schema_version,
        result_count=result_count,
        candidate_results=tuple(candidate_results),
        classification_results=tuple(
            classification_results if backlog else verification_results
        ),
        classification_backlog=backlog,
        adapter_schema_initialized=adapter_version is not None,
        deferred_results=tuple(deferred_results),
        deferred_count=deferred_count,
    )


def _load_existing_classification_targets(
    connection: sqlite3.Connection,
    targets: list[CatalogResult],
) -> dict[tuple[str, str], tuple[str, str]]:
    """Load one bounded exact target page through the unique history index."""

    if not targets:
        return {}
    if len(targets) > _CATALOG_QUERY_PAGE_SIZE:
        raise ValueError("Classification target lookup exceeds the query page bound")
    values_sql = ", ".join("(?, ?)" for _ in targets)
    parameters = tuple(
        value
        for result in targets
        for value in (
            result.run_id,
            result.target.baseline_identity if result.target is not None else "",
        )
    )
    rows = connection.execute(
        "WITH selected(run_id, baseline_identity) AS (VALUES "
        f"{values_sql}) "
        "SELECT history.run_id, history.baseline_identity, "
        "history.target_digest, history.evidence_digest "
        "FROM selected JOIN classification_history AS history "
        "ON history.run_id=selected.run_id "
        "AND history.baseline_identity=selected.baseline_identity",
        parameters,
    ).fetchall()
    if len(rows) > len(targets):
        raise RuntimeError("Classification target lookup returned duplicate rows")
    return {
        (str(row[0]), str(row[1])): (str(row[2]), str(row[3]))
        for row in rows
    }


def _catalog_result_from_row(
    row: tuple[Any, ...],
    registered: RegisteredValidationTest,
    *,
    adapter_version: int | None,
    expected_config_digest: str,
) -> CatalogResult:
    if len(row) != 22:
        raise RuntimeError("Health evaluator result catalog row shape is invalid")
    result_id = _positive_int(row[0], "result_id")
    run_id = _text(row[1], "run_id")
    if row[2] != registered.id:
        raise RuntimeError("Health evaluator result owner is invalid")
    _text(row[3], "node")
    _non_negative_int(row[4], "run_timestamp")
    if row[5] is not None:
        _non_negative_int(row[5], "started_timestamp")
    if row[6] is not None:
        _non_negative_int(row[6], "completed_timestamp")
    status = _text(row[7], "status")
    if status not in {"pass", "fail", "incomplete"}:
        raise RuntimeError("Health evaluator raw status is invalid")
    common = {
        "image_name": _text(row[8], "image_name", allow_empty=True),
        "pytorch_version": _text(row[9], "pytorch_version", allow_empty=True),
        "cuda_version": _text(row[10], "cuda_version", allow_empty=True),
    }
    test_config_digest = _digest(row[11], "test_config_digest")
    stored_combination_key = _text(row[12], "combination_key", allow_empty=True)
    if stored_combination_key and not _DIGEST_PATTERN.fullmatch(stored_combination_key):
        raise RuntimeError("Health evaluator combination key is invalid")
    raw_json = _text(row[13], "raw_result_json")
    result_digest = _digest(row[14], "result_digest")
    try:
        raw_payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Health evaluator raw result JSON is invalid") from exc
    if (
        not isinstance(raw_payload, dict)
        or raw_payload.get("schema_version") != "cval.test-result.v1"
        or raw_payload.get("test_id") != registered.id
        or raw_payload.get("status") != status
        or json.dumps(raw_payload, sort_keys=True, separators=(",", ":")) != raw_json
    ):
        raise RuntimeError("Health evaluator raw result JSON provenance is invalid")
    combination = resolve_environment_combination(registered.definition, common)
    raw_result_digest = "sha256:" + hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    common_values = {
        "result_digest": result_digest,
        "raw_result_digest": raw_result_digest,
        "test_config_digest": test_config_digest,
        "adapter_schema_version": adapter_version or 0,
        "receipt_evidence_digest": "",
    }
    receipt_values = tuple(row[15:22])
    if row[15] is None:
        if any(value is not None for value in receipt_values):
            raise RuntimeError("Health evaluator durable receipt state is partial")
        receipt = (None, None, None, None, None, None, None)
    else:
        if (
            row[15] != row[2]
            or row[15] != registered.id
            or row[16] != PLUGIN_API_VERSION
            or not isinstance(row[17], str)
            or not _DIGEST_PATTERN.fullmatch(row[17])
            or _positive_int(row[18], "receipt inserted_count") <= 0
            or _non_negative_int(row[19], "receipt updated_count") != 0
            or not isinstance(row[20], str)
            or _non_negative_int(row[21], "receipt created_at") < 0
        ):
            raise RuntimeError("Health evaluator durable receipt manifest is invalid")
        try:
            metric_names = json.loads(row[20])
        except json.JSONDecodeError as exc:
            raise RuntimeError("Health evaluator receipt metric names are invalid") from exc
        if (
            not isinstance(metric_names, list)
            or not metric_names
            or metric_names != sorted(set(metric_names))
            or not all(isinstance(name, str) and name for name in metric_names)
            or json.dumps(metric_names, separators=(",", ":")) != row[20]
        ):
            raise RuntimeError("Health evaluator receipt metric names are invalid")
        receipt = receipt_values
        common_values["receipt_evidence_digest"] = row[17]
    common_values["selected_evidence"] = SelectedResultEvidence(
        result_id=result_id,
        run_id=run_id,
        completed_timestamp=row[6],
        status=status,
        result_digest=result_digest,
        raw_result_digest=raw_result_digest,
        test_config_digest=test_config_digest,
        combination_key=stored_combination_key,
        receipt_test_id=receipt[0],
        receipt_adapter_api_version=receipt[1],
        receipt_evidence_digest=receipt[2],
        receipt_inserted_count=receipt[3],
        receipt_updated_count=receipt[4],
        receipt_metric_names_json=receipt[5],
        receipt_created_at=receipt[6],
    )
    if status != "pass":
        return CatalogResult(
            result_id, run_id, status, combination, None, None, **common_values
        )
    if combination is not None and combination.key != stored_combination_key:
        return CatalogResult(
            result_id,
            run_id,
            status,
            combination,
            None,
            "stored environment combination does not match current descriptor",
            **common_values,
        )
    if test_config_digest != expected_config_digest:
        return CatalogResult(
            result_id,
            run_id,
            status,
            combination,
            None,
            "result test configuration does not match the current descriptor",
            **common_values,
        )
    if adapter_version is None:
        return CatalogResult(
            result_id,
            run_id,
            status,
            combination,
            None,
            "passing result requires initialized adapter schema and receipt",
            **common_values,
        )
    if row[15] is None:
        return CatalogResult(
            result_id,
            run_id,
            status,
            combination,
            None,
            "passing result has no durable ingestion receipt",
            **common_values,
        )
    if combination is None:
        return CatalogResult(
            result_id,
            run_id,
            status,
            None,
            None,
            None,
            **{
                **common_values,
                "receipt_evidence_digest": row[17],
            },
        )
    if row[6] is None:
        raise RuntimeError("Passing health result has no completion timestamp")
    source = SourceResult(
        result_id=result_id,
        run_id=run_id,
        completed_timestamp=_non_negative_int(row[6], "completed_timestamp"),
        result_digest=result_digest,
        raw_result_digest=raw_result_digest,
        test_config_digest=test_config_digest,
        combination_key=combination.key,
        adapter_schema_version=adapter_version,
        receipt_evidence_digest=row[17],
    )
    return CatalogResult(
        result_id,
        run_id,
        status,
        combination,
        source,
        None,
        **{
            **common_values,
            "receipt_evidence_digest": row[17],
        },
    )


def _has_health_and_ingest_capabilities(test: RegisteredValidationTest) -> bool:
    health = test.definition.health
    plugin = test.definition.plugin
    return bool(
        health
        and health.enabled
        and plugin
        and {"health", "ingest"}.issubset(plugin.capabilities)
    )


def _require_apply_gate(
    config: CvalConfig,
    *,
    apply: bool,
    confirmation: str | None,
    expected: str,
) -> None:
    if not apply:
        if confirmation is not None:
            raise HealthEvaluatorPolicyError("Confirmation is valid only with --apply")
        return
    if not config.health_evaluator.write_enabled:
        raise HealthEvaluatorPolicyError(
            "health_evaluator.write_enabled=false blocks derived database writes"
        )
    if confirmation != expected:
        raise HealthEvaluatorPolicyError(
            f"Apply requires exact confirmation {expected!r}"
        )


def _error_report(
    config: CvalConfig,
    registered: RegisteredValidationTest,
    message: str,
) -> TestEvaluationReport:
    try:
        result_path = resolve_test_results_db_path(
            config.health_evaluator.state_root,
            registered,
        )
        result_value = str(result_path)
    except Exception:  # noqa: BLE001 - report must not mask the original error
        result_value = str(
            Path(config.health_evaluator.state_root)
            / registered.definition.artifacts.results_db_path
        )
    try:
        health_path = resolve_health_db_path(
            config.health_evaluator.state_root,
            registered,
        )
        health_value = str(health_path)
    except Exception:  # noqa: BLE001 - report must not mask the original error
        health_value = str(
            Path(config.health_evaluator.state_root)
            / registered.definition.artifacts.health_classes_db_path
        )
    return TestEvaluationReport(
        test_id=registered.id,
        status="error",
        result_db_path=result_value,
        health_db_path=health_value,
        source_schema_version=None,
        result_count=0,
        error=message,
    )


def _safe_error(exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    return " ".join(message.splitlines())


def _non_negative_timestamp(value: int | None) -> int:
    timestamp = int(time.time()) if value is None else value
    return _non_negative_int(timestamp, "evaluator timestamp")


def _positive_int(value: Any, field_name: str) -> int:
    result = _non_negative_int(value, field_name)
    if result == 0:
        raise RuntimeError(f"Health evaluator {field_name} must be positive")
    return result


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(
            f"Health evaluator {field_name} must be a non-negative integer"
        )
    return value


def _text(value: Any, field_name: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise RuntimeError(f"Health evaluator {field_name} must be text")
    return value


def _digest(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
        raise RuntimeError(f"Health evaluator {field_name} must be a SHA-256 digest")
    return value


__all__ = [
    "ACTIVATE_CONFIRMATION",
    "EVALUATE_CONFIRMATION",
    "ActivationReport",
    "EvaluationCycleReport",
    "HealthEvaluatorError",
    "HealthEvaluatorLockError",
    "HealthEvaluatorPolicyError",
    "TestEvaluationReport",
    "activate_health_candidate",
    "evaluate_health_cycle",
]
