"""Pure versioned health candidate construction and classification."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import asdict
from typing import Any, Iterable

from cval.baselines import stats
from cval.health.combination import (
    validate_combination_for_definition,
    validate_environment_combination,
)
from cval.health.models import (
    BuildDecision,
    BaselineLifecycle,
    DnrReason,
    EnvironmentCombination,
    HealthContext,
    HealthCandidate,
    HealthClassCode,
    HealthVerdict,
    MetricBaseline,
    MetricObservation,
    MetricSpec,
    MetricVerdict,
    QualityGate,
    QualityReport,
    ResultSampleCoverage,
    SourceCoverage,
    SourceResult,
    SourceSnapshot,
    StoredHealthBaseline,
    ThresholdBand,
)
from cval.validation.registry import (
    ValidationTestDefinition,
    validation_test_config_digest,
)


HEALTH_ENGINE_VERSION = "cval.health.v1"
HEALTH_METHOD = "robust_mad_bands.v1"
DECLARATIVE_AGGREGATION_POLICY = "max_metric_class.v1"
_BASELINE_ID_PATTERN = re.compile(r"^hb1:[0-9a-f]{64}$")
_PAYLOAD_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_VERSIONED_POLICY_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]*\.v[1-9][0-9]*$")
_VALID_RAW_STATUSES = frozenset({"pass", "fail", "incomplete"})
_VALID_DIRECTIONS = frozenset({"low_bad", "high_bad", "two_sided", "absolute"})


def metric_specs_from_definition(
    definition: ValidationTestDefinition,
) -> tuple[MetricSpec, ...]:
    """Return the descriptor's declared metric rules as engine value objects."""

    health = definition.health
    if health is None or not health.enabled:
        return ()
    return tuple(
        MetricSpec(
            name=metric.name,
            source=metric.source,
            direction=metric.direction,
            tolerance_pct=metric.tolerance_pct,
            units=metric.units,
            weight=metric.weight,
        )
        for metric in health.metrics
    )


def validate_metric_specs(
    specs: Iterable[MetricSpec],
    definition: ValidationTestDefinition,
) -> tuple[MetricSpec, ...]:
    """Validate adapter metric specs and require exact descriptor agreement."""

    values = tuple(specs)
    if not all(isinstance(spec, MetricSpec) for spec in values):
        raise TypeError("Health metric_specs must contain only MetricSpec values")
    expected = metric_specs_from_definition(definition)
    if values != expected:
        raise ValueError("Health adapter metric_specs do not match the test descriptor")
    seen_names: set[str] = set()
    seen_sources: set[str] = set()
    for spec in values:
        if not spec.name or not spec.source:
            raise ValueError("Health metric names and sources must be non-empty")
        if spec.name in seen_names or spec.source in seen_sources:
            raise ValueError("Health metric names and sources must be unique")
        seen_names.add(spec.name)
        seen_sources.add(spec.source)
        if spec.direction not in _VALID_DIRECTIONS:
            raise ValueError(f"Unsupported health metric direction: {spec.direction!r}")
        if not math.isfinite(spec.tolerance_pct) or spec.tolerance_pct < 0:
            raise ValueError("Health metric tolerance_pct must be finite and non-negative")
        if not math.isfinite(spec.weight) or spec.weight <= 0:
            raise ValueError("Health metric weight must be finite and positive")
    return values


def validate_source_snapshot(snapshot: SourceSnapshot) -> None:
    if not isinstance(snapshot, SourceSnapshot):
        raise TypeError("Expected SourceSnapshot")
    ids: set[int] = set()
    run_ids: set[str] = set()
    previous_id = 0
    adapter_schema_versions: set[int] = set()
    for result in snapshot.results:
        if not isinstance(result, SourceResult):
            raise TypeError("Health source snapshot must contain SourceResult values")
        if (
            isinstance(result.result_id, bool)
            or not isinstance(result.result_id, int)
            or result.result_id <= 0
        ):
            raise ValueError("Health source result IDs must be positive integers")
        if result.result_id in ids or result.run_id in run_ids:
            raise ValueError("Health source results must have unique IDs and run IDs")
        if result.result_id <= previous_id:
            raise ValueError("Health source results must be ordered by result_id")
        if not isinstance(result.run_id, str) or not result.run_id:
            raise ValueError("Health source run IDs must be non-empty")
        if (
            isinstance(result.completed_timestamp, bool)
            or not isinstance(result.completed_timestamp, int)
            or result.completed_timestamp < 0
        ):
            raise ValueError("Health source completion timestamps must be non-negative")
        for field_name, digest in (
            ("result_digest", result.result_digest),
            ("raw_result_digest", result.raw_result_digest),
            ("test_config_digest", result.test_config_digest),
            ("combination_key", result.combination_key),
            ("receipt_evidence_digest", result.receipt_evidence_digest),
        ):
            if not isinstance(digest, str) or not _PAYLOAD_DIGEST_PATTERN.fullmatch(
                digest
            ):
                raise ValueError(f"Health source {field_name} must be a SHA-256 digest")
        if (
            isinstance(result.adapter_schema_version, bool)
            or not isinstance(result.adapter_schema_version, int)
            or result.adapter_schema_version <= 0
        ):
            raise ValueError("Health source adapter_schema_version must be positive")
        ids.add(result.result_id)
        run_ids.add(result.run_id)
        adapter_schema_versions.add(result.adapter_schema_version)
        previous_id = result.result_id
    if len(adapter_schema_versions) > 1:
        raise ValueError(
            "Health source results must use one uniform adapter_schema_version"
        )


def validate_observations(
    observations: Iterable[MetricObservation],
    specs: Iterable[MetricSpec],
    *,
    allowed_result_ids: Iterable[int] | None = None,
    source_snapshot: SourceSnapshot | None = None,
    require_exact_result_ids: bool = False,
) -> tuple[MetricObservation, ...]:
    """Validate finite unique adapter observations against declared sources."""

    raw_values = tuple(observations)
    spec_values = tuple(specs)
    sources = {spec.source for spec in spec_values}
    if source_snapshot is not None:
        validate_source_snapshot(source_snapshot)
        snapshot_identity = {
            result.result_id: (result.run_id, result.completed_timestamp)
            for result in source_snapshot.results
        }
        allowed = set(snapshot_identity)
    else:
        snapshot_identity = None
        allowed = (
            _strict_result_id_set(allowed_result_ids, "allowed")
            if allowed_result_ids is not None
            else None
        )
    seen: set[tuple[int, str, str, str]] = set()
    observed_result_ids: set[int] = set()
    values: list[MetricObservation] = []
    for observation in raw_values:
        if not isinstance(observation, MetricObservation):
            raise TypeError("Health observations must contain MetricObservation values")
        if (
            isinstance(observation.result_id, bool)
            or not isinstance(observation.result_id, int)
            or observation.result_id <= 0
        ):
            raise ValueError("Health observation result_id must be a positive integer")
        if allowed is not None and observation.result_id not in allowed:
            raise ValueError("Health observation is outside its source snapshot")
        if snapshot_identity is not None and snapshot_identity[observation.result_id] != (
            observation.run_id,
            observation.completed_timestamp,
        ):
            raise ValueError("Health observation identity does not match its source result")
        if (
            not isinstance(observation.run_id, str)
            or not observation.run_id
            or not isinstance(observation.source, str)
            or not observation.source
            or not isinstance(observation.metric_name, str)
            or not observation.metric_name
        ):
            raise ValueError("Health observation identity fields must be non-empty")
        if not isinstance(observation.sample_key, str) or not observation.sample_key:
            raise ValueError("Health observation sample_key must be non-empty")
        if observation.source not in sources:
            raise ValueError(f"Health observation uses undeclared source {observation.source!r}")
        if isinstance(observation.value, bool) or not isinstance(
            observation.value, int | float
        ):
            raise ValueError("Health observation value must be numeric")
        if not math.isfinite(float(observation.value)):
            raise ValueError("Health observation value must be finite")
        normalized_value = float(observation.value)
        if normalized_value == 0.0:
            normalized_value = 0.0
        if (
            isinstance(observation.completed_timestamp, bool)
            or not isinstance(observation.completed_timestamp, int)
            or observation.completed_timestamp < 0
        ):
            raise ValueError("Health observation completion timestamp is invalid")
        identity = (
            observation.result_id,
            observation.source,
            observation.metric_name,
            observation.sample_key,
        )
        if identity in seen:
            raise ValueError(f"Duplicate health observation identity: {identity!r}")
        seen.add(identity)
        observed_result_ids.add(observation.result_id)
        values.append(
            observation
            if type(observation.value) is float
            and observation.value == normalized_value
            and not (
                observation.value == 0.0
                and math.copysign(1.0, observation.value) < 0
            )
            else MetricObservation(
                result_id=observation.result_id,
                run_id=observation.run_id,
                completed_timestamp=observation.completed_timestamp,
                source=observation.source,
                metric_name=observation.metric_name,
                sample_key=observation.sample_key,
                value=normalized_value,
            )
        )
    if require_exact_result_ids and allowed is not None and observed_result_ids != allowed:
        raise ValueError("Health observations do not exactly cover the source snapshot")
    return tuple(values)


def validate_source_snapshot_context(
    snapshot: SourceSnapshot,
    definition: ValidationTestDefinition,
    combination: EnvironmentCombination,
    *,
    require_nonempty: bool,
) -> None:
    """Bind every source result to the current descriptor and environment stratum."""

    validate_source_snapshot(snapshot)
    if require_nonempty and not snapshot.results:
        raise ValueError("Evaluable health operation requires a nonempty source snapshot")
    expected_config_digest = validation_test_config_digest(definition)
    for result in snapshot.results:
        if (
            result.test_config_digest != expected_config_digest
            or result.combination_key != combination.key
        ):
            raise ValueError(
                "Health source snapshot does not match the current config/combination"
            )


def evaluate_build_trigger(
    current_result_ids: Iterable[int],
    previous_result_ids: Iterable[int],
    *,
    min_samples: int,
    min_new_results: int,
) -> BuildDecision:
    """Evaluate distinct qualifying-result and genuinely-new-result thresholds."""

    if (
        isinstance(min_samples, bool)
        or not isinstance(min_samples, int)
        or min_samples < 1
    ):
        raise ValueError("min_samples must be a positive integer")
    if (
        isinstance(min_new_results, bool)
        or not isinstance(min_new_results, int)
        or min_new_results < 1
    ):
        raise ValueError("min_new_results must be a positive integer")
    current = _strict_result_id_set(current_result_ids, "current")
    previous = _strict_result_id_set(previous_result_ids, "previous")
    new_count = len(current - previous)
    reasons: list[str] = []
    if len(current) < min_samples:
        reasons.append("insufficient_samples")
    if new_count < min_new_results:
        reasons.append("insufficient_new_results")
    return BuildDecision(
        eligible=not reasons,
        qualifying_count=len(current),
        new_result_count=new_count,
        reasons=tuple(reasons),
    )


def _build_declarative_candidate(
    definition: ValidationTestDefinition,
    combination: EnvironmentCombination,
    specs: Iterable[MetricSpec],
    observations: Iterable[MetricObservation],
    source_snapshot: SourceSnapshot,
    *,
    parent_baseline_id: str | None = None,
    evaluator_version: str = HEALTH_ENGINE_VERSION,
    created_at: int | None = None,
    excluded_result_count: int = 0,
    robust_z_threshold: float | None = None,
) -> HealthCandidate:
    """Build one immutable content-addressed robust candidate."""

    health = definition.health
    if health is None or not health.enabled:
        raise ValueError("Cannot build a health candidate for a health-disabled test")
    validate_environment_combination(combination)
    validate_source_snapshot_context(
        source_snapshot,
        definition,
        combination,
        require_nonempty=True,
    )
    spec_values = validate_metric_specs(specs, definition)
    observation_values = tuple(
        sorted(
            validate_observations(
                observations,
                spec_values,
                source_snapshot=source_snapshot,
            ),
            key=_observation_sort_key,
        )
    )
    if parent_baseline_id is not None and not _BASELINE_ID_PATTERN.fullmatch(
        parent_baseline_id
    ):
        raise ValueError("parent_baseline_id must be a U8 health baseline ID")
    if not evaluator_version:
        raise ValueError("evaluator_version must be non-empty")
    if isinstance(excluded_result_count, bool) or excluded_result_count < 0:
        raise ValueError("excluded_result_count must be non-negative")
    if created_at is not None and (
        isinstance(created_at, bool)
        or not isinstance(created_at, int)
        or created_at < 0
    ):
        raise ValueError("created_at must be a non-negative integer")
    config_digest = validation_test_config_digest(definition)

    spec_by_source = {spec.source: spec for spec in spec_values}
    grouped: dict[tuple[str, str], list[float]] = {}
    for observation in observation_values:
        grouped.setdefault((observation.source, observation.metric_name), []).append(
            float(observation.value)
        )

    robust_z = (
        health.robust_z_threshold
        if health.robust_z_threshold is not None
        else robust_z_threshold
    )
    if (
        robust_z is None
        or isinstance(robust_z, bool)
        or not isinstance(robust_z, int | float)
        or not math.isfinite(float(robust_z))
        or float(robust_z) <= 0
    ):
        raise ValueError(
            "An effective finite positive robust_z_threshold is required"
        )
    robust_z = float(robust_z)
    metric_baselines: list[MetricBaseline] = []
    for (source, metric_name), raw_values in sorted(grouped.items()):
        spec = spec_by_source[source]
        summarized_values = (
            [abs(value) for value in raw_values]
            if spec.direction == "absolute"
            else raw_values
        )
        kernel_direction = (
            stats.DIRECTION_HIGH_BAD
            if spec.direction == "absolute"
            else spec.direction
        )
        summary = stats.summarize_metric(
            metric_name,
            summarized_values,
            direction=kernel_direction,
            tolerance_pct=spec.tolerance_pct,
            z_threshold=robust_z,
        )
        delta = _summary_delta(summary, kernel_direction)
        statistic_payload = summary.to_dict()
        statistic_payload["configured_direction"] = spec.direction
        statistic_payload["tolerance_pct"] = spec.tolerance_pct
        statistic_payload["robust_z_threshold"] = robust_z
        statistics_json = _canonical_json(statistic_payload)
        metric = MetricBaseline(
            spec_name=spec.name,
            source=source,
            metric_name=metric_name,
            direction=spec.direction,
            units=spec.units,
            weight=spec.weight,
            tolerance_pct=spec.tolerance_pct,
            center=summary.median,
            mad=summary.mad,
            mad_sigma=summary.mad_sigma,
            delta=delta,
            p05=summary.p05,
            p95=summary.p95,
            sample_count=summary.n,
            excluded_count=summary.n_excluded,
            statistics_json=statistics_json,
            thresholds=(),
        )
        metric = MetricBaseline(
            **{
                **asdict(metric),
                "thresholds": _generate_thresholds(metric),
            }
        )
        metric_baselines.append(metric)

    source_coverage = _coverage_from_observations(observation_values)
    adapter_schema_version = source_snapshot.results[0].adapter_schema_version

    provisional = HealthCandidate(
        baseline_id="",
        payload_digest="",
        test_id=definition.metadata.id,
        combination=combination,
        test_config_digest=config_digest,
        health_policy_version=health.policy_version,
        adapter_schema_version=adapter_schema_version,
        evaluator_version=evaluator_version,
        method=HEALTH_METHOD,
        robust_z_threshold=robust_z,
        observation_digest=observation_content_digest(observation_values),
        source_snapshot=source_snapshot,
        observations=observation_values,
        source_coverage=source_coverage,
        metrics=tuple(metric_baselines),
        parent_baseline_id=parent_baseline_id,
        created_at=int(time.time()) if created_at is None else int(created_at),
        excluded_result_count=excluded_result_count,
    )
    payload_digest, baseline_id = _candidate_identity(provisional)
    candidate = HealthCandidate(
        **{
            **asdict(provisional),
            "combination": combination,
            "source_snapshot": source_snapshot,
            "observations": observation_values,
            "source_coverage": source_coverage,
            "metrics": tuple(metric_baselines),
            "payload_digest": payload_digest,
            "baseline_id": baseline_id,
        }
    )
    assert_candidate_integrity(candidate)
    return candidate


def build_candidate_from_plugin(
    plugin: Any,
    context: HealthContext,
) -> HealthCandidate:
    """Load canonical adapter observations and build a framework-owned candidate."""

    health = context.definition.health
    if health is None or not health.enabled:
        raise ValueError("Health candidate context is not health-enabled")
    _validate_plugin_contract_preload(plugin, context.definition)
    if context.combination is None:
        raise ValueError("Health candidate requires a complete environment combination")
    validate_combination_for_definition(context.combination, context.definition)
    validate_source_snapshot_context(
        context.source_snapshot,
        context.definition,
        context.combination,
        require_nonempty=True,
    )
    validate_metric_specs(plugin.metric_specs(context.definition), context.definition)
    return _build_candidate_with_plugin_observations(
        plugin,
        context,
        plugin.load_observations(context),
    )


def _build_candidate_with_plugin_observations(
    plugin: Any,
    context: HealthContext,
    observations: Iterable[MetricObservation],
) -> HealthCandidate:
    """Dispatch declarative/custom candidate construction and validate its identity."""

    health = context.definition.health
    if health is None or not health.enabled:
        raise ValueError("Health candidate context is not health-enabled")
    if context.combination is None:
        raise ValueError("Health candidate requires a complete environment combination")
    if getattr(plugin, "health_policy_version", None) != health.policy_version:
        raise ValueError("Health adapter policy version does not match the descriptor")
    if callable(getattr(plugin, "build_candidate", None)):
        raise ValueError(
            "Health candidate construction is framework-owned; custom adapters "
            "may customize only verdict aggregation"
        )
    specs = validate_metric_specs(
        plugin.metric_specs(context.definition),
        context.definition,
    )
    observation_values = validate_observations(
        observations,
        specs,
        source_snapshot=context.source_snapshot,
    )
    candidate = _build_declarative_candidate(
        context.definition,
        context.combination,
        specs,
        observation_values,
        context.source_snapshot,
        parent_baseline_id=context.parent_baseline_id,
        evaluator_version=context.evaluator_version,
        created_at=context.created_at,
        robust_z_threshold=context.robust_z_threshold,
    )
    assert_candidate_integrity(candidate)
    effective_context_robust_z = (
        health.robust_z_threshold
        if health.robust_z_threshold is not None
        else context.robust_z_threshold
    )
    if (
        candidate.test_id != context.definition.metadata.id
        or candidate.combination != context.combination
        or candidate.source_snapshot != context.source_snapshot
        or candidate.parent_baseline_id != context.parent_baseline_id
        or candidate.evaluator_version != context.evaluator_version
        or candidate.robust_z_threshold != effective_context_robust_z
    ):
        raise ValueError("Custom health candidate does not match its framework context")
    validate_candidate(
        candidate,
        context.definition,
        robust_z_threshold=context.robust_z_threshold,
    )
    return candidate


def classify_from_plugin(
    plugin: Any,
    context: HealthContext,
    baseline: StoredHealthBaseline | None,
) -> HealthVerdict:
    """Load canonical adapter observations and classify through the framework."""

    health = context.definition.health
    if health is None or not health.enabled:
        raise ValueError("Health classification context is not health-enabled")
    dnr = _classification_identity_preflight(
        baseline,
        raw_status=context.raw_status,
        combination=context.combination,
        test_id=context.definition.metadata.id,
        definition=context.definition,
        robust_z_threshold=context.robust_z_threshold,
    )
    if dnr is not None:
        return dnr
    _validate_plugin_contract_preload(plugin, context.definition)
    assert baseline is not None and context.combination is not None
    if not context.source_snapshot.results:
        return _dnr(
            context.definition.metadata.id,
            context.combination,
            baseline.candidate,
            DnrReason.NO_OBSERVATIONS,
        )
    validate_source_snapshot_context(
        context.source_snapshot,
        context.definition,
        context.combination,
        require_nonempty=True,
    )
    if (
        context.source_snapshot.results[0].adapter_schema_version
        != baseline.candidate.adapter_schema_version
    ):
        return _dnr(
            context.definition.metadata.id,
            context.combination,
            baseline.candidate,
            DnrReason.INCOMPATIBLE_ADAPTER_VERSION,
        )
    validate_metric_specs(plugin.metric_specs(context.definition), context.definition)
    observations = plugin.load_observations(context)
    return _classify_with_plugin_observations(
        plugin,
        context,
        baseline,
        observations,
    )


def _validate_plugin_contract_preload(
    plugin: Any,
    definition: ValidationTestDefinition,
) -> None:
    health = definition.health
    if health is None or not health.enabled:
        raise ValueError("Health plugin definition is not enabled")
    if getattr(plugin, "health_policy_version", None) != health.policy_version:
        raise ValueError("Health adapter policy version does not match the descriptor")
    if callable(getattr(plugin, "build_candidate", None)):
        raise ValueError(
            "Health candidate construction is framework-owned; custom adapters "
            "may customize only verdict aggregation"
        )
    if not callable(getattr(plugin, "load_observations", None)):
        raise TypeError("Health adapter must provide load_observations")
    if health.strategy == "custom" and not callable(getattr(plugin, "classify", None)):
        raise TypeError("Custom health adapter must provide classify")


def _classify_with_plugin_observations(
    plugin: Any,
    context: HealthContext,
    baseline: StoredHealthBaseline | None,
    observations: Iterable[MetricObservation],
) -> HealthVerdict:
    """Dispatch declarative/custom classification and validate the returned verdict."""

    health = context.definition.health
    if health is None or not health.enabled:
        raise ValueError("Health classification context is not health-enabled")
    dnr = _classification_identity_preflight(
        baseline,
        raw_status=context.raw_status,
        combination=context.combination,
        test_id=context.definition.metadata.id,
        definition=context.definition,
        robust_z_threshold=context.robust_z_threshold,
    )
    if dnr is not None:
        return dnr
    if getattr(plugin, "health_policy_version", None) != health.policy_version:
        raise ValueError("Health adapter policy version does not match the descriptor")
    if callable(getattr(plugin, "build_candidate", None)):
        raise ValueError(
            "Health candidate construction is framework-owned; custom adapters "
            "may customize only verdict aggregation"
        )
    raw_observations = tuple(observations)
    if not raw_observations:
        assert baseline is not None and context.combination is not None
        return _dnr(
            context.definition.metadata.id,
            context.combination,
            baseline.candidate,
            DnrReason.NO_OBSERVATIONS,
        )
    assert context.combination is not None
    validate_source_snapshot_context(
        context.source_snapshot,
        context.definition,
        context.combination,
        require_nonempty=True,
    )
    specs = validate_metric_specs(
        plugin.metric_specs(context.definition),
        context.definition,
    )
    observation_values = validate_observations(
        raw_observations,
        specs,
        source_snapshot=context.source_snapshot,
        require_exact_result_ids=True,
    )
    assert baseline is not None
    base_verdict = _classify_declarative(
        baseline,
        observation_values,
        raw_status=context.raw_status,
        combination=context.combination,
        test_id=context.definition.metadata.id,
        definition=context.definition,
        source_snapshot=context.source_snapshot,
        robust_z_threshold=context.robust_z_threshold,
    )
    if base_verdict.class_code is HealthClassCode.DNR or health.strategy != "custom":
        return base_verdict
    verdict = plugin.classify(
        context,
        baseline.candidate,
        observation_values,
        base_verdict,
    )
    validate_health_verdict(
        verdict,
        test_id=context.definition.metadata.id,
        combination=context.combination,
        baseline=baseline.candidate,
    )
    if verdict.metrics != base_verdict.metrics:
        raise ValueError("Custom health verdict must preserve framework metric verdicts")
    _validate_custom_aggregation(verdict)
    return verdict


def _candidate_payload(candidate: HealthCandidate) -> dict[str, Any]:
    """Return the canonical identity payload (excluding lifecycle/wall-clock fields)."""

    return {
        "test_id": candidate.test_id,
        "combination_key": candidate.combination.key,
        "combination_factors_json": candidate.combination.factors_json,
        "test_config_digest": candidate.test_config_digest,
        "health_policy_version": candidate.health_policy_version,
        "adapter_schema_version": candidate.adapter_schema_version,
        "evaluator_version": candidate.evaluator_version,
        "method": candidate.method,
        "robust_z_threshold": candidate.robust_z_threshold,
        "observation_digest": candidate.observation_digest,
        "parent_baseline_id": candidate.parent_baseline_id,
        "excluded_result_count": candidate.excluded_result_count,
        "sources": [asdict(result) for result in candidate.source_snapshot.results],
        "observations": [
            asdict(observation)
            for observation in sorted(candidate.observations, key=_observation_sort_key)
        ],
        "source_coverage": [
            asdict(coverage)
            for coverage in sorted(
                candidate.source_coverage,
                key=lambda item: (item.source, item.metric_name),
            )
        ],
        "metrics": [
            {
                "spec_name": metric.spec_name,
                "source": metric.source,
                "metric_name": metric.metric_name,
                "direction": metric.direction,
                "units": metric.units,
                "weight": metric.weight,
                "tolerance_pct": metric.tolerance_pct,
                "center": metric.center,
                "mad": metric.mad,
                "mad_sigma": metric.mad_sigma,
                "delta": metric.delta,
                "p05": metric.p05,
                "p95": metric.p95,
                "sample_count": metric.sample_count,
                "excluded_count": metric.excluded_count,
                "statistics_json": metric.statistics_json,
                "thresholds": [
                    {
                        "class_code": int(band.class_code),
                        "band_index": band.band_index,
                        "lower_bound": band.lower_bound,
                        "upper_bound": band.upper_bound,
                        "lower_inclusive": band.lower_inclusive,
                        "upper_inclusive": band.upper_inclusive,
                    }
                    for band in metric.thresholds
                ],
            }
            for metric in sorted(
                candidate.metrics,
                key=lambda item: (item.source, item.metric_name),
            )
        ],
    }


def _candidate_identity(candidate: HealthCandidate) -> tuple[str, str]:
    encoded = _canonical_json(_candidate_payload(candidate)).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"sha256:{digest}", f"hb1:{digest}"


def assert_candidate_integrity(candidate: HealthCandidate) -> None:
    """Reject malformed or forged candidate value objects."""

    if not isinstance(candidate, HealthCandidate):
        raise TypeError("Expected HealthCandidate")
    validate_environment_combination(candidate.combination)
    validate_source_snapshot(candidate.source_snapshot)
    if (
        not isinstance(candidate.test_id, str)
        or not candidate.test_id
        or not isinstance(candidate.health_policy_version, str)
        or not _VERSIONED_POLICY_PATTERN.fullmatch(candidate.health_policy_version)
        or not isinstance(candidate.evaluator_version, str)
        or not candidate.evaluator_version
        or not isinstance(candidate.method, str)
        or not candidate.method
    ):
        raise ValueError("Health candidate identity fields must be non-empty")
    if candidate.method != HEALTH_METHOD or candidate.evaluator_version != HEALTH_ENGINE_VERSION:
        raise ValueError("Health candidate method/evaluator version is unsupported")
    if not _PAYLOAD_DIGEST_PATTERN.fullmatch(candidate.payload_digest):
        raise ValueError("Health candidate payload_digest is invalid")
    if not _BASELINE_ID_PATTERN.fullmatch(candidate.baseline_id):
        raise ValueError("Health candidate baseline_id is invalid")
    if candidate.parent_baseline_id is not None and not _BASELINE_ID_PATTERN.fullmatch(
        candidate.parent_baseline_id
    ):
        raise ValueError("Health candidate parent_baseline_id is invalid")
    if (
        isinstance(candidate.created_at, bool)
        or not isinstance(candidate.created_at, int)
        or candidate.created_at < 0
    ):
        raise ValueError("Health candidate created_at is invalid")
    if (
        isinstance(candidate.excluded_result_count, bool)
        or not isinstance(candidate.excluded_result_count, int)
        or candidate.excluded_result_count < 0
    ):
        raise ValueError("Health candidate excluded_result_count is invalid")
    if (
        candidate.source_snapshot.last_timestamp is not None
        and candidate.source_snapshot.last_timestamp > candidate.created_at
    ):
        raise ValueError("Health candidate creation precedes source completion")
    if not _PAYLOAD_DIGEST_PATTERN.fullmatch(candidate.test_config_digest):
        raise ValueError("Health candidate test_config_digest is invalid")
    if (
        isinstance(candidate.adapter_schema_version, bool)
        or not isinstance(candidate.adapter_schema_version, int)
        or candidate.adapter_schema_version <= 0
        or not candidate.source_snapshot.results
        or any(
            result.adapter_schema_version != candidate.adapter_schema_version
            for result in candidate.source_snapshot.results
        )
    ):
        raise ValueError("Health candidate adapter_schema_version is invalid")
    if (
        isinstance(candidate.robust_z_threshold, bool)
        or not isinstance(candidate.robust_z_threshold, int | float)
        or not math.isfinite(float(candidate.robust_z_threshold))
        or candidate.robust_z_threshold <= 0
    ):
        raise ValueError("Health candidate robust_z_threshold is invalid")
    if not _PAYLOAD_DIGEST_PATTERN.fullmatch(candidate.observation_digest):
        raise ValueError("Health candidate observation_digest is invalid")
    specs_by_source: dict[str, MetricSpec] = {}
    for metric in candidate.metrics:
        spec = MetricSpec(
            name=metric.spec_name,
            source=metric.source,
            direction=metric.direction,
            tolerance_pct=metric.tolerance_pct,
            units=metric.units,
            weight=metric.weight,
        )
        existing_spec = specs_by_source.setdefault(metric.source, spec)
        if existing_spec != spec:
            raise ValueError("Candidate expanded metrics disagree on their source spec")
    observation_values = tuple(
        sorted(
            validate_observations(
                candidate.observations,
                specs_by_source.values(),
                source_snapshot=candidate.source_snapshot,
            ),
            key=_observation_sort_key,
        )
    )
    if candidate.observations != observation_values or any(
        type(observation.value) is not float
        or (
            observation.value == 0.0
            and math.copysign(1.0, observation.value) < 0
        )
        for observation in candidate.observations
    ):
        raise ValueError("Candidate observations must use canonical order")
    if observation_content_digest(observation_values) != candidate.observation_digest:
        raise ValueError("Candidate observation digest does not match exact observations")
    if _coverage_from_observations(observation_values) != candidate.source_coverage:
        raise ValueError("Candidate source coverage does not match exact observations")
    expected_digest, expected_id = _candidate_identity(candidate)
    if (candidate.payload_digest, candidate.baseline_id) != (expected_digest, expected_id):
        raise ValueError("Health candidate content identity does not match its payload")

    metric_ids: set[tuple[str, str]] = set()
    coverage_metrics: set[tuple[str, str]] = set()
    allowed_result_ids = set(candidate.source_snapshot.result_ids)
    for coverage in candidate.source_coverage:
        if not isinstance(coverage, SourceCoverage):
            raise TypeError("Candidate source coverage must contain SourceCoverage values")
        coverage_identity = (coverage.source, coverage.metric_name)
        if (
            not isinstance(coverage.source, str)
            or not coverage.source
            or not isinstance(coverage.metric_name, str)
            or not coverage.metric_name
            or coverage_identity in coverage_metrics
        ):
            raise ValueError("Candidate source coverage identities must be unique")
        if not coverage.results:
            raise ValueError("Candidate source coverage must not be empty")
        if tuple(sorted(set(coverage.result_ids))) != coverage.result_ids:
            raise ValueError("Candidate source coverage result IDs must be sorted and unique")
        if any(
            isinstance(result_id, bool)
            or not isinstance(result_id, int)
            or result_id <= 0
            for result_id in coverage.result_ids
        ):
            raise ValueError("Candidate source coverage result IDs must be positive integers")
        if not set(coverage.result_ids).issubset(allowed_result_ids):
            raise ValueError("Candidate source coverage escapes its source snapshot")
        for result in coverage.results:
            if not isinstance(result, ResultSampleCoverage):
                raise TypeError(
                    "Candidate source coverage must contain ResultSampleCoverage values"
                )
            if not result.sample_keys or tuple(sorted(set(result.sample_keys))) != result.sample_keys:
                raise ValueError(
                    "Candidate source coverage sample keys must be nonempty, sorted, and unique"
                )
            if any(not isinstance(key, str) or not key for key in result.sample_keys):
                raise ValueError("Candidate source coverage sample keys are invalid")
        coverage_metrics.add(coverage_identity)
    for metric in candidate.metrics:
        if not isinstance(metric, MetricBaseline):
            raise TypeError("Candidate metrics must contain MetricBaseline values")
        if any(
            not isinstance(value, str) or not value
            for value in (
                metric.spec_name,
                metric.source,
                metric.metric_name,
                metric.direction,
                metric.statistics_json,
            )
        ) or not isinstance(metric.units, str):
            raise ValueError("Candidate metric string fields are invalid")
        identity = (metric.source, metric.metric_name)
        if identity in metric_ids:
            raise ValueError(f"Duplicate candidate metric identity: {identity!r}")
        metric_ids.add(identity)
        if metric.direction not in _VALID_DIRECTIONS:
            raise ValueError("Candidate metric direction is invalid")
        for value in (
            metric.weight,
            metric.tolerance_pct,
            metric.center,
            metric.mad,
            metric.mad_sigma,
            metric.delta,
            metric.p05,
            metric.p95,
        ):
            if isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError("Candidate metric statistics must be finite")
        if (
            metric.weight <= 0
            or metric.tolerance_pct < 0
            or metric.mad < 0
            or metric.mad_sigma < 0
            or metric.delta < 0
        ):
            raise ValueError("Candidate metric statistics contain an invalid negative value")
        if (
            isinstance(metric.sample_count, bool)
            or not isinstance(metric.sample_count, int)
            or metric.sample_count <= 0
            or isinstance(metric.excluded_count, bool)
            or not isinstance(metric.excluded_count, int)
            or metric.excluded_count < 0
        ):
            raise ValueError("Candidate metric sample counts are invalid")
        try:
            parsed_statistics = json.loads(metric.statistics_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Candidate metric statistics_json is invalid") from exc
        if _canonical_json(parsed_statistics) != metric.statistics_json:
            raise ValueError("Candidate metric statistics_json must be canonical")
        _validate_metric_statistics_payload(
            metric,
            parsed_statistics,
            candidate.robust_z_threshold,
        )
        if metric.thresholds != _generate_thresholds(metric):
            raise ValueError("Candidate thresholds do not match the versioned band formula")
        for band in metric.thresholds:
            if not isinstance(band, ThresholdBand):
                raise TypeError("Candidate thresholds must contain ThresholdBand values")
            if not isinstance(band.class_code, HealthClassCode):
                raise ValueError("Candidate threshold class code is invalid")
            if (
                isinstance(band.band_index, bool)
                or not isinstance(band.band_index, int)
                or band.band_index < 0
                or not isinstance(band.lower_inclusive, bool)
                or not isinstance(band.upper_inclusive, bool)
            ):
                raise ValueError("Candidate threshold metadata types are invalid")
            for bound in (band.lower_bound, band.upper_bound):
                if bound is not None and (
                    isinstance(bound, bool)
                    or not isinstance(bound, int | float)
                    or not math.isfinite(float(bound))
                ):
                    raise ValueError("Candidate threshold bounds must be finite")
        _assert_threshold_partition(metric)
    if candidate.metrics != tuple(
        sorted(candidate.metrics, key=lambda metric: (metric.source, metric.metric_name))
    ):
        raise ValueError("Candidate metrics must use canonical source/name order")
    if candidate.source_coverage != tuple(
        sorted(
            candidate.source_coverage,
            key=lambda coverage: (coverage.source, coverage.metric_name),
        )
    ):
        raise ValueError("Candidate source coverage must use canonical order")
    _validate_candidate_metric_derivation(candidate, observation_values)


def validate_stored_baseline(
    stored: StoredHealthBaseline,
    definition: ValidationTestDefinition | None = None,
    *,
    robust_z_threshold: float | None = None,
) -> QualityReport:
    """Validate lifecycle metadata and optionally bind an active baseline to config."""

    if not isinstance(stored, StoredHealthBaseline):
        raise TypeError("Expected StoredHealthBaseline")
    assert_candidate_integrity(stored.candidate)
    if not isinstance(stored.lifecycle, BaselineLifecycle):
        raise ValueError("Stored health baseline lifecycle is invalid")
    for field_name, value in (
        ("updated_at", stored.updated_at),
        ("activated_at", stored.activated_at),
        ("superseded_at", stored.superseded_at),
    ):
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise ValueError(f"Stored health baseline {field_name} is invalid")
    created = stored.candidate.created_at
    if stored.updated_at < created:
        raise ValueError("Stored health baseline update precedes candidate creation")
    lifecycle_valid = (
        stored.lifecycle is BaselineLifecycle.CANDIDATE
        and stored.activated_at is None
        and stored.superseded_at is None
    ) or (
        stored.lifecycle is BaselineLifecycle.ACTIVE
        and stored.activated_at is not None
        and stored.superseded_at is None
        and created <= stored.activated_at <= stored.updated_at
    ) or (
        stored.lifecycle is BaselineLifecycle.SUPERSEDED
        and stored.activated_at is not None
        and stored.superseded_at is not None
        and created
        <= stored.activated_at
        <= stored.superseded_at
        <= stored.updated_at
    )
    if not lifecycle_valid:
        raise ValueError("Stored health baseline lifecycle timestamps are inconsistent")
    if not isinstance(stored.quality, QualityReport):
        raise TypeError("Stored health baseline quality report is invalid")
    if definition is None:
        return stored.quality
    try:
        current = validate_candidate(
            stored.candidate,
            definition,
            robust_z_threshold=robust_z_threshold,
        )
    except ValueError as exc:
        raise ValueError(
            f"Stored health baseline quality/config binding is stale: {exc}"
        ) from exc
    if current != stored.quality:
        raise ValueError("Stored health baseline quality/config binding is stale")
    if stored.lifecycle is BaselineLifecycle.ACTIVE and not current.activation_ready:
        raise ValueError("Active health baseline is not activation-ready")
    return current


def validate_candidate(
    candidate: HealthCandidate,
    definition: ValidationTestDefinition,
    *,
    robust_z_threshold: float | None = None,
) -> QualityReport:
    """Recompute mandatory activation quality gates for one candidate."""

    assert_candidate_integrity(candidate)
    health = definition.health
    if health is None or not health.enabled:
        raise ValueError("Health candidate definition is not health-enabled")
    validate_combination_for_definition(candidate.combination, definition)
    validate_source_snapshot_context(
        candidate.source_snapshot,
        definition,
        candidate.combination,
        require_nonempty=True,
    )
    effective_robust_z = (
        health.robust_z_threshold
        if health.robust_z_threshold is not None
        else robust_z_threshold
    )
    if (
        effective_robust_z is None
        or isinstance(effective_robust_z, bool)
        or not isinstance(effective_robust_z, int | float)
        or not math.isfinite(float(effective_robust_z))
        or float(effective_robust_z) <= 0
    ):
        raise ValueError(
            "Current effective robust_z_threshold must be finite and positive"
        )
    configured_sources = {metric.source for metric in health.metrics}
    observed_sources = {metric.source for metric in candidate.metrics}
    spec_by_source = {
        metric.source: metric for metric in health.metrics
    }
    coverage = {
        (item.source, item.metric_name): set(item.result_ids)
        for item in candidate.source_coverage
    }
    coverage_records = {
        (item.source, item.metric_name): item
        for item in candidate.source_coverage
    }
    expected_result_ids = set(candidate.source_snapshot.result_ids)
    candidate_specs = {
        MetricSpec(
            name=metric.spec_name,
            source=metric.source,
            direction=metric.direction,
            tolerance_pct=metric.tolerance_pct,
            units=metric.units,
            weight=metric.weight,
        )
        for metric in candidate.metrics
    }
    if candidate_specs != set(metric_specs_from_definition(definition)):
        raise ValueError("Health candidate metric specs do not match the test descriptor")
    gates = (
        QualityGate(
            "test_identity",
            candidate.test_id == definition.metadata.id,
            "candidate test ID matches the active definition",
        ),
        QualityGate(
            "config_digest",
            candidate.test_config_digest == validation_test_config_digest(definition),
            "candidate config digest matches the active definition",
        ),
        QualityGate(
            "health_policy_version",
            candidate.health_policy_version == health.policy_version,
            "candidate health policy version matches the active definition",
        ),
        QualityGate(
            "adapter_schema_version",
            all(
                result.adapter_schema_version == candidate.adapter_schema_version
                for result in candidate.source_snapshot.results
            ),
            "candidate sources use one content-bound adapter schema version",
        ),
        QualityGate(
            "robust_z_policy",
            candidate.robust_z_threshold == float(effective_robust_z),
            "candidate robust-z policy matches the current effective config",
        ),
        QualityGate(
            "source_results",
            len(candidate.source_snapshot.results) >= health.min_samples,
            f"at least {health.min_samples} distinct source results are required",
        ),
        QualityGate(
            "metric_spec_identity",
            bool(candidate.metrics)
            and all(
                metric.source in spec_by_source
                and metric.spec_name == spec_by_source[metric.source].name
                and metric.direction == spec_by_source[metric.source].direction
                and metric.units == spec_by_source[metric.source].units
                and metric.weight == spec_by_source[metric.source].weight
                and metric.tolerance_pct
                == spec_by_source[metric.source].tolerance_pct
                for metric in candidate.metrics
            ),
            "every candidate metric must match its configured source rule",
        ),
        QualityGate(
            "metric_source_coverage",
            bool(candidate.metrics)
            and observed_sources == configured_sources
            and set(coverage)
            == {(metric.source, metric.metric_name) for metric in candidate.metrics}
            and all(
                coverage[(metric.source, metric.metric_name)] == expected_result_ids
                for metric in candidate.metrics
            ),
            "every configured metric source must produce candidate metrics",
        ),
        QualityGate(
            "metric_sample_coverage",
            bool(candidate.metrics)
            and all(
                (metric.source, metric.metric_name) in coverage_records
                and coverage_records[
                    (metric.source, metric.metric_name)
                ].expected_sample_keys is not None
                for metric in candidate.metrics
            ),
            "every expanded metric must have exact stable sample keys per result",
        ),
        QualityGate(
            "metric_samples",
            bool(candidate.metrics)
            and all(metric.sample_count >= health.min_samples for metric in candidate.metrics),
            f"every expanded metric requires at least {health.min_samples} clean samples",
        ),
        QualityGate(
            "normalized_thresholds",
            bool(candidate.metrics),
            "all candidate metrics have exact exhaustive normalized threshold bands",
        ),
        QualityGate(
            "lifecycle_parent",
            candidate.parent_baseline_id != candidate.baseline_id,
            "candidate cannot name itself as lifecycle parent",
        ),
    )
    return QualityReport(gates)


def _classify_declarative(
    baseline: StoredHealthBaseline | None,
    observations: Iterable[MetricObservation],
    *,
    raw_status: str,
    combination: EnvironmentCombination | None,
    test_id: str,
    definition: ValidationTestDefinition,
    source_snapshot: SourceSnapshot,
    robust_z_threshold: float | None = None,
) -> HealthVerdict:
    """Classify observations against exact normalized bands, returning DNR when absent."""

    dnr = _classification_identity_preflight(
        baseline,
        raw_status=raw_status,
        combination=combination,
        test_id=test_id,
        definition=definition,
        robust_z_threshold=robust_z_threshold,
    )
    if dnr is not None:
        return dnr
    raw_values = tuple(observations)
    if not raw_values:
        assert baseline is not None and combination is not None
        return _dnr(test_id, combination, baseline.candidate, DnrReason.NO_OBSERVATIONS)
    validate_source_snapshot_context(
        source_snapshot,
        definition,
        combination,
        require_nonempty=True,
    )
    specs = metric_specs_from_definition(definition)
    validate_observations(
        raw_values,
        specs,
        source_snapshot=source_snapshot,
        require_exact_result_ids=True,
    )
    assert baseline is not None and combination is not None
    candidate = baseline.candidate
    if candidate.test_id != test_id:
        raise ValueError("Health baseline test identity does not match classification")
    current_adapter_schema_version = source_snapshot.results[0].adapter_schema_version
    if current_adapter_schema_version != candidate.adapter_schema_version:
        return _dnr(
            test_id,
            combination,
            candidate,
            DnrReason.INCOMPATIBLE_ADAPTER_VERSION,
        )

    return _classify_candidate(
        candidate,
        raw_values,
        combination,
        test_id,
        expected_result_ids=set(source_snapshot.result_ids),
    )


def _classify_candidate(
    baseline: HealthCandidate,
    observations: Iterable[MetricObservation],
    combination: EnvironmentCombination,
    test_id: str,
    *,
    expected_result_ids: set[int],
) -> HealthVerdict:
    """Evaluate one already-proven active candidate."""

    spec_map = {
        metric.source: MetricSpec(
            name=metric.spec_name,
            source=metric.source,
            direction=metric.direction,
            tolerance_pct=0.0,
            units=metric.units,
            weight=metric.weight,
        )
        for metric in baseline.metrics
    }
    observation_values = validate_observations(observations, spec_map.values())
    if not observation_values:
        return _dnr(test_id, combination, baseline, DnrReason.NO_OBSERVATIONS)
    grouped: dict[tuple[str, str], list[MetricObservation]] = {}
    for observation in observation_values:
        grouped.setdefault((observation.source, observation.metric_name), []).append(
            observation
        )
    expected = {(metric.source, metric.metric_name) for metric in baseline.metrics}
    if expected != set(grouped):
        return _dnr(
            test_id,
            combination,
            baseline,
            DnrReason.INCOMPLETE_METRIC_COVERAGE,
        )
    current_coverage = {
        (coverage.source, coverage.metric_name): coverage
        for coverage in _coverage_from_observations(observation_values)
    }
    baseline_coverage = {
        (coverage.source, coverage.metric_name): coverage
        for coverage in baseline.source_coverage
    }
    if any(
        set(current_coverage[identity].result_ids) != expected_result_ids
        or baseline_coverage[identity].expected_sample_keys is None
        or any(
            result.sample_keys != baseline_coverage[identity].expected_sample_keys
            for result in current_coverage[identity].results
        )
        for identity in expected
    ):
        return _dnr(
            test_id,
            combination,
            baseline,
            DnrReason.INCOMPLETE_METRIC_COVERAGE,
        )

    metric_verdicts: list[MetricVerdict] = []
    for metric in baseline.metrics:
        raw_values = [
            float(observation.value)
            for observation in grouped[(metric.source, metric.metric_name)]
        ]
        value = (
            stats.median([abs(raw_value) for raw_value in raw_values])
            if metric.direction == "absolute"
            else stats.median(raw_values)
        )
        class_code = _classify_threshold(value, metric.thresholds)
        pct_diff = ((value - metric.center) / metric.center * 100.0) if metric.center else 0.0
        if metric.center:
            severity_pct = abs(pct_diff)
        elif value == metric.center:
            severity_pct = 0.0
        elif metric.delta > 0:
            severity_pct = abs(value - metric.center) / metric.delta * 100.0
        else:
            severity_pct = 100.0
        metric_verdicts.append(
            MetricVerdict(
                source=metric.source,
                metric_name=metric.metric_name,
                value=value,
                class_code=class_code,
                class_name=class_code.class_name,
                pct_diff=pct_diff,
                severity_pct=severity_pct,
            )
        )
    aggregate = max(
        (verdict.class_code for verdict in metric_verdicts),
        default=HealthClassCode.DNR,
    )
    details = {
        "aggregation": DECLARATIVE_AGGREGATION_POLICY,
        "n_metrics": len(metric_verdicts),
        "class_counts": {
            str(int(code)): sum(
                1 for verdict in metric_verdicts if verdict.class_code == code
            )
            for code in HealthClassCode
            if code is not HealthClassCode.DNR
        },
    }
    verdict = HealthVerdict(
        test_id=test_id,
        combination_key=combination.key,
        baseline_id=baseline.baseline_id,
        class_code=aggregate,
        class_name=aggregate.class_name,
        dnr_reason=None,
        metrics=tuple(metric_verdicts),
        details_json=_canonical_json(details),
    )
    validate_health_verdict(
        verdict,
        test_id=test_id,
        combination=combination,
        baseline=baseline,
    )
    return verdict


def _classification_identity_preflight(
    baseline: StoredHealthBaseline | None,
    *,
    raw_status: str,
    combination: EnvironmentCombination | None,
    test_id: str,
    definition: ValidationTestDefinition,
    robust_z_threshold: float | None,
) -> HealthVerdict | None:
    if raw_status not in _VALID_RAW_STATUSES:
        raise ValueError(f"Unsupported raw status: {raw_status!r}")
    if test_id != definition.metadata.id:
        raise ValueError("Health classification test identity does not match definition")
    if baseline is not None and not isinstance(baseline, StoredHealthBaseline):
        raise TypeError("Health classification requires a stored baseline")
    if combination is not None:
        validate_combination_for_definition(combination, definition)
    if raw_status == "fail":
        return _dnr(test_id, combination, None, DnrReason.RAW_FAILED)
    if raw_status == "incomplete":
        return _dnr(test_id, combination, None, DnrReason.RAW_INCOMPLETE)
    if combination is None:
        return _dnr(test_id, None, None, DnrReason.MISSING_COMBINATION)
    if baseline is None:
        return _dnr(test_id, combination, None, DnrReason.NO_ACTIVE_BASELINE)
    validate_stored_baseline(
        baseline,
        definition,
        robust_z_threshold=robust_z_threshold,
    )
    if (
        baseline.lifecycle is not BaselineLifecycle.ACTIVE
        or not baseline.quality.activation_ready
        or baseline.candidate.combination.key != combination.key
    ):
        return _dnr(test_id, combination, None, DnrReason.NO_ACTIVE_BASELINE)
    return None


def validate_health_verdict(
    verdict: HealthVerdict,
    *,
    test_id: str,
    combination: EnvironmentCombination | None,
    baseline: HealthCandidate | None,
) -> None:
    """Validate declarative or custom adapter verdict identity and class semantics."""

    if not isinstance(verdict, HealthVerdict):
        raise TypeError("Health classify hook must return HealthVerdict")
    if verdict.test_id != test_id:
        raise ValueError("Health verdict test identity mismatch")
    if not isinstance(verdict.class_code, HealthClassCode):
        raise ValueError("Health verdict class code is invalid")
    code = verdict.class_code
    if verdict.class_name != code.class_name:
        raise ValueError("Health verdict class name does not match its stable code")
    expected_key = combination.key if combination is not None else ""
    if verdict.combination_key != expected_key:
        raise ValueError("Health verdict combination identity mismatch")
    expected_baseline = baseline.baseline_id if baseline is not None else None
    if verdict.baseline_id != expected_baseline:
        raise ValueError("Health verdict baseline identity mismatch")
    if code is HealthClassCode.DNR:
        if not isinstance(verdict.dnr_reason, DnrReason) or verdict.metrics:
            raise ValueError("DNR verdict requires a reason and no metric verdicts")
    elif verdict.dnr_reason is not None or not verdict.metrics:
        raise ValueError("Evaluated health verdict requires metrics and no DNR reason")
    for metric in verdict.metrics:
        if not isinstance(metric.class_code, HealthClassCode):
            raise ValueError("Metric verdict class code is invalid")
        metric_code = metric.class_code
        if metric_code is HealthClassCode.DNR:
            raise ValueError("Metric verdicts cannot use the DNR class")
        if metric.class_name != metric_code.class_name:
            raise ValueError("Metric verdict class name mismatch")
        if (
            not math.isfinite(metric.value)
            or not math.isfinite(metric.pct_diff)
            or not math.isfinite(metric.severity_pct)
            or metric.severity_pct < 0
        ):
            raise ValueError("Metric verdict values must be finite")
    try:
        details = json.loads(verdict.details_json)
    except json.JSONDecodeError as exc:
        raise ValueError("Health verdict details_json is invalid") from exc
    if not isinstance(details, dict) or _canonical_json(details) != verdict.details_json:
        raise ValueError("Health verdict details_json must be a canonical object")
    if code is not HealthClassCode.DNR and (
        not isinstance(details.get("aggregation"), str)
        or not _VERSIONED_POLICY_PATTERN.fullmatch(details["aggregation"])
    ):
        raise ValueError("Evaluated health verdict requires a versioned aggregation policy")


def _generate_thresholds(metric: MetricBaseline) -> tuple[ThresholdBand, ...]:
    center = metric.center
    delta = metric.delta
    if metric.direction == "low_bad":
        good = max(metric.p95, center - delta)
        bands = [
            _band(0, 0, good, None, False, False),
            _band(1, 0, center - delta, good, True, True),
        ]
        if delta > 0:
            bands.extend(
                (
                    _band(2, 0, center - 2 * delta, center - delta, True, False),
                    _band(3, 0, center - 3 * delta, center - 2 * delta, True, False),
                )
            )
        bands.append(_band(4, 0, None, center - 3 * delta, False, False))
    elif metric.direction in {"high_bad", "absolute"}:
        good = min(metric.p05, center + delta)
        bands = [
            _band(0, 0, None, good, False, False),
            _band(1, 0, good, center + delta, True, True),
        ]
        if delta > 0:
            bands.extend(
                (
                    _band(2, 0, center + delta, center + 2 * delta, False, True),
                    _band(3, 0, center + 2 * delta, center + 3 * delta, False, True),
                )
            )
        bands.append(_band(4, 0, center + 3 * delta, None, False, False))
    elif metric.direction == "two_sided":
        bands = [_band(1, 0, center - delta, center + delta, True, True)]
        if delta > 0:
            bands.extend(
                (
                    _band(2, 0, center - 2 * delta, center - delta, True, False),
                    _band(2, 1, center + delta, center + 2 * delta, False, True),
                    _band(3, 0, center - 3 * delta, center - 2 * delta, True, False),
                    _band(3, 1, center + 2 * delta, center + 3 * delta, False, True),
                )
            )
        bands.extend(
            (
                _band(4, 0, None, center - 3 * delta, False, False),
                _band(4, 1, center + 3 * delta, None, False, False),
            )
        )
    else:  # pragma: no cover - guarded by candidate validation
        raise ValueError(f"Unsupported threshold direction: {metric.direction!r}")
    return tuple(sorted(bands, key=lambda band: (int(band.class_code), band.band_index)))


def _band(
    code: int,
    index: int,
    lower: float | None,
    upper: float | None,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> ThresholdBand:
    return ThresholdBand(
        class_code=HealthClassCode(code),
        band_index=index,
        lower_bound=lower,
        upper_bound=upper,
        lower_inclusive=lower_inclusive,
        upper_inclusive=upper_inclusive,
    )


def _assert_threshold_partition(metric: MetricBaseline) -> None:
    boundaries = sorted(
        {
            value
            for band in metric.thresholds
            for value in (band.lower_bound, band.upper_bound)
            if value is not None
        }
    )
    points: list[float] = list(boundaries)
    if boundaries:
        extent = max(1.0, max(abs(value) for value in boundaries))
        points.extend((boundaries[0] - extent, boundaries[-1] + extent))
        points.extend(
            (left + right) / 2.0
            for left, right in zip(boundaries, boundaries[1:])
            if left < right
        )
    else:
        points.append(0.0)
    for point in points:
        matches = [band for band in metric.thresholds if _band_contains(band, point)]
        if len(matches) != 1:
            raise ValueError(
                f"Health threshold bands are not exhaustive/disjoint at {point}: "
                f"{len(matches)} matches"
            )
    if any(band.class_code is HealthClassCode.DNR for band in metric.thresholds):
        raise ValueError("DNR must not have a threshold band")


def _classify_threshold(
    value: float,
    thresholds: tuple[ThresholdBand, ...],
) -> HealthClassCode:
    matches = [band.class_code for band in thresholds if _band_contains(band, value)]
    if len(matches) != 1:
        raise RuntimeError("Active health thresholds are incomplete or overlapping")
    return matches[0]


def _band_contains(band: ThresholdBand, value: float) -> bool:
    if band.lower_bound is not None:
        if value < band.lower_bound or (
            value == band.lower_bound and not band.lower_inclusive
        ):
            return False
    if band.upper_bound is not None:
        if value > band.upper_bound or (
            value == band.upper_bound and not band.upper_inclusive
        ):
            return False
    return True


def _summary_delta(summary: stats.MetricStat, direction: str) -> float:
    if direction == stats.DIRECTION_LOW_BAD:
        return max(0.0, summary.median - summary.lower_bound)
    return max(0.0, summary.upper_bound - summary.median)


def _dnr(
    test_id: str,
    combination: EnvironmentCombination | None,
    baseline: HealthCandidate | None,
    reason: DnrReason,
) -> HealthVerdict:
    verdict = HealthVerdict(
        test_id=test_id,
        combination_key=combination.key if combination is not None else "",
        baseline_id=baseline.baseline_id if baseline is not None else None,
        class_code=HealthClassCode.DNR,
        class_name=HealthClassCode.DNR.class_name,
        dnr_reason=reason,
        metrics=(),
        details_json=_canonical_json({"dnr_reason": reason.value}),
    )
    validate_health_verdict(
        verdict,
        test_id=test_id,
        combination=combination,
        baseline=baseline,
    )
    return verdict


def _strict_result_id_set(values: Iterable[int], label: str) -> set[int]:
    result: set[int] = set()
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} result IDs must be positive integers")
        if value in result:
            raise ValueError(f"{label} result IDs must be unique")
        result.add(value)
    return result


def _coverage_from_observations(
    observations: Iterable[MetricObservation],
) -> tuple[SourceCoverage, ...]:
    grouped: dict[tuple[str, str], dict[int, set[str]]] = {}
    for observation in observations:
        grouped.setdefault(
            (observation.source, observation.metric_name),
            {},
        ).setdefault(observation.result_id, set()).add(observation.sample_key)
    return tuple(
        SourceCoverage(
            source,
            metric_name,
            tuple(
                ResultSampleCoverage(result_id, tuple(sorted(sample_keys)))
                for result_id, sample_keys in sorted(results.items())
            ),
        )
        for (source, metric_name), results in sorted(grouped.items())
    )


def _observation_sort_key(
    observation: MetricObservation,
) -> tuple[int, str, str, str]:
    return (
        observation.result_id,
        observation.source,
        observation.metric_name,
        observation.sample_key,
    )


def observation_content_digest(
    observations: Iterable[MetricObservation],
) -> str:
    """Return a deterministic digest over exact validated observation content."""

    payload = [
        {
            "result_id": observation.result_id,
            "run_id": observation.run_id,
            "completed_timestamp": observation.completed_timestamp,
            "source": observation.source,
            "metric_name": observation.metric_name,
            "sample_key": observation.sample_key,
            "value": float(observation.value),
        }
        for observation in sorted(
            tuple(observations),
            key=lambda item: (
                item.result_id,
                item.source,
                item.metric_name,
                item.sample_key,
            ),
        )
    ]
    encoded = _canonical_json(payload).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _validate_candidate_metric_derivation(
    candidate: HealthCandidate,
    observations: tuple[MetricObservation, ...],
) -> None:
    """Reconstruct all metric evidence from exact immutable observations."""

    grouped: dict[tuple[str, str], list[float]] = {}
    for observation in observations:
        grouped.setdefault(
            (observation.source, observation.metric_name),
            [],
        ).append(float(observation.value))
    metric_map = {
        (metric.source, metric.metric_name): metric
        for metric in candidate.metrics
    }
    if set(grouped) != set(metric_map):
        raise ValueError("Candidate metrics do not exactly match observations")
    for identity, raw_values in sorted(grouped.items()):
        metric = metric_map[identity]
        summarized_values = (
            [abs(value) for value in raw_values]
            if metric.direction == "absolute"
            else raw_values
        )
        kernel_direction = (
            stats.DIRECTION_HIGH_BAD
            if metric.direction == "absolute"
            else metric.direction
        )
        summary = stats.summarize_metric(
            metric.metric_name,
            summarized_values,
            direction=kernel_direction,
            tolerance_pct=metric.tolerance_pct,
            z_threshold=candidate.robust_z_threshold,
        )
        statistic_payload = summary.to_dict()
        statistic_payload["configured_direction"] = metric.direction
        statistic_payload["tolerance_pct"] = metric.tolerance_pct
        statistic_payload["robust_z_threshold"] = candidate.robust_z_threshold
        expected = MetricBaseline(
            spec_name=metric.spec_name,
            source=metric.source,
            metric_name=metric.metric_name,
            direction=metric.direction,
            units=metric.units,
            weight=metric.weight,
            tolerance_pct=metric.tolerance_pct,
            center=summary.median,
            mad=summary.mad,
            mad_sigma=summary.mad_sigma,
            delta=_summary_delta(summary, kernel_direction),
            p05=summary.p05,
            p95=summary.p95,
            sample_count=summary.n,
            excluded_count=summary.n_excluded,
            statistics_json=_canonical_json(statistic_payload),
            thresholds=(),
        )
        expected = MetricBaseline(
            **{
                **asdict(expected),
                "thresholds": _generate_thresholds(expected),
            }
        )
        if metric != expected:
            raise ValueError(
                "Candidate metric statistics/thresholds do not derive from observations"
            )


def _validate_metric_statistics_payload(
    metric: MetricBaseline,
    payload: Any,
    robust_z_threshold: float,
) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Candidate metric statistics_json must contain an object")
    expected_keys = {
        "metric",
        "direction",
        "n",
        "n_excluded",
        "median",
        "mad",
        "mad_sigma",
        "iqr",
        "p01",
        "p05",
        "p25",
        "p50",
        "p75",
        "p95",
        "p99",
        "minimum",
        "maximum",
        "skewness",
        "kurtosis",
        "ci_low",
        "ci_high",
        "deterministic",
        "lower_bound",
        "upper_bound",
        "method",
        "configured_direction",
        "tolerance_pct",
        "robust_z_threshold",
    }
    if set(payload) != expected_keys:
        raise ValueError("Candidate metric statistics_json field manifest is invalid")
    kernel_direction = (
        stats.DIRECTION_HIGH_BAD
        if metric.direction == "absolute"
        else metric.direction
    )
    expected_normalized = {
        "metric": metric.metric_name,
        "direction": kernel_direction,
        "n": metric.sample_count,
        "n_excluded": metric.excluded_count,
        "median": metric.center,
        "mad": metric.mad,
        "mad_sigma": metric.mad_sigma,
        "p05": metric.p05,
        "p50": metric.center,
        "p95": metric.p95,
        "configured_direction": metric.direction,
        "tolerance_pct": metric.tolerance_pct,
        "robust_z_threshold": robust_z_threshold,
    }
    for key, expected in expected_normalized.items():
        if type(payload.get(key)) is not type(expected) or payload.get(key) != expected:
            raise ValueError(
                f"Candidate metric statistics_json {key!r} does not match normalized data"
            )
    for key in (
        "iqr",
        "p01",
        "p25",
        "p75",
        "p99",
        "minimum",
        "maximum",
        "skewness",
        "kurtosis",
        "ci_low",
        "ci_high",
    ):
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(
            float(value)
        ):
            raise ValueError(f"Candidate metric statistics_json {key!r} must be finite")
    if not isinstance(payload["deterministic"], bool) or payload["method"] not in {
        "deterministic",
        "robust_mad",
    }:
        raise ValueError("Candidate metric statistics_json method metadata is invalid")
    ordered = [
        float(payload[key])
        for key in (
            "minimum",
            "p01",
            "p05",
            "p25",
            "p50",
            "p75",
            "p95",
            "p99",
            "maximum",
        )
    ]
    if any(
        left > right and left - right > _floating_order_tolerance(left, right)
        for left, right in zip(ordered, ordered[1:])
    ):
        raise ValueError("Candidate metric statistics_json percentiles are unordered")
    if not math.isclose(
        float(payload["iqr"]),
        float(payload["p75"]) - float(payload["p25"]),
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError("Candidate metric statistics_json IQR is inconsistent")
    if not (
        float(payload["ci_low"])
        <= metric.center
        <= float(payload["ci_high"])
    ):
        raise ValueError("Candidate metric statistics_json median CI is inconsistent")
    deterministic = metric.mad == 0.0
    if payload["deterministic"] is not deterministic or payload["method"] != (
        "deterministic" if deterministic else "robust_mad"
    ):
        raise ValueError("Candidate metric deterministic/method metadata is inconsistent")
    if not math.isclose(
        metric.mad_sigma,
        1.4826 * metric.mad,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError("Candidate metric MAD-sigma is inconsistent")
    expected_lower = None
    expected_upper = None
    if kernel_direction == stats.DIRECTION_LOW_BAD:
        expected_lower = metric.center - metric.delta
    elif kernel_direction == stats.DIRECTION_HIGH_BAD:
        expected_upper = metric.center + metric.delta
    else:
        expected_lower = metric.center - metric.delta
        expected_upper = metric.center + metric.delta
    if not _same_optional_float(payload["lower_bound"], expected_lower) or not _same_optional_float(
        payload["upper_bound"], expected_upper
    ):
        raise ValueError("Candidate metric statistics_json bounds do not match delta")


def _same_optional_float(left: Any, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or not isinstance(left, int | float):
        return False
    return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)


def _floating_order_tolerance(left: float, right: float) -> float:
    """Permit only a tiny bounded interpolation/ULP inversion."""

    ulp = max(math.ulp(left), math.ulp(right))
    return min(1e-12, 8.0 * ulp)


def _validate_custom_aggregation(verdict: HealthVerdict) -> None:
    details = json.loads(verdict.details_json)
    policy = details.get("aggregation") if isinstance(details, dict) else None
    if not isinstance(policy, str) or not re.fullmatch(
        r"[a-z][a-z0-9_.-]*\.v[1-9][0-9]*",
        policy,
    ):
        raise ValueError("Custom health verdict requires a versioned aggregation policy")
    degraded_codes = {
        HealthClassCode.UNDERPERFORMING,
        HealthClassCode.VERY_BAD,
        HealthClassCode.TERRIBLE,
    }
    metric_codes = {metric.class_code for metric in verdict.metrics}
    if verdict.class_code is HealthClassCode.EXCELLENT and metric_codes & degraded_codes:
        raise ValueError("Custom aggregation cannot be Excellent with degraded metrics")
    if verdict.class_code in degraded_codes and verdict.class_code not in metric_codes:
        raise ValueError("Custom aggregate degradation class is absent from metric verdicts")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
