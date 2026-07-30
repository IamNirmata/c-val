from __future__ import annotations

import unittest
import json
from dataclasses import replace
from pathlib import Path

import cval.health.engine as health_engine
from cval.baselines import stats
from cval.health.combination import canonicalize_factors
from cval.health.engine import (
    _build_declarative_candidate,
    _build_candidate_with_plugin_observations as build_candidate_with_plugin,
    build_candidate_from_plugin,
    _candidate_identity,
    _classify_declarative,
    _classify_with_plugin_observations as classify_with_plugin,
    classify_from_plugin,
    evaluate_build_trigger,
    metric_specs_from_definition,
    validate_candidate,
    validate_health_verdict,
    validate_observations,
)
from cval.health.models import (
    BaselineLifecycle,
    DnrReason,
    HealthClassCode,
    MetricObservation,
    HealthContext,
    HealthVerdict,
    SourceResult,
    SourceSnapshot,
    StoredHealthBaseline,
)
from cval.validation.registry import (
    HealthMetric,
    TestArtifacts,
    TestHealth,
    TestMetadata,
    TestPlugin,
    TestRequirements,
    ValidationTestDefinition,
    validation_test_config_digest,
)


def definition(
    direction: str = "low_bad",
    *,
    min_samples: int = 3,
    metrics: tuple[HealthMetric, ...] | None = None,
    strategy: str = "declarative",
) -> ValidationTestDefinition:
    configured = metrics or (
        HealthMetric(
            name="metric-rule",
            source="source-a",
            direction=direction,
            tolerance_pct=10.0,
            units="units",
        ),
    )
    return ValidationTestDefinition(
        schema_version="cval.test.v1",
        metadata=TestMetadata(
            id="smoke",
            display_name="Smoke",
            description="",
            order=1,
            entrypoint="run-test.sh",
            setup="setup.sh",
            timeout_seconds=30,
        ),
        requirements=TestRequirements(),
        artifacts=TestArtifacts(
            "validation_tests/smoke/smoke_results.db",
            "validation_tests/smoke/smoke_health_classes.db",
        ),
        settings={},
        plugin=TestPlugin("plugin.py", "cval.plugin.v1", ("health",)),
        health=TestHealth(
            enabled=True,
            policy_version="smoke.health.v1",
            strategy=strategy,
            min_samples=min_samples,
            min_new_results=2,
            target_class_count=5,
            combination_factors=("image_name",),
            auto_activate=False,
            robust_z_threshold=3.5,
            metrics=configured,
        ),
    )


def snapshot(
    count: int,
    test_definition: ValidationTestDefinition | None = None,
    combination=None,
) -> SourceSnapshot:
    active_definition = test_definition or definition()
    active_combination = combination or canonicalize_factors({"image_name": "img"})
    return SourceSnapshot(
        tuple(
            SourceResult(
                result_id=index,
                run_id=f"run-{index}",
                completed_timestamp=100 + index,
                result_digest="sha256:" + f"{index:064x}",
                raw_result_digest="sha256:" + f"{index + 100:064x}",
                test_config_digest=validation_test_config_digest(active_definition),
                combination_key=active_combination.key,
                adapter_schema_version=1,
                receipt_evidence_digest="sha256:" + f"{index + 200:064x}",
            )
            for index in range(1, count + 1)
        )
    )


def observations(
    values: list[float],
    *,
    source: str = "source-a",
    metric_name: str = "expanded-metric",
) -> tuple[MetricObservation, ...]:
    return tuple(
        MetricObservation(
            result_id=index,
            run_id=f"run-{index}",
            completed_timestamp=100 + index,
            source=source,
            metric_name=metric_name,
            sample_key="sample",
            value=value,
        )
        for index, value in enumerate(values, start=1)
    )


def active(candidate, test_definition) -> StoredHealthBaseline:
    activated_at = candidate.created_at + 1
    return StoredHealthBaseline(
        candidate=candidate,
        lifecycle=BaselineLifecycle.ACTIVE,
        quality=validate_candidate(candidate, test_definition),
        updated_at=activated_at,
        activated_at=activated_at,
        superseded_at=None,
    )


class HealthBuildTriggerTests(unittest.TestCase):
    def test_requires_total_and_set_difference_thresholds(self) -> None:
        blocked = evaluate_build_trigger(
            [1, 2, 3],
            [1, 2],
            min_samples=3,
            min_new_results=2,
        )
        ready = evaluate_build_trigger(
            [1, 2, 3, 4],
            [1, 2],
            min_samples=3,
            min_new_results=2,
        )

        self.assertFalse(blocked.eligible)
        self.assertEqual(blocked.reasons, ("insufficient_new_results",))
        self.assertTrue(ready.eligible)
        self.assertEqual(ready.new_result_count, 2)

    def test_late_older_result_counts_as_new(self) -> None:
        decision = evaluate_build_trigger(
            [1, 2, 3, 10],
            [2, 3, 10],
            min_samples=4,
            min_new_results=1,
        )
        self.assertTrue(decision.eligible)
        self.assertEqual(decision.new_result_count, 1)

    def test_rejects_duplicate_or_boolean_result_ids(self) -> None:
        for ids in ([1, 1], [True]):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                evaluate_build_trigger(ids, [], min_samples=1, min_new_results=1)

    def test_rejects_fractional_trigger_counts(self) -> None:
        with self.assertRaisesRegex(ValueError, "min_samples"):
            evaluate_build_trigger([1], [], min_samples=1.5, min_new_results=1)


class HealthCandidateTests(unittest.TestCase):
    def test_reuses_existing_robust_median_mad_kernel(self) -> None:
        values = [100.0, 101.0, 99.0, 100.0, 1000.0]
        test_definition = definition()
        candidate = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            observations(values),
            snapshot(len(values)),
            created_at=1000,
        )
        expected = stats.summarize_metric(
            "expanded-metric",
            values,
            direction="low_bad",
            tolerance_pct=10.0,
        )

        metric = candidate.metrics[0]
        self.assertEqual(metric.center, expected.median)
        self.assertEqual(metric.mad, expected.mad)
        self.assertEqual(metric.mad_sigma, expected.mad_sigma)
        self.assertTrue(validate_candidate(candidate, test_definition).activation_ready)

    def test_candidate_identity_excludes_wall_clock_but_includes_sources(self) -> None:
        test_definition = definition()
        combination = canonicalize_factors({"image_name": "img"})
        args = (
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            observations([100.0, 100.0, 100.0]),
            snapshot(3, test_definition, combination),
        )
        first = _build_declarative_candidate(*args, created_at=200)
        second = _build_declarative_candidate(*args, created_at=200)
        changed = _build_declarative_candidate(
            test_definition,
            args[1],
            args[2],
            observations([100.0, 100.0, 100.0, 100.0]),
            snapshot(4),
            created_at=200,
        )

        self.assertEqual(first.baseline_id, second.baseline_id)
        self.assertEqual(first.payload_digest, second.payload_digest)
        self.assertNotEqual(first.baseline_id, changed.baseline_id)

    def test_effective_global_robust_z_changes_candidate_identity(self) -> None:
        test_definition = definition()
        test_definition = replace(
            test_definition,
            health=replace(test_definition.health, robust_z_threshold=None),
        )
        values = observations([90.0, 100.0, 110.0])
        combination = canonicalize_factors({"image_name": "img"})
        narrow = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            values,
            snapshot(3, test_definition, combination),
            robust_z_threshold=1.0,
        )
        wide = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            values,
            snapshot(3, test_definition, combination),
            robust_z_threshold=3.5,
        )

        self.assertLess(narrow.metrics[0].delta, wide.metrics[0].delta)
        self.assertNotEqual(narrow.baseline_id, wide.baseline_id)
        self.assertTrue(
            validate_candidate(
                narrow,
                test_definition,
                robust_z_threshold=1.0,
            ).activation_ready
        )
        self.assertFalse(
            validate_candidate(
                narrow,
                test_definition,
                robust_z_threshold=3.5,
            ).activation_ready
        )

    def test_candidate_identity_canonicalizes_metric_order(self) -> None:
        test_definition = definition(
            metrics=(
                HealthMetric("first", "source-a", "low_bad", 10.0),
                HealthMetric("second", "source-b", "high_bad", 10.0),
            )
        )
        combination = canonicalize_factors({"image_name": "img"})
        values = observations([100.0] * 3) + observations(
            [50.0] * 3,
            source="source-b",
            metric_name="second-expanded",
        )
        built = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            values,
            snapshot(3, test_definition, combination),
        )
        reversed_candidate = replace(
            built,
            baseline_id="",
            payload_digest="",
            metrics=tuple(reversed(built.metrics)),
        )
        digest, baseline_id = _candidate_identity(reversed_candidate)

        self.assertEqual(digest, built.payload_digest)
        self.assertEqual(baseline_id, built.baseline_id)

    def test_candidate_identity_and_statistics_ignore_observation_order(self) -> None:
        test_definition = definition(min_samples=8)
        combination = canonicalize_factors({"image_name": "img"})
        values = observations([1.0, 8.0, 2.0, 7.0, 3.0, 6.0, 4.0, 5.0])
        source = snapshot(8, test_definition, combination)

        first = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            values,
            source,
            created_at=200,
        )
        permuted = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            tuple(reversed(values)),
            source,
            created_at=999,
        )

        self.assertEqual(first.metrics, permuted.metrics)
        self.assertEqual(first.observation_digest, permuted.observation_digest)
        self.assertEqual(first.payload_digest, permuted.payload_digest)
        self.assertEqual(first.baseline_id, permuted.baseline_id)

    def test_health_policy_version_is_content_bound(self) -> None:
        first_definition = definition()
        second_definition = replace(
            first_definition,
            health=replace(
                first_definition.health,
                policy_version="smoke.health.v2",
            ),
        )
        combination = canonicalize_factors({"image_name": "img"})
        first = _build_declarative_candidate(
            first_definition,
            combination,
            metric_specs_from_definition(first_definition),
            observations([100.0] * 3),
            snapshot(3, first_definition, combination),
        )
        second = _build_declarative_candidate(
            second_definition,
            combination,
            metric_specs_from_definition(second_definition),
            observations([100.0] * 3),
            snapshot(3, second_definition, combination),
        )

        self.assertEqual(first.health_policy_version, "smoke.health.v1")
        self.assertEqual(second.health_policy_version, "smoke.health.v2")
        self.assertNotEqual(first.baseline_id, second.baseline_id)

    def test_forged_derived_evidence_is_rejected_even_when_rehashed(self) -> None:
        test_definition = definition()
        genuine = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            observations([100.0] * 3),
            snapshot(3, test_definition),
        )
        variants = (
            replace(genuine.metrics[0], delta=1000.0),
            replace(genuine.metrics[0], sample_count=100),
        )
        for forged_metric in variants:
            provisional = replace(
                genuine,
                baseline_id="",
                payload_digest="",
                metrics=(forged_metric,),
            )
            digest, baseline_id = _candidate_identity(provisional)
            forged = replace(
                provisional,
                payload_digest=digest,
                baseline_id=baseline_id,
            )
            with self.subTest(forged_metric=forged_metric), self.assertRaises(
                ValueError
            ):
                validate_candidate(forged, test_definition)

        provisional = replace(
            genuine,
            baseline_id="",
            payload_digest="",
            observation_digest="sha256:" + "0" * 64,
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged_digest = replace(
            provisional,
            payload_digest=digest,
            baseline_id=baseline_id,
        )
        with self.assertRaisesRegex(ValueError, "observation digest"):
            validate_candidate(forged_digest, test_definition)

        forged_payload = json.loads(genuine.metrics[0].statistics_json)
        forged_payload["lower_bound"] = genuine.metrics[0].center - 1000.0
        forged_metric = replace(
            genuine.metrics[0],
            delta=1000.0,
            statistics_json=json.dumps(
                forged_payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
            thresholds=(),
        )
        forged_metric = replace(
            forged_metric,
            thresholds=health_engine._generate_thresholds(forged_metric),
        )
        provisional = replace(
            genuine,
            baseline_id="",
            payload_digest="",
            metrics=(forged_metric,),
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged_delta = replace(
            provisional,
            payload_digest=digest,
            baseline_id=baseline_id,
        )
        with self.assertRaisesRegex(ValueError, "derive from observations"):
            validate_candidate(forged_delta, test_definition)

        coverage = genuine.source_coverage[0]
        ghost_coverage = replace(
            coverage,
            results=tuple(
                replace(
                    result,
                    sample_keys=tuple(sorted((*result.sample_keys, "ghost"))),
                )
                for result in coverage.results
            ),
        )
        provisional = replace(
            genuine,
            baseline_id="",
            payload_digest="",
            source_coverage=(ghost_coverage,),
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged_coverage = replace(
            provisional,
            payload_digest=digest,
            baseline_id=baseline_id,
        )
        with self.assertRaisesRegex(ValueError, "coverage"):
            validate_candidate(forged_coverage, test_definition)

    def test_large_percentile_inversion_is_not_hidden_by_relative_tolerance(self) -> None:
        test_definition = definition()
        genuine = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            observations([1_000_000_000.0] * 3),
            snapshot(3, test_definition),
        )
        payload = json.loads(genuine.metrics[0].statistics_json)
        payload["p01"] = float(payload["p05"]) + 0.0005
        forged_metric = replace(
            genuine.metrics[0],
            statistics_json=json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        provisional = replace(
            genuine,
            baseline_id="",
            payload_digest="",
            metrics=(forged_metric,),
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged = replace(
            provisional,
            payload_digest=digest,
            baseline_id=baseline_id,
        )
        with self.assertRaisesRegex(ValueError, "percentiles are unordered"):
            validate_candidate(forged, test_definition)

    def test_under_sampled_candidate_is_not_activation_ready(self) -> None:
        test_definition = definition(min_samples=3)
        candidate = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            observations([100.0, 100.0]),
            snapshot(2),
        )

        report = validate_candidate(candidate, test_definition)

        self.assertFalse(report.activation_ready)
        self.assertEqual(
            {gate.name for gate in report.gates if not gate.passed},
            {"source_results", "metric_samples"},
        )

    def test_partial_per_source_result_coverage_blocks_activation(self) -> None:
        test_definition = definition(
            metrics=(
                HealthMetric("first", "source-a", "low_bad", 10.0),
                HealthMetric("second", "source-b", "high_bad", 10.0),
            )
        )
        partial = observations([100.0] * 3) + observations(
            [50.0] * 2,
            source="source-b",
            metric_name="second-expanded",
        )
        candidate = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            partial,
            snapshot(3, test_definition),
        )

        report = validate_candidate(candidate, test_definition)

        self.assertFalse(report.activation_ready)
        self.assertIn(
            "metric_source_coverage",
            {gate.name for gate in report.gates if not gate.passed},
        )

    def test_source_union_cannot_hide_partial_expanded_metric_coverage(self) -> None:
        test_definition = definition()
        partial = tuple(
            MetricObservation(
                result_id=result_id,
                run_id=f"run-{result_id}",
                completed_timestamp=100 + result_id,
                source="source-a",
                metric_name=metric_name,
                sample_key=f"{metric_name}-sample",
                value=100.0,
            )
            for metric_name, result_ids in (
                ("metric-a", (1, 2, 3)),
                ("metric-b", (2, 3, 4)),
            )
            for result_id in result_ids
        )
        candidate = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            partial,
            snapshot(4, test_definition),
        )

        report = validate_candidate(candidate, test_definition)

        self.assertFalse(report.activation_ready)
        self.assertIn(
            "metric_source_coverage",
            {gate.name for gate in report.gates if not gate.passed},
        )

    def test_inconsistent_training_sample_keys_block_activation(self) -> None:
        test_definition = definition()
        training = tuple(
            MetricObservation(
                result_id=result_id,
                run_id=f"run-{result_id}",
                completed_timestamp=100 + result_id,
                source="source-a",
                metric_name="expanded-metric",
                sample_key=sample_key,
                value=100.0,
            )
            for result_id, sample_keys in (
                (1, ("rank0", "rank1")),
                (2, ("rank0",)),
                (3, ("rank0", "rank1")),
            )
            for sample_key in sample_keys
        )
        candidate = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            training,
            snapshot(3, test_definition),
        )

        report = validate_candidate(candidate, test_definition)

        self.assertFalse(report.activation_ready)
        self.assertIn(
            "metric_sample_coverage",
            {gate.name for gate in report.gates if not gate.passed},
        )

    def test_mixed_adapter_schema_versions_are_rejected(self) -> None:
        test_definition = definition()
        source = snapshot(3, test_definition)
        mixed = replace(
            source,
            results=(
                source.results[0],
                replace(source.results[1], adapter_schema_version=2),
                source.results[2],
            ),
        )
        with self.assertRaisesRegex(ValueError, "uniform adapter_schema_version"):
            _build_declarative_candidate(
                test_definition,
                canonicalize_factors({"image_name": "img"}),
                metric_specs_from_definition(test_definition),
                observations([100.0] * 3),
                mixed,
            )

    def test_building_candidate_never_mutates_parent_identity(self) -> None:
        test_definition = definition()
        combination = canonicalize_factors({"image_name": "img"})
        original = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            observations([100.0] * 3),
            snapshot(3, test_definition, combination),
        )
        child = _build_declarative_candidate(
            test_definition,
            original.combination,
            metric_specs_from_definition(test_definition),
            observations([101.0] * 3),
            snapshot(3, test_definition, combination),
            parent_baseline_id=original.baseline_id,
        )

        self.assertEqual(child.parent_baseline_id, original.baseline_id)
        self.assertNotEqual(child.baseline_id, original.baseline_id)

    def test_forged_observation_run_identity_is_rejected(self) -> None:
        test_definition = definition()
        forged = list(observations([100.0] * 3))
        forged[0] = replace(forged[0], run_id="forged-run")

        with self.assertRaisesRegex(ValueError, "identity"):
            _build_declarative_candidate(
                test_definition,
                canonicalize_factors({"image_name": "img"}),
                metric_specs_from_definition(test_definition),
                forged,
                snapshot(3),
            )

    def test_rejects_fractional_ids_timestamps_and_nonstring_identities(self) -> None:
        spec = metric_specs_from_definition(definition())
        malformed = (
            replace(observations([1.0])[0], result_id=1.5),
            replace(observations([1.0])[0], run_id=123),
            replace(observations([1.0])[0], completed_timestamp=1.5),
            replace(observations([1.0])[0], sample_key=123),
        )
        for value in malformed:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_observations((value,), spec)
        with self.assertRaises(ValueError):
            _build_declarative_candidate(
                definition(),
                canonicalize_factors({"image_name": "img"}),
                spec,
                observations([1.0] * 3),
                SourceSnapshot(
                    (
                        SourceResult(
                            1.5,
                            "run-1",
                            101,
                            "sha256:" + "1" * 64,
                            "sha256:" + "2" * 64,
                            validation_test_config_digest(definition()),
                            canonicalize_factors({"image_name": "img"}).key,
                            1,
                            "sha256:" + "3" * 64,
                        ),
                    )
                ),
            )

    def test_rejects_fractional_or_boolean_candidate_timestamp(self) -> None:
        for timestamp in (True, 1.5, -1):
            with self.subTest(timestamp=timestamp), self.assertRaisesRegex(
                ValueError,
                "created_at",
            ):
                _build_declarative_candidate(
                    definition(),
                    canonicalize_factors({"image_name": "img"}),
                    metric_specs_from_definition(definition()),
                    observations([100.0] * 3),
                    snapshot(3),
                    created_at=timestamp,
                )

    def test_forged_metric_spec_identity_fails_quality_gate(self) -> None:
        test_definition = definition()
        original = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            observations([100.0] * 3),
            snapshot(3),
        )
        forged_metric = replace(original.metrics[0], units="forged")
        provisional = replace(
            original,
            baseline_id="",
            payload_digest="",
            metrics=(forged_metric,),
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged = replace(
            provisional,
            baseline_id=baseline_id,
            payload_digest=digest,
        )

        with self.assertRaisesRegex(ValueError, "metric specs"):
            validate_candidate(forged, test_definition)

    def test_nonstring_candidate_method_is_rejected(self) -> None:
        original = _build_declarative_candidate(
            definition(),
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(definition()),
            observations([100.0] * 3),
            snapshot(3),
        )
        provisional = replace(
            original,
            baseline_id="",
            payload_digest="",
            method=1,
        )
        digest, baseline_id = _candidate_identity(provisional)
        forged = replace(
            provisional,
            baseline_id=baseline_id,
            payload_digest=digest,
        )
        with self.assertRaisesRegex(ValueError, "identity fields"):
            validate_candidate(forged, definition())

    def test_effective_global_robust_z_is_content_bound(self) -> None:
        test_definition = definition()
        test_definition = replace(
            test_definition,
            health=replace(
                test_definition.health,
                robust_z_threshold=None,
                metrics=(replace(test_definition.health.metrics[0], tolerance_pct=0.0),),
            ),
        )
        values = observations([90.0, 95.0, 100.0, 105.0, 110.0])
        first = _build_declarative_candidate(
            test_definition,
            canonicalize_factors({"image_name": "img"}),
            metric_specs_from_definition(test_definition),
            values,
            snapshot(5, test_definition),
            robust_z_threshold=1.0,
        )
        second = _build_declarative_candidate(
            test_definition,
            first.combination,
            metric_specs_from_definition(test_definition),
            values,
            snapshot(5, test_definition),
            robust_z_threshold=3.5,
        )

        self.assertEqual(first.robust_z_threshold, 1.0)
        self.assertNotEqual(first.metrics[0].delta, second.metrics[0].delta)
        self.assertNotEqual(first.baseline_id, second.baseline_id)
        self.assertTrue(
            validate_candidate(
                first,
                test_definition,
                robust_z_threshold=1.0,
            ).activation_ready
        )


class HealthBoundaryTests(unittest.TestCase):
    def _candidate(self, direction: str):
        test_definition = definition(direction)
        combination = canonicalize_factors({"image_name": "img"})
        candidate = _build_declarative_candidate(
                test_definition,
                combination,
                metric_specs_from_definition(test_definition),
                observations([100.0] * 3),
                snapshot(3, test_definition, combination),
            )
        return (
            test_definition,
            combination,
            active(candidate, test_definition),
        )

    def _classify(self, direction: str, value: float) -> HealthClassCode:
        test_definition, combination, candidate = self._candidate(direction)
        verdict = _classify_declarative(
            candidate,
            observations([value]),
            raw_status="pass",
            combination=combination,
            test_id="smoke",
            definition=test_definition,
            source_snapshot=snapshot(1, test_definition, combination),
        )
        return verdict.class_code

    def test_low_bad_exact_and_epsilon_boundaries(self) -> None:
        expected = {
            100.0001: HealthClassCode.EXCELLENT,
            100.0: HealthClassCode.NOMINAL,
            90.0: HealthClassCode.NOMINAL,
            89.9999: HealthClassCode.UNDERPERFORMING,
            80.0: HealthClassCode.UNDERPERFORMING,
            79.9999: HealthClassCode.VERY_BAD,
            70.0: HealthClassCode.VERY_BAD,
            69.9999: HealthClassCode.TERRIBLE,
        }
        for value, code in expected.items():
            with self.subTest(value=value):
                self.assertEqual(self._classify("low_bad", value), code)

    def test_high_bad_exact_and_epsilon_boundaries(self) -> None:
        expected = {
            99.9999: HealthClassCode.EXCELLENT,
            100.0: HealthClassCode.NOMINAL,
            110.0: HealthClassCode.NOMINAL,
            110.0001: HealthClassCode.UNDERPERFORMING,
            120.0: HealthClassCode.UNDERPERFORMING,
            120.0001: HealthClassCode.VERY_BAD,
            130.0: HealthClassCode.VERY_BAD,
            130.0001: HealthClassCode.TERRIBLE,
        }
        for value, code in expected.items():
            with self.subTest(value=value):
                self.assertEqual(self._classify("high_bad", value), code)

    def test_two_sided_has_no_excellent_band(self) -> None:
        expected = {
            100.0: HealthClassCode.NOMINAL,
            90.0: HealthClassCode.NOMINAL,
            110.0: HealthClassCode.NOMINAL,
            89.9999: HealthClassCode.UNDERPERFORMING,
            110.0001: HealthClassCode.UNDERPERFORMING,
            79.9999: HealthClassCode.VERY_BAD,
            120.0001: HealthClassCode.VERY_BAD,
            69.9999: HealthClassCode.TERRIBLE,
            130.0001: HealthClassCode.TERRIBLE,
        }
        for value, code in expected.items():
            with self.subTest(value=value):
                self.assertEqual(self._classify("two_sided", value), code)

    def test_absolute_uses_magnitude_as_lower_is_better(self) -> None:
        self.assertEqual(
            self._classify("absolute", -99.0),
            HealthClassCode.EXCELLENT,
        )
        self.assertEqual(
            self._classify("absolute", -110.0),
            HealthClassCode.NOMINAL,
        )
        self.assertEqual(
            self._classify("absolute", -131.0),
            HealthClassCode.TERRIBLE,
        )

    def test_zero_delta_omits_empty_intermediate_bands(self) -> None:
        test_definition = replace(
            definition("two_sided"),
            health=replace(definition("two_sided").health, metrics=(
                HealthMetric(
                    name="metric-rule",
                    source="source-a",
                    direction="two_sided",
                    tolerance_pct=0.0,
                ),
            )),
        )
        combination = canonicalize_factors({"image_name": "img"})
        candidate = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            observations([0.0, 0.0, 0.0]),
            snapshot(3, test_definition, combination),
        )

        self.assertEqual(
            {band.class_code for band in candidate.metrics[0].thresholds},
            {HealthClassCode.NOMINAL, HealthClassCode.TERRIBLE},
        )


class HealthDnrTests(unittest.TestCase):
    def setUp(self) -> None:
        self.definition = definition()
        self.combination = canonicalize_factors({"image_name": "img"})
        self.candidate = _build_declarative_candidate(
            self.definition,
            self.combination,
            metric_specs_from_definition(self.definition),
            observations([100.0] * 3),
            snapshot(3),
        )
        self.active = active(self.candidate, self.definition)

    def test_raw_failure_and_incomplete_are_dnr(self) -> None:
        for status, reason in (
            ("fail", DnrReason.RAW_FAILED),
            ("incomplete", DnrReason.RAW_INCOMPLETE),
        ):
            verdict = _classify_declarative(
                self.active,
                (),
                raw_status=status,
                combination=self.combination,
                test_id="smoke",
                definition=self.definition,
                source_snapshot=SourceSnapshot(()),
            )
            self.assertEqual(verdict.class_code, HealthClassCode.DNR)
            self.assertEqual(verdict.dnr_reason, reason)
            self.assertIsNone(verdict.baseline_id)

    def test_raw_dnr_still_binds_owner_and_omits_unvalidated_baseline(self) -> None:
        foreign = replace(
            self.active,
            candidate=replace(self.active.candidate, test_id="foreign"),
        )
        verdict = _classify_declarative(
            foreign,
            (),
            raw_status="fail",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=SourceSnapshot(()),
        )
        self.assertIsNone(verdict.baseline_id)
        with self.assertRaisesRegex(ValueError, "test identity"):
            _classify_declarative(
                self.active,
                (),
                raw_status="fail",
                combination=self.combination,
                test_id="foreign",
                definition=self.definition,
                source_snapshot=SourceSnapshot(()),
            )

    def test_missing_combination_baseline_or_observations_are_dnr(self) -> None:
        cases = (
            (self.active, None, (), DnrReason.MISSING_COMBINATION),
            (None, self.combination, (), DnrReason.NO_ACTIVE_BASELINE),
            (self.active, self.combination, (), DnrReason.NO_OBSERVATIONS),
        )
        for baseline, combination, values, reason in cases:
            with self.subTest(reason=reason):
                verdict = _classify_declarative(
                    baseline,
                    values,
                    raw_status="pass",
                    combination=combination,
                    test_id="smoke",
                    definition=self.definition,
                    source_snapshot=SourceSnapshot(()),
                )
                self.assertEqual(verdict.class_code, HealthClassCode.DNR)
                self.assertEqual(verdict.dnr_reason, reason)

    def test_incomplete_metric_coverage_is_dnr_not_nominal(self) -> None:
        two_metric_definition = definition(
            metrics=(
                HealthMetric("first", "source-a", "low_bad", 10.0),
                HealthMetric("second", "source-b", "high_bad", 10.0),
            )
        )
        all_observations = observations([100.0] * 3) + observations(
            [50.0] * 3,
            source="source-b",
            metric_name="second-expanded",
        )
        candidate = _build_declarative_candidate(
            two_metric_definition,
            self.combination,
            metric_specs_from_definition(two_metric_definition),
            all_observations,
            snapshot(3, two_metric_definition, self.combination),
        )

        verdict = _classify_declarative(
            active(candidate, two_metric_definition),
            observations([100.0]),
            raw_status="pass",
            combination=self.combination,
            test_id="smoke",
            definition=two_metric_definition,
            source_snapshot=snapshot(1, two_metric_definition, self.combination),
        )

        self.assertEqual(verdict.class_code, HealthClassCode.DNR)
        self.assertEqual(verdict.dnr_reason, DnrReason.INCOMPLETE_METRIC_COVERAGE)

    def test_missing_or_extra_sample_key_is_incomplete_coverage(self) -> None:
        training = tuple(
            MetricObservation(
                result_id=result_id,
                run_id=f"run-{result_id}",
                completed_timestamp=100 + result_id,
                source="source-a",
                metric_name="expanded-metric",
                sample_key=sample_key,
                value=100.0,
            )
            for result_id in range(1, 4)
            for sample_key in ("rank0", "rank1")
        )
        candidate = _build_declarative_candidate(
            self.definition,
            self.combination,
            metric_specs_from_definition(self.definition),
            training,
            snapshot(3, self.definition, self.combination),
        )
        stored = active(candidate, self.definition)
        for sample_keys in (("rank0",), ("rank0", "rank1", "rank2")):
            with self.subTest(sample_keys=sample_keys):
                current = tuple(
                    MetricObservation(
                        result_id=1,
                        run_id="run-1",
                        completed_timestamp=101,
                        source="source-a",
                        metric_name="expanded-metric",
                        sample_key=sample_key,
                        value=100.0,
                    )
                    for sample_key in sample_keys
                )
                verdict = _classify_declarative(
                    stored,
                    current,
                    raw_status="pass",
                    combination=self.combination,
                    test_id="smoke",
                    definition=self.definition,
                    source_snapshot=snapshot(
                        1,
                        self.definition,
                        self.combination,
                    ),
                )
                self.assertEqual(
                    verdict.dnr_reason,
                    DnrReason.INCOMPLETE_METRIC_COVERAGE,
                )

    def test_adapter_schema_version_drift_is_dnr(self) -> None:
        current_source = snapshot(1, self.definition, self.combination)
        current_source = replace(
            current_source,
            results=(
                replace(
                    current_source.results[0],
                    adapter_schema_version=2,
                ),
            ),
        )
        verdict = _classify_declarative(
            self.active,
            observations([100.0]),
            raw_status="pass",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=current_source,
        )
        self.assertEqual(
            verdict.dnr_reason,
            DnrReason.INCOMPATIBLE_ADAPTER_VERSION,
        )

    def test_declarative_verdict_records_versioned_aggregation(self) -> None:
        verdict = _classify_declarative(
            self.active,
            observations([100.0]),
            raw_status="pass",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=snapshot(1, self.definition, self.combination),
        )
        self.assertEqual(
            json.loads(verdict.details_json)["aggregation"],
            "max_metric_class.v1",
        )

    def test_nonfinite_observation_is_engine_error_not_dnr(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite"):
            _classify_declarative(
                self.active,
                observations([float("nan")]),
                raw_status="pass",
                combination=self.combination,
                test_id="smoke",
                definition=self.definition,
                source_snapshot=snapshot(1),
            )

    def test_classification_rebinds_active_baseline_to_current_definition(self) -> None:
        changed = replace(
            self.definition,
            health=replace(
                self.definition.health,
                metrics=(
                    replace(self.definition.health.metrics[0], tolerance_pct=20.0),
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "stale"):
            _classify_declarative(
                self.active,
                observations([85.0]),
                raw_status="pass",
                combination=self.combination,
                test_id="smoke",
                definition=changed,
                source_snapshot=snapshot(1),
            )

    def test_extra_expanded_metric_is_incomplete_coverage_dnr(self) -> None:
        values = observations([100.0]) + observations(
            [100.0],
            metric_name="unexpected-expanded-metric",
        )
        verdict = _classify_declarative(
            self.active,
            values,
            raw_status="pass",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=snapshot(1),
        )
        self.assertEqual(verdict.class_code, HealthClassCode.DNR)
        self.assertEqual(verdict.dnr_reason, DnrReason.INCOMPLETE_METRIC_COVERAGE)

    def test_malformed_active_lifecycle_metadata_is_rejected(self) -> None:
        variants = (
            replace(self.active, activated_at=None),
            replace(self.active, activated_at=True, updated_at=True),
            replace(self.active, activated_at=1.5, updated_at=1.5),
            replace(self.active, activated_at=-1, updated_at=-1),
        )
        for stored in variants:
            with self.subTest(stored=stored), self.assertRaises(ValueError):
                _classify_declarative(
                    stored,
                    observations([100.0]),
                    raw_status="pass",
                    combination=self.combination,
                    test_id="smoke",
                    definition=self.definition,
                    source_snapshot=snapshot(1),
                )

    def test_raw_dnr_does_not_iterate_observations(self) -> None:
        class Exploding:
            def __iter__(self):
                raise RuntimeError("observation iterable executed")

        verdict = _classify_declarative(
            self.active,
            Exploding(),
            raw_status="fail",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=SourceSnapshot(()),
        )
        self.assertEqual(verdict.dnr_reason, DnrReason.RAW_FAILED)

    def test_evaluable_observations_require_nonempty_exact_snapshot(self) -> None:
        with self.assertRaisesRegex(ValueError, "source snapshot"):
            _classify_declarative(
                self.active,
                observations([100.0]),
                raw_status="pass",
                combination=self.combination,
                test_id="smoke",
                definition=self.definition,
                source_snapshot=SourceSnapshot(()),
            )

    def test_boolean_or_float_class_codes_are_rejected(self) -> None:
        for code in (True, 1.0):
            forged = HealthVerdict(
                test_id="smoke",
                combination_key=self.combination.key,
                baseline_id=self.candidate.baseline_id,
                class_code=code,
                class_name="Nominal",
                dnr_reason=None,
                metrics=(),
                details_json="{}",
            )
            with self.subTest(code=code), self.assertRaisesRegex(
                ValueError,
                "class code",
            ):
                validate_health_verdict(
                    forged,
                    test_id="smoke",
                    combination=self.combination,
                    baseline=self.candidate,
                )


class HealthCustomHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.definition = definition(strategy="custom")
        self.combination = canonicalize_factors({"image_name": "img"})
        self.snapshot = snapshot(3, self.definition, self.combination)
        self.observations = observations([100.0] * 3)
        self.context = HealthContext(
            definition=self.definition,
            result_db_path=Path("/tmp/unused.db"),
            combination=self.combination,
            source_snapshot=self.snapshot,
            created_at=200,
        )

    class ValidPlugin:
        health_policy_version = "smoke.health.v1"

        def __init__(self, loaded=()):
            self.loaded = loaded

        def metric_specs(self, definition):
            return metric_specs_from_definition(definition)

        def load_observations(self, _context):
            return self.loaded

        def classify(self, context, baseline, values, base):
            return replace(
                base,
                details_json=json.dumps(
                    {"aggregation": "test-policy.v1"},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )

    def test_valid_custom_hooks_are_framework_validated(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(
            plugin,
            self.context,
            self.observations,
        )
        verdict = classify_with_plugin(
            plugin,
            self.context,
            active(candidate, self.definition),
            self.observations,
        )
        self.assertEqual(verdict.class_code, HealthClassCode.NOMINAL)

    def test_public_plugin_api_loads_canonical_observations(self) -> None:
        plugin = self.ValidPlugin(self.observations)
        candidate = build_candidate_from_plugin(plugin, self.context)
        verdict = classify_from_plugin(
            plugin,
            self.context,
            active(candidate, self.definition),
        )
        self.assertEqual(candidate.observations, validate_observations(
            self.observations,
            metric_specs_from_definition(self.definition),
            source_snapshot=self.snapshot,
        ))
        self.assertEqual(verdict.class_code, HealthClassCode.NOMINAL)

    def test_public_plugin_api_preflights_before_loader(self) -> None:
        class ExplodingPlugin(self.ValidPlugin):
            def load_observations(self, _context):
                raise AssertionError("loader must not run")

        candidate = build_candidate_with_plugin(
            self.ValidPlugin(),
            self.context,
            self.observations,
        )
        stored = active(candidate, self.definition)
        empty = replace(self.context, source_snapshot=SourceSnapshot(()))
        verdict = classify_from_plugin(ExplodingPlugin(), empty, stored)
        self.assertEqual(verdict.dnr_reason, DnrReason.NO_OBSERVATIONS)

        incompatible_source = replace(
            self.snapshot,
            results=tuple(
                replace(result, adapter_schema_version=2)
                for result in self.snapshot.results
            ),
        )
        verdict = classify_from_plugin(
            ExplodingPlugin(),
            replace(self.context, source_snapshot=incompatible_source),
            stored,
        )
        self.assertEqual(verdict.dnr_reason, DnrReason.INCOMPATIBLE_ADAPTER_VERSION)

        stale = ExplodingPlugin()
        stale.health_policy_version = "smoke.health.v2"
        with self.assertRaisesRegex(ValueError, "policy version"):
            build_candidate_from_plugin(stale, self.context)

        class MissingClassifier:
            health_policy_version = "smoke.health.v1"

            def __init__(self):
                self.load_calls = 0

            def metric_specs(self, test_definition):
                return metric_specs_from_definition(test_definition)

            def load_observations(self, _context):
                self.load_calls += 1
                return self.observations

        missing = MissingClassifier()
        with self.assertRaisesRegex(TypeError, "provide classify"):
            build_candidate_from_plugin(missing, self.context)
        self.assertEqual(missing.load_calls, 0)

    def test_custom_build_hook_is_rejected(self) -> None:
        plugin = self.ValidPlugin()
        plugin.build_candidate = lambda _context, _values: (_ for _ in ()).throw(
            AssertionError("custom build hook must not run")
        )
        with self.assertRaisesRegex(ValueError, "framework-owned"):
            build_candidate_with_plugin(plugin, self.context, self.observations)

    def test_custom_candidate_context_forgery_is_rejected(self) -> None:
        plugin = self.ValidPlugin()
        forged_context = replace(
            self.context,
            combination=canonicalize_factors({"image_name": "other"}),
        )
        with self.assertRaisesRegex(ValueError, "provenance|combination"):
            build_candidate_with_plugin(plugin, forged_context, self.observations)

    def test_custom_candidate_uses_only_supplied_result_coverage(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(
            plugin,
            self.context,
            self.observations[:1],
        )
        self.assertEqual(candidate.source_coverage[0].result_ids, (1,))
        self.assertFalse(validate_candidate(candidate, self.definition).activation_ready)

    def test_framework_candidate_uses_supplied_observation_values(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(plugin, self.context, self.observations)
        self.assertEqual(candidate.metrics[0].center, 100.0)

    def test_custom_verdict_invalid_code_is_rejected(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(
            plugin,
            self.context,
            self.observations,
        )
        plugin.classify = lambda _context, _baseline, _values, _base: HealthVerdict(
            test_id="smoke",
            combination_key=self.combination.key,
            baseline_id=candidate.baseline_id,
            class_code=9,
            class_name="Forged",
            dnr_reason=None,
            metrics=(),
            details_json="{}",
        )
        with self.assertRaisesRegex(ValueError, "class code"):
            classify_with_plugin(
                plugin,
                self.context,
                active(candidate, self.definition),
                self.observations,
            )

    def test_custom_hook_cannot_override_framework_dnr_precedence(self) -> None:
        candidate = build_candidate_with_plugin(
            self.ValidPlugin(),
            self.context,
            self.observations,
        )
        stored = active(candidate, self.definition)

        class ForgingPlugin(self.ValidPlugin):
            def metric_specs(self, _definition):
                raise AssertionError("metric_specs must not run for DNR")

            def classify(self, *_args):
                raise AssertionError("custom classifier must not run for DNR")

        cases = (
            (replace(self.context, raw_status="fail"), stored, self.observations, DnrReason.RAW_FAILED),
            (
                replace(self.context, raw_status="incomplete"),
                stored,
                self.observations,
                DnrReason.RAW_INCOMPLETE,
            ),
            (
                replace(self.context, combination=None),
                stored,
                self.observations,
                DnrReason.MISSING_COMBINATION,
            ),
            (self.context, None, self.observations, DnrReason.NO_ACTIVE_BASELINE),
            (self.context, stored, (), DnrReason.NO_OBSERVATIONS),
        )
        for context, baseline, values, reason in cases:
            with self.subTest(reason=reason):
                verdict = classify_with_plugin(
                    ForgingPlugin(),
                    context,
                    baseline,
                    values,
                )
                self.assertEqual(verdict.class_code, HealthClassCode.DNR)
                self.assertEqual(verdict.dnr_reason, reason)

    def test_custom_raw_dnr_does_not_iterate_observations(self) -> None:
        class Exploding:
            def __iter__(self):
                raise RuntimeError("observation iterable executed")

        verdict = classify_with_plugin(
            self.ValidPlugin(),
            replace(self.context, raw_status="fail"),
            None,
            Exploding(),
        )
        self.assertEqual(verdict.dnr_reason, DnrReason.RAW_FAILED)

    def test_custom_hook_must_preserve_framework_metric_verdicts(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(
            plugin,
            self.context,
            self.observations,
        )
        stored = active(candidate, self.definition)
        base = _classify_declarative(
            stored,
            self.observations,
            raw_status="pass",
            combination=self.combination,
            test_id="smoke",
            definition=self.definition,
            source_snapshot=self.snapshot,
        )
        plugin.classify = lambda _context, _baseline, _values, _base: replace(
            base,
            metrics=(replace(base.metrics[0], value=999.0),),
        )

        with self.assertRaisesRegex(ValueError, "preserve framework metric"):
            classify_with_plugin(
                plugin,
                self.context,
                stored,
                self.observations,
            )

    def test_bare_candidate_cannot_be_used_for_classification(self) -> None:
        plugin = self.ValidPlugin()
        candidate = build_candidate_with_plugin(
            plugin,
            self.context,
            self.observations,
        )
        with self.assertRaisesRegex(TypeError, "stored baseline"):
            _classify_declarative(
                candidate,
                self.observations,
                raw_status="pass",
                combination=self.combination,
                test_id="smoke",
                definition=self.definition,
                source_snapshot=self.snapshot,
            )


if __name__ == "__main__":
    unittest.main()
