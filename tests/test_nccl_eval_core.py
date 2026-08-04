from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone
from uuid import UUID

from cval.config import load_config
from cval.nccl_eval.models import (
    IngestionBatch,
    NicResult,
    NodeResult,
    ResultStatus,
    TestRun,
)
from cval.nccl_eval.profile import (
    build_profile_identity,
    canonical_test_config,
    test_config_fingerprint,
)
from cval.nccl_eval.thresholds import (
    MetricName,
    ThresholdRange,
    classify,
    derive_thresholds,
    empirical_severity,
    overall_health,
    percentile,
    piecewise_severity,
    validate_ranges,
)
from cval.validation.runtime import build_runtime_environment


UTC = timezone.utc


def test_run(**overrides: object) -> TestRun:
    values: dict[str, object] = {
        "run_id": UUID("11111111-1111-4111-8111-111111111111"),
        "test_name": "nccl-loopback-allreduce",
        "test_definition_version": "test-v1",
        "started_at": datetime(2026, 1, 1, tzinfo=UTC),
        "completed_at": datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        "image_name": "example/image@sha256:" + "b" * 64,
        "image_digest": "sha256:" + "b" * 64,
        "cuda_version": "13.2",
        "pytorch_version": "2.12",
        "compiled_nccl_version": "2.27",
        "runtime_nccl_package_version": "nvidia-nccl-cu13==2.27.7",
        "driver_version": "600.1",
        "driver_version_group": "r600",
        "topology_class": "8gpu-loopback-v1",
        "gpu_model": "B200",
        "gpus_per_node": 8,
        "iterations": 20,
        "samples": 20,
        "cval_run_id": "node-a-123",
        "cval_result_digest": "sha256:" + "c" * 64,
        "summary_sha256": "sha256:" + "d" * 64,
        "runtime_evidence_sha256": "sha256:" + "e" * 64,
        "source_commit": "a" * 40,
        "implementation_identity": "sha256:" + "f" * 64,
        "legacy_source": False,
        "test_config": {
            "collective": "all_reduce",
            "datatype": "bfloat16",
            "reduction": "sum",
            "message_size": "16GiB",
            "warmup_iterations": 1,
            "latency_unit": "us",
        },
    }
    values.update(overrides)
    return TestRun(**values)  # type: ignore[arg-type]


class NcclEvalModelProfileTests(unittest.TestCase):
    def test_descriptor_exposes_material_constants_but_not_live_hardware_guesses(self) -> None:
        config = load_config()
        settings = config.tests.registry.require("nccl").definition.settings
        environment = build_runtime_environment(config)

        self.assertFalse(settings["evaluation_enabled"])
        self.assertEqual(settings["evaluation_test_name"], "nccl-loopback-allreduce")
        self.assertEqual(settings["evaluation_driver_group_source"], "runtime_evidence")
        self.assertEqual(settings["evaluation_topology_class_source"], "runtime_evidence")
        self.assertEqual(environment["CVAL_NCCL_EVALUATION_COLLECTIVE"], "all_reduce")
        self.assertEqual(environment["CVAL_NCCL_EVALUATION_DATATYPE"], "bfloat16")
        self.assertEqual(settings["evaluation_latency_unit"], "us")
        self.assertEqual(settings["evaluation_latency_source_unit"], "ms")
        self.assertEqual(settings["evaluation_latency_conversion"], "ms_to_us_x1000")
        self.assertEqual(environment["CVAL_NCCL_EVALUATION_LATENCY_UNIT"], "us")
        self.assertEqual(environment["CVAL_NCCL_EVALUATION_SAMPLES_PER_RESULT"], "1")
        self.assertEqual(
            environment["CVAL_NCCL_EVALUATION_ITERATION_SEMANTICS"],
            "timed_collective_repetitions",
        )
        self.assertEqual(
            environment["CVAL_NCCL_OUTBOX_ROOT"],
            "/data/continuous_validation/nccl_eval/outbox",
        )
        for guessed_field in ("gpu_model", "driver_version", "nccl_version", "topology_class"):
            self.assertNotIn(guessed_field, settings)

    def test_models_are_validated_frozen_and_utc(self) -> None:
        node = NodeResult(
            node_name="node-a",
            test_timestamp=datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-8))),
            bus_bw_gbps=44.5,
            latency_us=600.0,
            nics=(NicResult("mlx5_0", 44.0), NicResult("mlx5_4", None)),
        )
        batch = IngestionBatch(test_run(), (node,))

        self.assertEqual(node.test_timestamp.tzinfo, UTC)
        self.assertEqual(batch.node_results[0].result_status, ResultStatus.SUCCESS)
        with self.assertRaises(TypeError):
            batch.test_run.test_config["collective"] = "broadcast"  # type: ignore[index]

    def test_models_reject_nonfinite_metrics_duplicates_and_missing_profile_config(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite and non-negative"):
            NicResult("mlx5_0", math.inf)
        for invalid in ("mlx5_x", "mlx5_0.bad", "MLX5_0", "mlx5_0_extra"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "mlx5_<device>"
            ):
                NicResult(invalid, 1.0)
        with self.assertRaisesRegex(ValueError, "unique device"):
            NodeResult(
                "node-a",
                datetime.now(UTC),
                1.0,
                1.0,
                nics=(NicResult("mlx5_0", 1.0), NicResult("mlx5_0", 2.0)),
            )
        with self.assertRaisesRegex(ValueError, "message_size"):
            test_run(test_config={"collective": "all_reduce", "datatype": "bf16", "reduction": "sum"})
        with self.assertRaisesRegex(ValueError, "SUCCESS"):
            NodeResult("node-a", datetime.now(UTC), 1.0, None)
        invalid_unit = dict(test_run().test_config)
        invalid_unit["latency_unit"] = "ms"
        with self.assertRaisesRegex(ValueError, "canonical unit 'us'"):
            test_run(test_config=invalid_unit)
        with self.assertRaisesRegex(ValueError, "unique node_name"):
            node = NodeResult("node-a", datetime.now(UTC), 1.0, 1.0)
            IngestionBatch(test_run(), (node, node))

    def test_dict_loader_is_strict_and_round_trips(self) -> None:
        original = IngestionBatch(
            test_run(),
            (
                NodeResult(
                    "node-a",
                    datetime(2026, 1, 1, tzinfo=UTC),
                    42.0,
                    700.0,
                    nics=(NicResult("mlx5_0", 40.0),),
                ),
            ),
        )
        loaded = IngestionBatch.from_dict(original.to_dict())
        self.assertEqual(loaded, original)
        self.assertEqual(loaded.test_run.test_config["iterations"], 20)
        self.assertEqual(loaded.test_run.test_config["samples"], 20)
        payload = original.to_dict()
        payload["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unknown"):
            IngestionBatch.from_dict(payload)

    def test_legacy_source_ingestion_is_rejected(self) -> None:
        payload = IngestionBatch(
            test_run(),
            (NodeResult("node-a", datetime.now(UTC), 1.0, 1.0),),
        ).to_dict()
        payload["test_run"]["legacy_source"] = True

        with self.assertRaisesRegex(ValueError, "copied SQLite ingestion is removed"):
            IngestionBatch.from_dict(payload)

    def test_fingerprint_is_canonical_and_profile_identity_is_deterministic(self) -> None:
        left = {
            "message_size": "16GiB",
            "collective": "all_reduce",
            "datatype": "bfloat16",
            "reduction": "sum",
            "nested": {"b": 2, "a": 1},
            "warmup_iterations": 1,
            "latency_unit": "us",
        }
        right = {
            "nested": {"a": 1, "b": 2},
            "reduction": "sum",
            "datatype": "bfloat16",
            "collective": "all_reduce",
            "message_size": "16GiB",
            "warmup_iterations": 1,
            "latency_unit": "us",
        }
        self.assertEqual(canonical_test_config(left), canonical_test_config(right))
        self.assertEqual(test_config_fingerprint(left), test_config_fingerprint(right))
        self.assertRegex(test_config_fingerprint(left), r"^sha256:[0-9a-f]{64}$")

        first = build_profile_identity(test_run(test_config=left))
        second = build_profile_identity(test_run(test_config=right))
        changed = build_profile_identity(test_run(topology_class="different-topology", test_config=right))
        self.assertEqual(first.profile_id, second.profile_id)
        self.assertEqual(first.profile_key, second.profile_key)
        self.assertNotEqual(first.profile_id, changed.profile_id)
        self.assertEqual(first.test_config["iterations"], 20)
        self.assertEqual(first.test_config["samples"], 20)
        self.assertNotEqual(first.profile_id, build_profile_identity(test_run(iterations=100)))
        self.assertNotEqual(first.profile_id, build_profile_identity(test_run(samples=None)))
        self.assertNotEqual(
            first.profile_id,
            build_profile_identity(test_run(source_commit="b" * 40)),
        )
        self.assertNotEqual(
            first.profile_id,
            build_profile_identity(
                test_run(
                    image_name="example/image@sha256:" + "c" * 64,
                    image_digest="sha256:" + "c" * 64,
                )
            ),
        )
        self.assertNotEqual(
            first.profile_id,
            build_profile_identity(
                test_run(implementation_identity="sha256:" + "0" * 64)
            ),
        )
        for fragment in ("b200", "8gpu", "cuda-13.2", "nccl-compiled-2.27"):
            self.assertIn(fragment, first.profile_key)
        self.assertLessEqual(len(first.profile_key), 255)

    def test_fingerprint_preserves_json_scalar_types_and_signed_zero(self) -> None:
        base = dict(test_run().test_config)
        for left, right in ((True, 1), (1, 1.0), (-0.0, 0.0)):
            with self.subTest(left=left, right=right):
                self.assertNotEqual(
                    test_config_fingerprint(base | {"variant": left}),
                    test_config_fingerprint(base | {"variant": right}),
                )


class NcclEvalThresholdTests(unittest.TestCase):
    def test_percentile_uses_linear_interpolation_and_rejects_bad_samples(self) -> None:
        self.assertEqual(percentile([0.0, 10.0], 0.5), 5.0)
        self.assertEqual(percentile([7.0], 0.95), 7.0)
        self.assertEqual(percentile([0.0, 10.0, 20.0, 30.0], 0.25), 7.5)
        with self.assertRaises(ValueError):
            percentile([], 0.5)
        with self.assertRaises(ValueError):
            percentile([1.0, math.nan], 0.5)

    def test_five_ranges_are_contiguous_cover_infinity_and_classify_boundaries_once(self) -> None:
        values = [float(item) for item in range(40, 80)]
        bus = derive_thresholds(
            MetricName.BUS_BW, values, derivation_method_version="derive-v1"
        )
        latency = derive_thresholds(
            MetricName.LATENCY, values, derivation_method_version="derive-v1"
        )
        for derived in (bus, latency):
            validate_ranges(derived.ranges, metric_name=derived.metric_name)
            ordered = sorted(derived.ranges, key=lambda item: item.lower_bound)
            self.assertEqual(ordered[0].lower_bound, 0.0)
            self.assertIsNone(ordered[-1].upper_bound)
            for current, following in zip(ordered, ordered[1:]):
                boundary = current.upper_bound
                self.assertIsNotNone(boundary)
                self.assertEqual(classify(boundary, derived.ranges), following.class_id)

        self.assertEqual(classify(0.0, bus.ranges), 5)
        self.assertEqual(classify(1_000_000.0, bus.ranges), 1)
        self.assertEqual(classify(0.0, latency.ranges), 1)
        self.assertEqual(classify(1_000_000.0, latency.ranges), 5)

    def test_equal_and_small_distributions_are_deterministic(self) -> None:
        first = derive_thresholds(
            MetricName.BUS_BW, [44.0] * 40, derivation_method_version="derive-v1"
        )
        second = derive_thresholds(
            MetricName.BUS_BW, [44.0] * 40, derivation_method_version="derive-v1"
        )
        self.assertEqual(first, second)
        latency = derive_thresholds(
            MetricName.LATENCY, [44.0] * 40, derivation_method_version="derive-v1"
        )
        self.assertEqual(classify(44.0, first.ranges), 2)
        self.assertEqual(classify(44.0, latency.ranges), 2)
        self.assertEqual(piecewise_severity(44.0, first.summary, higher_is_better=True), 50.0)
        self.assertEqual(piecewise_severity(44.0, latency.summary, higher_is_better=False), 50.0)
        with self.assertRaisesRegex(ValueError, "strictly positive"):
            derive_thresholds(
                MetricName.LATENCY, [0.0], derivation_method_version="derive-v1"
            )

    def test_median_bands_have_exact_boundaries_and_monotonic_health(self) -> None:
        bus = derive_thresholds(
            MetricName.BUS_BW, [44.0] * 40, derivation_method_version="derive-v2"
        )
        latency = derive_thresholds(
            MetricName.LATENCY, [44.0] * 40, derivation_method_version="derive-v2"
        )
        self.assertEqual(
            [item.class_id for item in sorted(bus.ranges, key=lambda item: item.lower_bound)],
            [5, 4, 3, 2, 1],
        )
        self.assertEqual(
            [item.class_id for item in sorted(latency.ranges, key=lambda item: item.lower_bound)],
            [1, 2, 3, 4, 5],
        )
        for actual, expected in zip(
            [item.lower_bound for item in sorted(bus.ranges, key=lambda item: item.lower_bound)],
            [0.0, 30.8, 37.4, 41.8, 46.2],
        ):
            self.assertAlmostEqual(actual, expected)
        for actual, expected in zip(
            [item.lower_bound for item in sorted(latency.ranges, key=lambda item: item.lower_bound)],
            [0.0, 41.8, 46.2, 50.6, 57.2],
        ):
            self.assertAlmostEqual(actual, expected)
        values = [20.0, 35.0, 44.0, 50.0, 70.0]
        self.assertEqual(
            [classify(value, bus.ranges) for value in values],
            sorted((classify(value, bus.ranges) for value in values), reverse=True),
        )
        self.assertEqual(
            [classify(value, latency.ranges) for value in values],
            sorted(classify(value, latency.ranges) for value in values),
        )
        bus_severity = [
            piecewise_severity(value, bus.summary, higher_is_better=True)
            for value in values
        ]
        latency_severity = [
            piecewise_severity(value, latency.summary, higher_is_better=False)
            for value in values
        ]
        self.assertEqual(bus_severity, sorted(bus_severity, reverse=True))
        self.assertEqual(latency_severity, sorted(latency_severity))

    def test_invalid_ranges_are_rejected(self) -> None:
        ranges = (
            ThresholdRange(MetricName.BUS_BW, 5, 0.0, 1.0, "GB/s"),
            ThresholdRange(MetricName.BUS_BW, 4, 2.0, 3.0, "GB/s"),
            ThresholdRange(MetricName.BUS_BW, 3, 3.0, 4.0, "GB/s"),
            ThresholdRange(MetricName.BUS_BW, 2, 4.0, 5.0, "GB/s"),
            ThresholdRange(MetricName.BUS_BW, 1, 5.0, None, "GB/s"),
        )
        with self.assertRaisesRegex(ValueError, "contiguous"):
            validate_ranges(ranges)

    def test_severity_and_overall_use_worse_metric(self) -> None:
        samples = [10.0, 20.0, 30.0, 40.0, 50.0]
        self.assertEqual(empirical_severity(samples, 50.0, higher_is_better=True), 0.0)
        self.assertEqual(empirical_severity(samples, 10.0, higher_is_better=True), 100.0)
        self.assertEqual(empirical_severity(samples, 10.0, higher_is_better=False), 0.0)
        self.assertEqual(empirical_severity([44.0] * 5, 44.0, higher_is_better=True), 50.0)
        summary = derive_thresholds(
            MetricName.BUS_BW, samples, derivation_method_version="v1"
        ).summary
        self.assertLess(
            piecewise_severity(50.0, summary, higher_is_better=True),
            piecewise_severity(10.0, summary, higher_is_better=True),
        )
        self.assertEqual(overall_health(2, 5, 10.0, 99.0), (5, 99.0))


if __name__ == "__main__":
    unittest.main()
