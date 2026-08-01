from __future__ import annotations

import sqlite3
import hashlib
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from cval.health.combination import resolve_environment_combination
from cval.health.engine import (
    validate_candidate,
    validate_health_verdict,
    validate_metric_specs,
    validate_observations,
)
from cval.health.engine import (
    _build_candidate_with_plugin_observations as build_candidate_with_plugin,
    _classify_with_plugin_observations as classify_with_plugin,
)
from cval.health.models import (
    BaselineLifecycle,
    HealthClassCode,
    HealthContext,
    HealthVerdict,
    MetricObservation,
    MetricVerdict,
    SourceResult,
    SourceSnapshot,
    StoredHealthBaseline,
)
from cval.validation.plugins import load_registered_plugin, validate_registry_plugins
from cval.validation.registry import validation_test_config_digest
from cval.storage.per_test_results import prepare_immutable_table_triggers
from cval.storage.per_test_results import COMMON_IMMUTABLE_KEY_GROUPS
from tests.test_per_test_ingestion import ModularPerTestIngestionTests, _ingest


def _synthetic_sources(definition, combination, count: int = 8):
    return tuple(
        SourceResult(
            index,
            f"run-{index}",
            100 + index,
            "sha256:" + f"{index:064x}",
            "sha256:" + f"{index + 100:064x}",
            validation_test_config_digest(definition),
            combination.key,
            1,
            "sha256:" + f"{index + 200:064x}",
        )
        for index in range(1, count + 1)
    )


def _mutate_behind_immutable_trigger(
    connection: sqlite3.Connection,
    mutation: str,
) -> None:
    table_name = mutation.split()[1]
    connection.execute(f"DROP TRIGGER trg_{table_name}_immutable_update")
    connection.execute(mutation)
    key_groups = (
        (("node", "timestamp"), ("run_id",))
        if table_name == "storage_performance"
        else COMMON_IMMUTABLE_KEY_GROUPS[table_name]
    )
    prepare_immutable_table_triggers(connection, table_name, key_groups)


class BuiltinHealthPluginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = ModularPerTestIngestionTests()

    def _prepared(self, root: Path):
        config = self.fixture._config(root, enabled=True)
        result_path = self.fixture._write_builtin_result(root, config)
        report = _ingest(result_path, config)
        self.assertTrue(report.ok)
        return config

    @staticmethod
    def _context(config, test_id: str, db_path: Path) -> HealthContext:
        registered = config.tests.registry.require(test_id)
        combination = resolve_environment_combination(
            registered.definition,
            {
                "image_name": "image",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
            },
        )
        with closing(sqlite3.connect(db_path)) as connection:
            row = connection.execute(
                "SELECT result_id, run_id, "
                "COALESCE(completed_timestamp, started_timestamp), combination_key, "
                "result_digest, raw_result_json, test_config_digest, "
                "(SELECT version FROM adapter_schema_versions LIMIT 1), "
                "(SELECT evidence_digest FROM metric_ingestion_receipts "
                " WHERE metric_ingestion_receipts.run_id=test_results.run_id) "
                "FROM test_results"
            ).fetchone()
        assert combination is not None
        if row[3] != combination.key:
            raise AssertionError("ingested combination key does not match health context")
        return HealthContext(
            definition=registered.definition,
            result_db_path=db_path,
            combination=combination,
            source_snapshot=SourceSnapshot(
                (
                    SourceResult(
                        int(row[0]),
                        str(row[1]),
                        int(row[2]),
                        str(row[4]),
                        "sha256:"
                        + hashlib.sha256(str(row[5]).encode("utf-8")).hexdigest(),
                        str(row[6]),
                        str(row[3]),
                        int(row[7]),
                        str(row[8]),
                    ),
                )
            ),
            robust_z_threshold=config.baseline.robust_z_threshold,
            created_at=int(row[2]) + 100,
        )

    def test_builtin_health_specs_and_observations_are_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            expected = {
                "storage": ("storage_performance", 12),
                "nccl": ("busbw", 2),
                "dltest": ("numerical_correctness", 96),
            }
            for test_id, (source, minimum_count) in expected.items():
                with self.subTest(test_id=test_id):
                    registered = config.tests.registry.require(test_id)
                    plugin = load_registered_plugin(registered)
                    db_path = (
                        root
                        / "evaluator_state"
                        / "validation_tests"
                        / test_id
                        / f"{test_id}_results.db"
                    )
                    context = self._context(config, test_id, db_path)
                    specs = plugin.metric_specs(registered.definition)
                    observations = plugin.load_observations(context)

                    validate_metric_specs(specs, registered.definition)
                    validate_observations(
                        observations,
                        specs,
                        allowed_result_ids=context.source_snapshot.result_ids,
                    )
                    self.assertGreaterEqual(len(observations), minimum_count)
                    self.assertIn(source, {observation.source for observation in observations})

    def test_dl_custom_hooks_return_framework_validated_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            registered = config.tests.registry.require("dltest")
            plugin = load_registered_plugin(registered)
            db_path = root / "evaluator_state/validation_tests/dltest/dltest_results.db"
            context = self._context(config, "dltest", db_path)
            observations = plugin.load_observations(context)
            first_source = context.source_snapshot.results[0]
            expanded = tuple(
                replace(
                    observation,
                    result_id=result_id,
                    run_id=(
                        first_source.run_id if result_id == 1 else f"synthetic-{result_id}"
                    ),
                    completed_timestamp=first_source.completed_timestamp + result_id - 1,
                )
                for result_id in range(1, 9)
                for observation in observations
            )
            build_context = replace(
                context,
                source_snapshot=SourceSnapshot(
                    tuple(
                        SourceResult(
                            result_id,
                            first_source.run_id
                            if result_id == 1
                            else f"synthetic-{result_id}",
                            first_source.completed_timestamp + result_id - 1,
                            "sha256:" + f"{result_id:064x}",
                            "sha256:" + f"{result_id + 100:064x}",
                            first_source.test_config_digest,
                            first_source.combination_key,
                            first_source.adapter_schema_version,
                            "sha256:" + f"{result_id + 200:064x}",
                        )
                        for result_id in range(1, 9)
                    )
                ),
            )
            candidate = build_candidate_with_plugin(plugin, build_context, expanded)
            stored = StoredHealthBaseline(
                candidate=candidate,
                lifecycle=BaselineLifecycle.ACTIVE,
                quality=validate_candidate(
                    candidate,
                    registered.definition,
                    robust_z_threshold=config.baseline.robust_z_threshold,
                ),
                updated_at=candidate.created_at + 1,
                activated_at=candidate.created_at + 1,
                superseded_at=None,
            )
            verdict = classify_with_plugin(plugin, context, stored, observations)

            self.assertIn(
                verdict.class_code,
                {HealthClassCode.EXCELLENT, HealthClassCode.NOMINAL},
            )
            validate_health_verdict(
                verdict,
                test_id="dltest",
                combination=context.combination,
                baseline=candidate,
            )

    def test_context_cannot_forge_unlisted_result_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            registered = config.tests.registry.require("storage")
            plugin = load_registered_plugin(registered)
            db_path = root / "evaluator_state/validation_tests/storage/storage_results.db"
            valid = self._context(config, "storage", db_path)
            forged = HealthContext(
                definition=valid.definition,
                result_db_path=valid.result_db_path,
                combination=valid.combination,
                source_snapshot=SourceSnapshot(
                    (
                        SourceResult(
                            999,
                            "forged",
                            999,
                            "sha256:" + "1" * 64,
                            "sha256:" + "2" * 64,
                            valid.source_snapshot.results[0].test_config_digest,
                            valid.source_snapshot.results[0].combination_key,
                            valid.source_snapshot.results[0].adapter_schema_version,
                            "sha256:" + "3" * 64,
                        ),
                    )
                ),
            )

            with self.assertRaisesRegex(RuntimeError, "missing raw results"):
                plugin.load_observations(forged)

    def test_builtin_storage_reader_rejects_coercive_sqlite_values(self) -> None:
        for mutation in (
            "UPDATE test_results SET completed_timestamp=started_timestamp+0.5",
            "UPDATE storage_performance SET randread_iops=x'3130302e30'",
        ):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config = self._prepared(root)
                registered = config.tests.registry.require("storage")
                plugin = load_registered_plugin(registered)
                db_path = root / "evaluator_state/validation_tests/storage/storage_results.db"
                context = self._context(config, "storage", db_path)
                with closing(sqlite3.connect(db_path)) as connection:
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(mutation)
                    _mutate_behind_immutable_trigger(connection, mutation)
                    connection.commit()

                with self.assertRaisesRegex((ValueError, RuntimeError), "SQLite|provenance"):
                    plugin.load_observations(context)

    def test_builtin_storage_reader_rejects_version_receipt_and_owner_corruption(self) -> None:
        mutations = (
            "UPDATE adapter_schema_versions SET version=999",
            "UPDATE adapter_schema_versions SET applied_at=1.5",
            "UPDATE metric_ingestion_receipts SET adapter_api_version='future'",
            "UPDATE metric_ingestion_receipts SET evidence_digest='bad'",
            "UPDATE metric_ingestion_receipts SET metric_names_json='[]'",
            "UPDATE metric_ingestion_receipts SET inserted_count=0",
            "UPDATE metric_ingestion_receipts SET created_at=1.5",
            "UPDATE test_results SET test_id='other'",
            "UPDATE test_results SET test_config_digest='sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'",
            "UPDATE test_results SET result_digest='bad'",
            "UPDATE test_results SET raw_result_json='{}'",
            "UPDATE storage_performance SET node='other'",
            "UPDATE storage_performance SET image_name='other'",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config = self._prepared(root)
                registered = config.tests.registry.require("storage")
                plugin = load_registered_plugin(registered)
                db_path = root / "evaluator_state/validation_tests/storage/storage_results.db"
                context = self._context(config, "storage", db_path)
                with closing(sqlite3.connect(db_path)) as connection:
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(mutation)
                    _mutate_behind_immutable_trigger(connection, mutation)
                    connection.commit()

                with self.assertRaisesRegex(
                    RuntimeError,
                    "manifest|provenance|ownership|receipt",
                ):
                    plugin.load_observations(context)

    def test_registry_validation_loads_all_health_hooks(self) -> None:
        config = self.fixture._config(Path("/tmp/cval-health-unused"), enabled=False)
        loaded = validate_registry_plugins(config.tests.registry.tests)
        self.assertEqual(loaded, ("storage", "nccl", "dltest"))

    def test_dl_custom_aggregation_config_rejects_invalid_ranges_and_types(self) -> None:
        config = self.fixture._config(Path("/tmp/cval-health-unused"), enabled=False)
        registered = config.tests.registry.require("dltest")
        plugin = load_registered_plugin(registered)
        invalid = (
            {"degraded_metric_fraction": 1.1, "min_degraded_metrics": 10, "degraded_severity_pct": 10.0},
            {"degraded_metric_fraction": 0.1, "min_degraded_metrics": True, "degraded_severity_pct": 10.0},
            {"degraded_metric_fraction": 0.1, "min_degraded_metrics": 10, "degraded_severity_pct": float("nan")},
            {"degraded_metric_fraction": 0.1, "min_degraded_metrics": 10, "degraded_severity_pct": -1.0},
        )
        for aggregation in invalid:
            with self.subTest(aggregation=aggregation):
                definition = replace(
                    registered.definition,
                    settings={
                        **registered.definition.settings,
                        "health_aggregation": aggregation,
                    },
                )
                self.assertTrue(plugin.validate_config(definition))

    def test_dl_zero_center_terrible_metric_is_not_hidden_by_relative_percent(self) -> None:
        config = self.fixture._config(Path("/tmp/cval-health-unused"), enabled=False)
        registered = config.tests.registry.require("dltest")
        plugin = load_registered_plugin(registered)
        combination = resolve_environment_combination(
            registered.definition,
            {
                "image_name": "image",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
            },
        )
        assert combination is not None
        source_results = _synthetic_sources(registered.definition, combination)
        sources = (
            "numerical_correctness",
            "compute_performance",
            "collective_performance",
            "overlap_performance",
        )
        training = tuple(
            MetricObservation(
                result_id=index,
                run_id=f"run-{index}",
                completed_timestamp=100 + index,
                source=source,
                metric_name=f"{source}/metric",
                sample_key=source,
                value=0.0,
            )
            for index in range(1, 9)
            for source in sources
        )
        build_context = HealthContext(
            definition=registered.definition,
            result_db_path=Path("/tmp/unused.db"),
            combination=combination,
            source_snapshot=SourceSnapshot(source_results),
            robust_z_threshold=config.baseline.robust_z_threshold,
            created_at=200,
        )
        candidate = build_candidate_with_plugin(plugin, build_context, training)
        stored = StoredHealthBaseline(
            candidate=candidate,
            lifecycle=BaselineLifecycle.ACTIVE,
            quality=validate_candidate(
                candidate,
                registered.definition,
                robust_z_threshold=config.baseline.robust_z_threshold,
            ),
            updated_at=201,
            activated_at=201,
            superseded_at=None,
        )
        current = tuple(
            MetricObservation(
                result_id=1,
                run_id="run-1",
                completed_timestamp=101,
                source=source,
                metric_name=f"{source}/metric",
                sample_key=source,
                value=1.0 if source == "numerical_correctness" else 0.0,
            )
            for source in sources
        )
        classify_context = replace(
            build_context,
            source_snapshot=SourceSnapshot((source_results[0],)),
        )

        verdict = classify_with_plugin(plugin, classify_context, stored, current)

        numerical = next(
            metric
            for metric in verdict.metrics
            if metric.source == "numerical_correctness"
        )
        self.assertEqual(numerical.class_code, HealthClassCode.TERRIBLE)
        self.assertEqual(numerical.pct_diff, 0.0)
        self.assertGreaterEqual(numerical.severity_pct, 10.0)
        self.assertEqual(verdict.class_code, HealthClassCode.TERRIBLE)

    def test_dl_cannot_be_excellent_with_nonsevere_underperforming_metric(self) -> None:
        config = self.fixture._config(Path("/tmp/cval-health-unused"), enabled=False)
        registered = config.tests.registry.require("dltest")
        plugin = load_registered_plugin(registered)
        combination = resolve_environment_combination(
            registered.definition,
            {
                "image_name": "image",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
            },
        )
        assert combination is not None
        context = HealthContext(
            definition=registered.definition,
            result_db_path=Path("/tmp/unused.db"),
            combination=combination,
            source_snapshot=SourceSnapshot(()),
            robust_z_threshold=config.baseline.robust_z_threshold,
        )
        training_results = _synthetic_sources(registered.definition, combination)
        training = tuple(
            MetricObservation(
                result_id=index,
                run_id=f"run-{index}",
                completed_timestamp=100 + index,
                source=source,
                metric_name=f"{source}/metric",
                sample_key=source,
                value=100.0,
            )
            for index in range(1, 9)
            for source in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            )
        )
        candidate = build_candidate_with_plugin(
            plugin,
            replace(
                context,
                source_snapshot=SourceSnapshot(training_results),
                created_at=200,
            ),
            training,
        )
        metrics = (
            MetricVerdict(
                source="compute_performance",
                metric_name="fast",
                value=90.0,
                class_code=HealthClassCode.EXCELLENT,
                class_name="Excellent",
                pct_diff=-10.0,
                severity_pct=10.0,
            ),
            MetricVerdict(
                source="numerical_correctness",
                metric_name="small-miss",
                value=100.15,
                class_code=HealthClassCode.UNDERPERFORMING,
                class_name="Underperforming",
                pct_diff=0.15,
                severity_pct=0.15,
            ),
        )
        base = HealthVerdict(
            test_id="dltest",
            combination_key=combination.key,
            baseline_id=candidate.baseline_id,
            class_code=HealthClassCode.UNDERPERFORMING,
            class_name="Underperforming",
            dnr_reason=None,
            metrics=metrics,
            details_json="{}",
        )

        verdict = plugin.classify(context, candidate, (), base)

        self.assertEqual(verdict.class_code, HealthClassCode.NOMINAL)


if __name__ == "__main__":
    unittest.main()
