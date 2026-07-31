"""Exact built-in compatibility fixtures through U10 plugin dispatch."""

from __future__ import annotations

import copy
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.baselines import build, stats
from cval.baselines.classify import classify_node
from cval.config import load_config
from cval.storage.ingest import STORAGE_METRIC_COLUMNS
from cval.validation.operations import (
    build_compatibility_baseline,
    classify_compatibility_target,
    resolve_operational_target,
    validate_compatibility_classification_verdicts,
)
from cval.validation.operational_targets import BASELINE_CLASSIFY
from tests.test_baseline_build import NOW, _make_nccl_db, _make_storage_db
from tests.test_baseline_classify import _make_nccl_two_nodes, _make_storage_two_nodes


class BuiltinOperationalCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config()

    def test_storage_baseline_and_classification_match_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "storage.db"
            _make_storage_db(source)
            with patch("cval.baselines.build.time.time", return_value=NOW):
                expected = build.build_storage_baseline(
                    config=self.config,
                    db_path=source,
                    window_days=365,
                    min_samples=5,
                    image_name="img:1",
                    node="node-a",
                    baseline_id="storage-fixture",
                )
                actual = build_compatibility_baseline(
                    self.config,
                    "storage",
                    source_db=str(source),
                    window_days=365,
                    min_samples=5,
                    image_name="img:1",
                    node="node-a",
                    baseline_id="storage-fixture",
                )
            self.assertEqual(actual, expected)

            two_nodes = Path(tmpdir) / "storage-two.db"
            _make_storage_two_nodes(two_nodes)
            baseline = build.build_storage_baseline(
                config=self.config,
                db_path=two_nodes,
                window_days=365,
                min_samples=5,
                node="node-good",
                baseline_id="storage-classify",
            )
            expected_verdict = classify_node(
                "storage",
                "node-bad",
                baseline,
                config=self.config,
                db_path=two_nodes,
                window_days=365,
            )
            actual_verdict = classify_compatibility_target(
                self.config,
                "storage",
                baseline,
                source_db=str(two_nodes),
                node="node-bad",
                window_days=365,
            )[0]
            self.assertEqual(actual_verdict, expected_verdict)

    def test_nccl_baseline_and_classification_match_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "nccl.db"
            _make_nccl_db(source)
            with patch("cval.baselines.build.time.time", return_value=NOW):
                expected = build.build_nccl_baseline(
                    config=self.config,
                    db_path=source,
                    window_days=365,
                    min_samples=5,
                    image_name="img:1",
                    node="node-a",
                    baseline_id="nccl-fixture",
                )
                actual = build_compatibility_baseline(
                    self.config,
                    "nccl",
                    source_db=str(source),
                    window_days=365,
                    min_samples=5,
                    image_name="img:1",
                    node="node-a",
                    baseline_id="nccl-fixture",
                )
            self.assertEqual(actual, expected)

            two_nodes = Path(tmpdir) / "nccl-two.db"
            _make_nccl_two_nodes(two_nodes)
            baseline = build.build_nccl_baseline(
                config=self.config,
                db_path=two_nodes,
                window_days=365,
                min_samples=5,
                node="node-good",
                baseline_id="nccl-classify",
            )
            expected_verdict = classify_node(
                "nccl",
                "node-bad",
                baseline,
                config=self.config,
                db_path=two_nodes,
                window_days=365,
            )
            actual_verdict = classify_compatibility_target(
                self.config,
                "nccl",
                baseline,
                source_db=str(two_nodes),
                node="node-bad",
                window_days=365,
            )[0]
            self.assertEqual(actual_verdict, expected_verdict)

    def test_four_dl_aliases_and_aggregate_match_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = {
                component: root / f"{component}.db"
                for component in (
                    "numerical_correctness",
                    "compute_performance",
                    "collective_performance",
                    "overlap_performance",
                )
            }
            for component, path in paths.items():
                self._make_dl_component_db(path, component)
            config = replace(
                self.config,
                storage=replace(
                    self.config.storage,
                    dl_numerical_db_path=str(paths["numerical_correctness"]),
                    dl_compute_db_path=str(paths["compute_performance"]),
                    dl_collective_db_path=str(paths["collective_performance"]),
                    dl_overlap_db_path=str(paths["overlap_performance"]),
                ),
            )
            baseline = self._dl_baseline()
            targets = (
                "dltest-numerical",
                "dltest-compute",
                "dltest-collective",
                "dltest-overlap",
                "dltest",
            )
            for target in targets:
                with self.subTest(target=target):
                    expected = classify_node(
                        target,
                        "node-a",
                        baseline,
                        config=config,
                        window_days=365,
                    )
                    actual = classify_compatibility_target(
                        config,
                        target,
                        baseline,
                        node="node-a",
                        window_days=365,
                    )[0]
                    self.assertEqual(actual, expected)
                    self.assertEqual(actual["status"], "degraded")

    def test_dl_verdict_validation_rejects_contradictory_derived_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = {
                component: root / f"{component}.db"
                for component in (
                    "numerical_correctness",
                    "compute_performance",
                    "collective_performance",
                    "overlap_performance",
                )
            }
            for component, path in paths.items():
                self._make_dl_component_db(path, component)
            config = replace(
                self.config,
                storage=replace(
                    self.config.storage,
                    dl_numerical_db_path=str(paths["numerical_correctness"]),
                    dl_compute_db_path=str(paths["compute_performance"]),
                    dl_collective_db_path=str(paths["collective_performance"]),
                    dl_overlap_db_path=str(paths["overlap_performance"]),
                ),
            )
            baseline = self._dl_baseline()
            verdict = classify_node(
                "dltest",
                "node-a",
                baseline,
                config=config,
                window_days=365,
            )
            target = resolve_operational_target(config, "dltest", BASELINE_CLASSIFY)
            validate_compatibility_classification_verdicts(
                [verdict],
                target=target,
                expected_baseline_id=baseline["baseline_id"],
            )

            malformed = []
            wrong_top_status = copy.deepcopy(verdict)
            wrong_top_status["status"] = "normal"
            malformed.append(("status", wrong_top_status))

            wrong_component_status = copy.deepcopy(verdict)
            wrong_component_status["components"]["numerical_correctness"]["status"] = "normal"
            malformed.append(("component status", wrong_component_status))

            wrong_component_count = copy.deepcopy(verdict)
            wrong_component_count["components"]["numerical_correctness"]["n_improved"] = 1
            malformed.append(("component n_improved", wrong_component_count))

            wrong_component_worst = copy.deepcopy(verdict)
            wrong_component_worst["components"]["numerical_correctness"][
                "worst_pct_diff"
            ] += 1.0
            malformed.append(("component worst", wrong_component_worst))

            wrong_component_threshold = copy.deepcopy(verdict)
            wrong_component_threshold["components"]["numerical_correctness"][
                "dl_degraded_severity_pct"
            ] += 1.0
            malformed.append(("component threshold", wrong_component_threshold))

            wrong_metric_threshold_flag = copy.deepcopy(verdict)
            wrong_metric_threshold_flag["dl_degraded_severity_pct"] = 200.0
            for summary in wrong_metric_threshold_flag["components"].values():
                summary["dl_degraded_severity_pct"] = 200.0
            malformed.append(("metric threshold flag", wrong_metric_threshold_flag))

            for label, value in malformed:
                with self.subTest(label=label), self.assertRaisesRegex(
                    ValueError,
                    "DL|Classification",
                ):
                    validate_compatibility_classification_verdicts(
                        [value],
                        target=target,
                        expected_baseline_id=baseline["baseline_id"],
                    )

    @staticmethod
    def _make_dl_component_db(path: Path, component: str) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                f"""
                CREATE TABLE {component} (
                    node TEXT NOT NULL,
                    cval_timestamp INTEGER NOT NULL,
                    test_plan TEXT NOT NULL,
                    task_group TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL,
                    run_key TEXT NOT NULL
                )
                """
            )
            connection.execute(
                f"INSERT INTO {component} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "node-a",
                    NOW,
                    "80gb-example",
                    "group",
                    component,
                    0,
                    "metric",
                    2.0,
                    "node-a-run",
                ),
            )
            connection.commit()

    @staticmethod
    def _dl_baseline() -> dict:
        metrics = {}
        for component in (
            "numerical_correctness",
            "compute_performance",
            "collective_performance",
            "overlap_performance",
        ):
            key = (
                f"group/{component}/rank0/metric"
                if component == "numerical_correctness"
                else f"group/{component}/metric"
            )
            direction = (
                "two_sided"
                if component in {"numerical_correctness", "overlap_performance"}
                else "high_bad"
            )
            metric = stats.summarize_metric(
                key,
                [1.0] * 8,
                direction=direction,
                tolerance_pct=10.0,
                bootstrap=False,
            ).to_dict()
            metric["source_table"] = component
            metrics[key] = metric
        return {
            "schema_version": "cval.baseline.v2",
            "baseline_id": "dl-all-components",
            "test_type": "dltest",
            "stratum_key": "",
            "window_days": 365,
            "created_at": NOW,
            "timestamp": NOW,
            "n_samples": 8,
            "method": "robust_mad",
            "metrics": metrics,
        }


if __name__ == "__main__":
    unittest.main()
