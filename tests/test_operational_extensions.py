"""End-to-end U10 compatibility extension tests with a synthetic metric plugin."""

from __future__ import annotations

import csv
import copy
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from cval.baselines.storage import (
    activate_baseline,
    default_dynamic_baseline_db_path,
    get_active_baseline,
    store_classification_results,
    store_dynamic_baseline,
)
from cval.config import load_config
from cval.models import ClassificationResultRow, LatestStatusRow
from cval.storage.results_export import write_export_rows_csv
from cval.validation.operational_targets import (
    BASELINE_BUILD,
    BASELINE_CLASSIFY,
    RESULTS_EXPORT,
    build_operational_target_catalog,
)
from cval.validation.operations import (
    build_evaluator_baseline,
    classify_evaluator_target,
    export_evaluator_rows,
    resolve_operational_target,
    validate_baseline_record,
    validate_classification_verdicts,
)
from cval.validation.plugins import ExportContext, ExportRows
from cval.validation.registry import ValidationTestRegistry, load_test_registry


SYNTHETIC_PLUGIN = r'''
from __future__ import annotations

import time

from cval.baselines import stats
from cval.storage.sqlite_uri import connect_sqlite_file
from cval.validation.plugins import ExportRows, export_rows_from_records

CVAL_PLUGIN_API = "cval.plugin.v1"

class SyntheticPlugin:
    plugin_id = "synthetic"
    capabilities = frozenset({"baseline", "export"})

    def build_baseline(self, context):
        if not context.source_db:
            raise ValueError("synthetic source DB is required")
        connection = connect_sqlite_file(context.source_db, mode="ro")
        try:
            query = "SELECT value FROM synthetic_metrics"
            params = ()
            if context.node:
                query += " WHERE node=?"
                params = (context.node,)
            values = [float(row[0]) for row in connection.execute(query, params)]
        finally:
            connection.close()
        metric = stats.summarize_metric(
            "throughput",
            values,
            direction=stats.DIRECTION_LOW_BAD,
            tolerance_pct=10.0,
            z_threshold=3.5,
        ).to_dict()
        metric["source_table"] = "synthetic_metrics"
        created_at = int(time.time())
        return {
            "schema_version": "cval.baseline.v2",
            "baseline_id": context.baseline_id or f"synthetic-all-{created_at}",
            "test_type": "synthetic",
            "stratum_key": f"node={context.node}" if context.node else "",
            "window_days": context.window_days,
            "created_at": created_at,
            "timestamp": created_at,
            "n_samples": len(values),
            "method": "robust_mad",
            "metrics": {"throughput": metric},
        }

    def classify(self, context, baseline):
        if not context.source_db:
            raise ValueError("synthetic source DB is required")
        connection = connect_sqlite_file(context.source_db, mode="ro")
        try:
            query = "SELECT node, value FROM synthetic_metrics"
            params = ()
            if context.node:
                query += " WHERE node=?"
                params = (context.node,)
            rows = connection.execute(query, params).fetchall()
        finally:
            connection.close()
        by_node = {}
        for node, value in rows:
            by_node.setdefault(str(node), []).append(float(value))
        verdicts = []
        metric = baseline["metrics"]["throughput"]
        for node in sorted(by_node):
            value = stats.median(by_node[node])
            status, pct_diff = stats.classify_value(value, metric)
            verdicts.append({
                "node": node,
                "test_type": context.target.name,
                "baseline_test_type": "synthetic",
                "dl_component": "",
                "baseline_id": baseline["baseline_id"],
                "status": status,
                "n_metrics": 1,
                "n_compared": 1,
                "n_degraded": int(status == "degraded"),
                "n_band_degraded": int(status == "degraded"),
                "n_improved": int(status == "improved"),
                "degraded_metric_fraction": 1.0 if status == "degraded" else 0.0,
                "degraded_metric_percent": 100.0 if status == "degraded" else 0.0,
                "worst_pct_diff": abs(float(pct_diff)) if status == "degraded" else 0.0,
                "metrics": [{
                    "metric": "throughput",
                    "component": "synthetic_metrics",
                    "value": value,
                    "median": metric["median"],
                    "status": status,
                    "pct_diff": pct_diff,
                    "abs_pct_diff": abs(float(pct_diff)),
                    "counts_for_degraded_status": False,
                    "direction": metric["direction"],
                    "lower_bound": metric["lower_bound"],
                    "upper_bound": metric["upper_bound"],
                }],
            })
        return tuple(verdicts)

    def export_rows(self, context):
        classifications = {
            (row.node, row.test_type): row for row in context.classification_rows
        }
        records = []
        for row in sorted(context.status_rows, key=lambda item: item.node):
            if row.test != context.target.status_test:
                continue
            classification = classifications.get((row.node, context.target.name))
            records.append({
                "node": row.node,
                "test": context.target.name,
                "result": row.result,
                "classification_status": classification.status if classification else "",
            })
        return export_rows_from_records(
            ("node", "test", "result", "classification_status"), records
        )

PLUGIN = SyntheticPlugin()
'''


class OperationalExtensionTests(unittest.TestCase):
    def _make_config(self, root: Path, *, enabled: bool = True):
        test_dir = root / "validation-tests" / "synthetic"
        test_dir.mkdir(parents=True)
        for name in ("setup.sh", "run-test.sh"):
            (test_dir / name).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "plugin.py").write_text(SYNTHETIC_PLUGIN, encoding="utf-8")
        (test_dir / "test_config.toml").write_text(
            f'''
schema_version = "cval.test.v1"

[test]
id = "synthetic"
display_name = "Synthetic metric"
order = 15
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30

[artifacts]
summary_filename = "summary.json"

[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = ["baseline", "export"]
''',
            encoding="utf-8",
        )
        registry = load_test_registry(
            {
                "synthetic": {
                    "enabled": enabled,
                    "config_path": "validation-tests/synthetic/test_config.toml",
                }
            },
            repo_root=root,
            include_defaults=False,
            require_enabled=enabled,
        )
        base = load_config()
        return replace(
            base,
            tests=replace(base.tests, registry=registry),
            baseline=replace(base.baseline, baseline_root_path=str(root / "baselines")),
            runtime=replace(base.runtime, validation_root=str(root / "validation-root")),
        )

    @staticmethod
    def _make_source(path: Path) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "CREATE TABLE synthetic_metrics (node TEXT NOT NULL, value REAL NOT NULL)"
            )
            connection.executemany(
                "INSERT INTO synthetic_metrics VALUES (?, ?)",
                [
                    *(('node-good', 100.0) for _ in range(8)),
                    *(('node-bad', 50.0) for _ in range(4)),
                ],
            )
            connection.commit()

    def test_synthetic_target_build_classify_store_and_export_without_core_edit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._make_config(root)
            source_db = root / "synthetic.db"
            self._make_source(source_db)
            catalog = build_operational_target_catalog(config.tests.registry)

            self.assertEqual(catalog.names_for(BASELINE_BUILD), ("synthetic",))
            self.assertEqual(catalog.names_for(BASELINE_CLASSIFY), ("synthetic",))
            self.assertEqual(catalog.names_for(RESULTS_EXPORT), ("synthetic",))
            baseline_path = default_dynamic_baseline_db_path("synthetic", config=config)
            self.assertEqual(
                baseline_path,
                root / "baselines/plugin-synthetic-baselines.db",
            )
            self.assertFalse(baseline_path.exists())

            baseline = build_evaluator_baseline(
                config,
                "synthetic",
                window_days=30,
                min_samples=3,
                source_db=str(source_db),
                node="node-good",
                baseline_id="synthetic-good-1",
            )
            self.assertFalse(baseline_path.exists(), "read-only build must not create DBs")
            store_dynamic_baseline(baseline, config=config)
            self.assertTrue(activate_baseline("synthetic-good-1", "synthetic", config=config))
            active = get_active_baseline("synthetic", config=config)
            self.assertEqual(active["baseline_id"], baseline["baseline_id"])
            self.assertEqual(active["metrics"], baseline["metrics"])

            verdicts = classify_evaluator_target(
                config,
                "synthetic",
                active,
                window_days=30,
                source_db=str(source_db),
            )
            self.assertEqual(
                {row["node"]: row["status"] for row in verdicts},
                {"node-bad": "degraded", "node-good": "normal"},
            )
            classification_db = root / "baselines/synthetic-classifications.db"
            self.assertEqual(
                store_classification_results(
                    verdicts,
                    db_path=classification_db,
                    classified_at=123,
                    config=config,
                ),
                2,
            )

            target = resolve_operational_target(config, "synthetic", RESULTS_EXPORT)
            registered = config.tests.registry.require("synthetic")
            classifications = tuple(
                ClassificationResultRow(
                    123,
                    verdict["node"],
                    "synthetic",
                    "synthetic-good-1",
                    verdict["status"],
                    verdict["status"] != "degraded",
                    verdict["n_compared"],
                    verdict["n_degraded"],
                    verdict["n_improved"],
                    verdict["n_band_degraded"],
                    verdict["degraded_metric_fraction"],
                    verdict["worst_pct_diff"],
                )
                for verdict in verdicts
            )
            context = ExportContext(
                target=target,
                definition=registered.definition,
                config=config,
                status_rows=(
                    LatestStatusRow("node-good", "synthetic", 100, "pass"),
                    LatestStatusRow("node-bad", "synthetic", 100, "pass"),
                    LatestStatusRow("node-other", "other", 100, "pass"),
                ),
                classification_rows=classifications,
                pod="unused-read-only-pod",
                namespace="unused-read-only-namespace",
                source_db_paths=(("synthetic", str(source_db)),),
                include_metrics=False,
            )
            exported = export_evaluator_rows(config, "synthetic", context)
            self.assertIsInstance(exported, ExportRows)
            self.assertEqual(exported.columns, ("node", "test", "result", "classification_status"))
            self.assertEqual(len(exported.rows), 2)
            output_path = write_export_rows_csv(
                exported,
                "synthetic",
                output_dir=root / "exports",
            )
            with output_path.open(newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))
            self.assertEqual(csv_rows[0], list(exported.columns))
            self.assertEqual(csv_rows[1][0], "node-bad")
            self.assertTrue(output_path.name.startswith("cval_synthetic_"))

    def test_disabled_synthetic_target_cannot_be_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(Path(tmpdir), enabled=False)
            self.assertEqual(
                build_operational_target_catalog(config.tests.registry).targets, ()
            )
            with self.assertRaisesRegex(ValueError, "not enabled"):
                resolve_operational_target(config, "synthetic", RESULTS_EXPORT)

    def test_strict_baseline_and_classification_validation_rejects_malformed_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._make_config(root)
            source_db = root / "synthetic.db"
            self._make_source(source_db)
            baseline = build_evaluator_baseline(
                config,
                "synthetic",
                window_days=30,
                min_samples=3,
                source_db=str(source_db),
                node="node-good",
                baseline_id="synthetic-good-1",
            )
            verdicts = classify_evaluator_target(
                config,
                "synthetic",
                baseline,
                window_days=30,
                source_db=str(source_db),
            )
            target = resolve_operational_target(config, "synthetic", BASELINE_CLASSIFY)

            malformed_baselines = []
            unknown = copy.deepcopy(baseline)
            unknown["unexpected"] = True
            malformed_baselines.append(unknown)
            wrong_identity = copy.deepcopy(baseline)
            wrong_identity["metrics"]["throughput"]["metric"] = "other"
            malformed_baselines.append(wrong_identity)
            bool_count = copy.deepcopy(baseline)
            bool_count["n_samples"] = True
            malformed_baselines.append(bool_count)
            non_finite = copy.deepcopy(baseline)
            non_finite["metrics"]["throughput"]["median"] = float("nan")
            malformed_baselines.append(non_finite)
            inverted = copy.deepcopy(baseline)
            inverted["metrics"]["throughput"]["p05"] = 999.0
            malformed_baselines.append(inverted)
            for malformed in malformed_baselines:
                with self.subTest(kind="baseline", value=malformed), self.assertRaises(
                    (TypeError, ValueError)
                ):
                    validate_baseline_record(
                        malformed,
                        expected_test_type="synthetic",
                    )

            malformed_verdicts = []
            unknown_verdict = copy.deepcopy(verdicts[0])
            unknown_verdict["unexpected"] = 1
            malformed_verdicts.append(unknown_verdict)
            wrong_baseline = copy.deepcopy(verdicts[0])
            wrong_baseline["baseline_id"] = "other"
            malformed_verdicts.append(wrong_baseline)
            bad_count = copy.deepcopy(verdicts[0])
            bad_count["n_compared"] = 2
            malformed_verdicts.append(bad_count)
            bad_fraction = copy.deepcopy(verdicts[0])
            bad_fraction["degraded_metric_fraction"] = 0.5
            malformed_verdicts.append(bad_fraction)
            bad_distance = copy.deepcopy(verdicts[0])
            bad_distance["metrics"][0]["abs_pct_diff"] = float("inf")
            malformed_verdicts.append(bad_distance)
            for malformed in malformed_verdicts:
                with self.subTest(kind="verdict", value=malformed), self.assertRaises(
                    (TypeError, ValueError)
                ):
                    validate_classification_verdicts(
                        [malformed],
                        target=target,
                        expected_baseline_id="synthetic-good-1",
                    )

            malformed_store = copy.deepcopy(verdicts)
            malformed_store[0]["n_compared"] = 99
            with self.assertRaisesRegex(ValueError, "n_compared"):
                store_classification_results(
                    malformed_store,
                    db_path=root / "classifications.db",
                    config=config,
                )
            self.assertFalse((root / "classifications.db").exists())

    def test_export_contract_rejects_mutable_or_non_rectangular_rows(self) -> None:
        with self.assertRaisesRegex(TypeError, "rows must be a tuple"):
            ExportRows(("node",), [])  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "same-width"):
            ExportRows(("node", "result"), (("node-a",),))
        with self.assertRaisesRegex(ValueError, "unique safe"):
            ExportRows(("node", "node"), (("node-a", "node-a"),))


if __name__ == "__main__":
    unittest.main()
