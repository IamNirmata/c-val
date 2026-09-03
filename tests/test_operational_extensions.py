"""End-to-end raw export extension tests with a synthetic plugin."""

from __future__ import annotations

import csv
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cval.config import load_config
from cval.models import LatestStatusRow
from cval.storage.results_export import write_export_rows_csv
from cval.validation.operational_targets import (
    RESULTS_EXPORT,
    build_operational_target_catalog,
)
from cval.validation.operations import (
    export_result_rows,
    resolve_operational_target,
)
from cval.validation.plugins import ExportContext, ExportRows
from cval.validation.registry import ValidationTestRegistry, load_test_registry


SYNTHETIC_PLUGIN = r'''
from __future__ import annotations

from cval.validation.plugins import ExportRows, export_rows_from_records

CVAL_PLUGIN_API = "cval.plugin.v1"

class SyntheticPlugin:
    plugin_id = "synthetic"
    capabilities = frozenset({"export"})

    def export_rows(self, context):
        records = []
        for row in sorted(context.status_rows, key=lambda item: item.node):
            if row.test != context.target.status_test:
                continue
            records.append({
                "node": row.node,
                "test": context.target.name,
                "result": row.result,
            })
        return export_rows_from_records(
            ("node", "test", "result"), records
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
capabilities = ["export"]
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
            runtime=replace(base.runtime, validation_root=str(root / "validation-root")),
        )

    def test_synthetic_target_exports_without_core_edit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._make_config(root)
            catalog = build_operational_target_catalog(config.tests.registry)

            self.assertEqual(catalog.names_for(RESULTS_EXPORT), ("synthetic",))
            target = resolve_operational_target(config, "synthetic", RESULTS_EXPORT)
            registered = config.tests.registry.require("synthetic")
            context = ExportContext(
                target=target,
                definition=registered.definition,
                config=config,
                status_rows=(
                    LatestStatusRow("node-good", "synthetic", 100, "pass"),
                    LatestStatusRow("node-bad", "synthetic", 100, "pass"),
                    LatestStatusRow("node-other", "other", 100, "pass"),
                ),
                pod="unused-read-only-pod",
                namespace="unused-read-only-namespace",
                source_db_paths=(),
                include_metrics=False,
            )
            exported = export_result_rows(config, "synthetic", context)
            self.assertIsInstance(exported, ExportRows)
            self.assertEqual(exported.columns, ("node", "test", "result"))
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

    def test_export_contract_rejects_mutable_or_non_rectangular_rows(self) -> None:
        with self.assertRaisesRegex(TypeError, "rows must be a tuple"):
            ExportRows(("node",), [])  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "same-width"):
            ExportRows(("node", "result"), (("node-a",),))
        with self.assertRaisesRegex(ValueError, "unique safe"):
            ExportRows(("node", "node"), (("node-a", "node-a"),))


if __name__ == "__main__":
    unittest.main()
