"""Byte-for-byte built-in CSV compatibility through U10 export hooks."""

from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cval.config import load_config
from cval.models import (
    ClassificationResultRow,
    LatestStatusRow,
    NcclHealthMetric,
    StorageMetrics,
)
from cval.storage.results_export import (
    write_export_rows_csv,
    write_latest_results_csv,
    write_nccl_health_results_csv,
)
from cval.validation.operational_targets import RESULTS_EXPORT
from cval.validation.operations import (
    export_evaluator_rows,
    resolve_operational_target,
)
from cval.validation.plugins import ExportContext


class OperationalExportCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config()
        self.now = dt.datetime(2026, 6, 18, 3, 0, 0, tzinfo=dt.timezone.utc)
        self.rows = (
            LatestStatusRow("node-a", "storage", 1781748000, "pass"),
            LatestStatusRow("node-a", "nccl", 1781748000, "pass"),
            LatestStatusRow("node-a", "dltest", 1781748000, "pass"),
        )
        self.classifications = (
            ClassificationResultRow(
                1781749000,
                "node-a",
                "storage",
                "storage-1",
                "normal",
                True,
                12,
                0,
                0,
                0,
                0.0,
                0.0,
            ),
            ClassificationResultRow(
                1781749000,
                "node-a",
                "nccl",
                "nccl-1",
                "normal",
                True,
                2,
                0,
                0,
                0,
                0.0,
                0.0,
            ),
            ClassificationResultRow(
                1781749000,
                "node-a",
                "dltest-compute",
                "dl-1",
                "degraded",
                False,
                100,
                12,
                0,
                12,
                0.12,
                20.0,
            ),
        )

    def _context(self, target_name: str) -> ExportContext:
        target = resolve_operational_target(self.config, target_name, RESULTS_EXPORT)
        registered = self.config.tests.registry.require(target.owner_test_id)
        return ExportContext(
            target=target,
            definition=registered.definition,
            config=self.config,
            status_rows=self.rows,
            classification_rows=self.classifications,
            pod="read-only-pod",
            namespace="read-only-namespace",
            source_db_paths=(("storage", "/read/storage.db"), ("nccl", "/read/nccl.db")),
            include_metrics=True,
        )

    def test_storage_csv_is_byte_for_byte_compatible(self) -> None:
        metrics = {
            "node-a": StorageMetrics(
                1000.0,
                512.0,
                900.0,
                460.0,
                2000.0,
                1024.0,
                1800.0,
                920.0,
                3000.0,
                1500.0,
                2500.0,
                1250.0,
            )
        }
        with patch(
            "cval.storage.metrics.get_latest_storage_metrics", return_value=metrics
        ):
            export = export_evaluator_rows(
                self.config, "storage", self._context("storage")
            )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            old = write_latest_results_csv(
                list(self.rows),
                "storage",
                output_dir=root / "old",
                now=self.now,
                classifications=list(self.classifications),
                storage_metrics=metrics,
            )
            new = write_export_rows_csv(
                export, "storage", output_dir=root / "new", now=self.now
            )
            self.assertEqual(old.read_bytes(), new.read_bytes())

    def test_nccl_wide_csv_is_byte_for_byte_compatible(self) -> None:
        metrics = {
            "node-a": NcclHealthMetric(
                "node-a",
                1781748000,
                "2026-06-17T20:20:00-07:00",
                20,
                "img",
                "13.0",
                "2.9.0",
                26,
                44.5,
                628.2,
                {"mlx5_0": 46.1, "mlx5_13": 46.3},
            )
        }
        with patch(
            "cval.storage.metrics.get_latest_nccl_health_metrics", return_value=metrics
        ):
            export = export_evaluator_rows(
                self.config, "nccl", self._context("nccl")
            )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            old = write_nccl_health_results_csv(
                list(self.rows),
                output_dir=root / "old",
                now=self.now,
                health_metrics=metrics,
                classifications=list(self.classifications),
            )
            new = write_export_rows_csv(
                export, "nccl", output_dir=root / "new", now=self.now
            )
            self.assertEqual(old.read_bytes(), new.read_bytes())

    def test_dl_alias_csv_is_byte_for_byte_compatible(self) -> None:
        export = export_evaluator_rows(
            self.config, "dltest-compute", self._context("dltest-compute")
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            old = write_latest_results_csv(
                list(self.rows),
                "dltest-compute",
                output_dir=root / "old",
                now=self.now,
                classifications=list(self.classifications),
            )
            new = write_export_rows_csv(
                export, "dltest-compute", output_dir=root / "new", now=self.now
            )
            self.assertEqual(old.read_bytes(), new.read_bytes())


if __name__ == "__main__":
    unittest.main()
