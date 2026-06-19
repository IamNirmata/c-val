"""Tests for exporting latest c-val results to CSV."""

from __future__ import annotations

import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.models import ClassificationResultRow, LatestStatusRow
from cval.storage.classification_status import (
    classification_rows_to_csv_records,
    filter_classification_rows,
    write_classifications_csv,
)
from cval.storage.results_export import (
    default_results_filename,
    latest_result_rows,
    normalize_result_test,
    rows_to_csv_records,
    write_latest_results_csv,
)


class ResultsExportTests(unittest.TestCase):
    def test_normalize_overall_to_all(self) -> None:
        self.assertEqual(normalize_result_test("overall"), "all")
        self.assertEqual(normalize_result_test("all"), "all")
        self.assertEqual(normalize_result_test("storage"), "storage")

    def test_latest_result_rows_filters_and_sorts(self) -> None:
        rows = [
            LatestStatusRow("node-b", "storage", 200, "pass"),
            LatestStatusRow("node-a", "storage", 100, "fail"),
            LatestStatusRow("node-a", "nccl", 100, "pass"),
        ]

        selected = latest_result_rows(rows, "storage")

        self.assertEqual([row.node for row in selected], ["node-a", "node-b"])

    def test_filename_uses_los_angeles_time(self) -> None:
        # 2026-06-18 03:00 UTC is 2026-06-17 20:00 PDT.
        now = dt.datetime(2026, 6, 18, 3, 0, 0, tzinfo=dt.timezone.utc)

        filename = default_results_filename("overall", now)

        self.assertEqual(filename, "cval_overall_20260617_200000_PDT.csv")

    def test_rows_to_csv_records_include_utc_and_la_time(self) -> None:
        rows = [LatestStatusRow("node-a", "all", 1781748000, "pass")]

        records = rows_to_csv_records(rows, "overall")

        self.assertEqual(records[0]["test"], "overall")
        self.assertEqual(records[0]["db_test"], "all")
        self.assertIn("+00:00", records[0]["latest_time_utc"])
        self.assertIn("-07:00", records[0]["latest_time_los_angeles"])

    def test_rows_to_csv_records_join_classification(self) -> None:
        rows = [LatestStatusRow("node-a", "dltest", 1781748000, "pass")]
        classifications = [
            ClassificationResultRow(
                classified_at=1781749000,
                node="node-a",
                test_type="dltest-compute",
                baseline_id="dl-1",
                status="degraded",
                passed=False,
                n_compared=100,
                n_degraded=12,
                n_improved=0,
                n_band_degraded=20,
                degraded_metric_fraction=0.12,
                worst_pct_diff=15.5,
            )
        ]

        records = rows_to_csv_records(rows, "dltest-compute", classifications)

        self.assertEqual(records[0]["result"], "pass")
        self.assertEqual(records[0]["classification_status"], "degraded")
        self.assertEqual(records[0]["classification_passed"], "false")
        self.assertEqual(records[0]["n_degraded"], "12")
        self.assertEqual(records[0]["degraded_metric_percent"], "12.000")

    def test_write_latest_results_csv(self) -> None:
        rows = [
            LatestStatusRow("node-a", "all", 1781748000, "pass"),
            LatestStatusRow("node-b", "storage", 1781749000, "fail"),
        ]
        now = dt.datetime(2026, 6, 18, 3, 0, 0, tzinfo=dt.timezone.utc)

        with TemporaryDirectory() as tmpdir:
            path = write_latest_results_csv(rows, "overall", output_dir=tmpdir, now=now)
            text = Path(path).read_text(encoding="utf-8")

        self.assertTrue(str(path).endswith("cval_overall_20260617_200000_PDT.csv"))
        self.assertIn("node,test,db_test,latest_timestamp", text)
        self.assertIn("node-a,overall,all,1781748000", text)
        self.assertNotIn("node-b", text)

    def test_classification_csv_helpers(self) -> None:
        rows = [
            ClassificationResultRow(100, "node-a", "storage", "s1", "normal", True, 12, 0, 0, 0, 0.0, 0.0),
            ClassificationResultRow(200, "node-b", "nccl", "n1", "degraded", False, 2, 1, 0, 1, 0.5, 20.0),
        ]

        selected = filter_classification_rows(rows, "nccl")
        records = classification_rows_to_csv_records(selected)

        self.assertEqual([row.node for row in selected], ["node-b"])
        self.assertEqual(records[0]["degraded_metric_percent"], "50.000")
        with TemporaryDirectory() as tmpdir:
            path = write_classifications_csv(rows, "nccl", output_dir=tmpdir)
            text = Path(path).read_text(encoding="utf-8")
        self.assertIn("node-b,nccl,200", text)
        self.assertNotIn("node-a", text)


if __name__ == "__main__":
    unittest.main()
