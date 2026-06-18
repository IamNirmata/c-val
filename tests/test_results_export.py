"""Tests for exporting latest c-val results to CSV."""

from __future__ import annotations

import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.models import LatestStatusRow
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


if __name__ == "__main__":
    unittest.main()
