"""Tests for exporting latest c-val results to CSV."""

from __future__ import annotations

import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.models import LatestStatusRow, NcclHealthMetric, NcclMetrics, StorageMetrics
from cval.storage.metrics import _parse_nccl_health_json, _parse_nccl_json, _parse_storage_json
from cval.storage.results_export import (
    CSV_BASE_COLUMNS,
    NCCL_HEALTH_CSV_COLUMNS,
    Nccl_EXTRA_COLUMNS,
    STORAGE_EXTRA_COLUMNS,
    default_results_filename,
    get_csv_columns,
    latest_result_rows,
    nccl_health_rows_to_csv_records,
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

    def test_get_csv_columns_nccl(self) -> None:
        cols = get_csv_columns("nccl")
        self.assertIn("nccl_busbw", cols)
        self.assertIn("nccl_latency", cols)
        self.assertNotIn("randread_iops", cols)

    def test_get_csv_columns_storage(self) -> None:
        cols = get_csv_columns("storage")
        self.assertIn("randread_iops", cols)
        self.assertIn("iodepth_write_1file_bw", cols)
        self.assertNotIn("nccl_busbw", cols)

    def test_get_csv_columns_overall_has_both(self) -> None:
        cols = get_csv_columns("overall")
        self.assertIn("nccl_busbw", cols)
        self.assertIn("randread_iops", cols)

    def test_get_csv_columns_dltest_base_only(self) -> None:
        cols = get_csv_columns("dltest")
        self.assertEqual(
            cols,
            (
                "node",
                "test",
                "db_test",
                "latest_timestamp",
                "latest_time_utc",
                "latest_time_los_angeles",
                "result",
            ),
        )
        self.assertEqual(cols, CSV_BASE_COLUMNS)

    def test_rows_to_csv_records_nccl_metrics(self) -> None:
        rows = [LatestStatusRow("node-a", "nccl", 1781748000, "pass")]
        nccl = {"node-a": NcclMetrics(busbw=195.3, latency=23.7)}

        records = rows_to_csv_records(rows, "nccl", nccl_metrics=nccl)

        self.assertEqual(records[0]["nccl_busbw"], "195.3000")
        self.assertEqual(records[0]["nccl_latency"], "23.7000")

    def test_rows_to_csv_records_nccl_metrics_missing_node(self) -> None:
        rows = [LatestStatusRow("node-z", "nccl", 1781748000, "pass")]
        nccl = {"node-a": NcclMetrics(busbw=195.3, latency=23.7)}

        records = rows_to_csv_records(rows, "nccl", nccl_metrics=nccl)

        self.assertEqual(records[0]["nccl_busbw"], "")
        self.assertEqual(records[0]["nccl_latency"], "")

    def test_rows_to_csv_records_storage_metrics(self) -> None:
        rows = [LatestStatusRow("node-a", "storage", 1781748000, "pass")]
        storage = {
            "node-a": StorageMetrics(
                iodepth_read_1file_iops=1000.0, iodepth_read_1file_bw=512.0,
                iodepth_write_1file_iops=900.0, iodepth_write_1file_bw=460.0,
                numjobs_read_nfiles_iops=2000.0, numjobs_read_nfiles_bw=1024.0,
                numjobs_write_nfiles_iops=1800.0, numjobs_write_nfiles_bw=920.0,
                randread_iops=3000.0, randread_bw=1500.0,
                randwrite_iops=2500.0, randwrite_bw=1250.0,
            )
        }

        records = rows_to_csv_records(rows, "storage", storage_metrics=storage)

        self.assertEqual(records[0]["randread_iops"], "3000.0000")
        self.assertEqual(records[0]["iodepth_write_1file_bw"], "460.0000")

    def test_write_nccl_csv_has_metric_columns(self) -> None:
        rows = [LatestStatusRow("node-a", "nccl", 1781748000, "pass")]
        nccl = {"node-a": NcclMetrics(busbw=195.3, latency=23.7)}
        now = dt.datetime(2026, 6, 18, 3, 0, 0, tzinfo=dt.timezone.utc)

        with TemporaryDirectory() as tmpdir:
            path = write_latest_results_csv(
                rows, "nccl", output_dir=tmpdir, now=now, nccl_metrics=nccl
            )
            text = Path(path).read_text(encoding="utf-8")

        self.assertIn("nccl_busbw,nccl_latency", text)
        self.assertIn("195.3000", text)
        self.assertIn("23.7000", text)

    def test_parse_nccl_json(self) -> None:
        raw = '[{"node": "node-a", "busbw": 195.3, "latency": 23.7}]'
        result = _parse_nccl_json(raw)
        self.assertIn("node-a", result)
        self.assertAlmostEqual(result["node-a"].busbw, 195.3)
        self.assertAlmostEqual(result["node-a"].latency, 23.7)

    def test_parse_nccl_json_empty(self) -> None:
        self.assertEqual(_parse_nccl_json("[]"), {})
        self.assertEqual(_parse_nccl_json(""), {})
        self.assertEqual(_parse_nccl_json("invalid json"), {})

    def test_parse_storage_json(self) -> None:
        raw = (
            '[{"node": "node-a", "iodepth_read_1file_iops": 1000.0, "iodepth_read_1file_bw": 512.0,'
            ' "iodepth_write_1file_iops": 900.0, "iodepth_write_1file_bw": 460.0,'
            ' "numjobs_read_nfiles_iops": 2000.0, "numjobs_read_nfiles_bw": 1024.0,'
            ' "numjobs_write_nfiles_iops": 1800.0, "numjobs_write_nfiles_bw": 920.0,'
            ' "randread_iops": 3000.0, "randread_bw": 1500.0,'
            ' "randwrite_iops": 2500.0, "randwrite_bw": 1250.0}]'
        )
        result = _parse_storage_json(raw)
        self.assertIn("node-a", result)
        self.assertAlmostEqual(result["node-a"].randread_iops, 3000.0)
        self.assertAlmostEqual(result["node-a"].iodepth_write_1file_bw, 460.0)

    def test_nccl_health_records_one_wide_row_per_node(self) -> None:
        rows = [LatestStatusRow("node-a", "nccl", 1781748000, "pass")]
        health = {
            "node-a": NcclHealthMetric(
                node="node-a",
                timestamp=1781748000,
                la_timestamp="2026-06-17T20:20:00-07:00",
                iterations=20,
                image_name="pytorch:26.05-py3",
                cuda="13.0",
                pytorch="2.9.0",
                samples=26,
                bus_bw=44.5,
                latency=628.2,
                port_max_gbps={"mlx5_0": 46.1, "mlx5_13": 46.3},
            )
        }

        records = nccl_health_rows_to_csv_records(rows, health)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["iterations"], "20")
        self.assertEqual(records[0]["BUS_BW"], "44.5000")
        self.assertEqual(records[0]["LATENCY"], "628.2000")
        self.assertEqual(records[0]["mlx5_0"], "46.1000")
        self.assertEqual(records[0]["mlx5_13"], "46.3000")
        self.assertEqual(records[0]["mlx5_1"], "")

    def test_nccl_health_records_node_without_health_row(self) -> None:
        rows = [LatestStatusRow("node-z", "nccl", 1781748000, "pass")]

        records = nccl_health_rows_to_csv_records(rows, {})

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["BUS_BW"], "")
        self.assertEqual(records[0]["mlx5_0"], "")

    def test_nccl_health_records_use_raw_health_schema(self) -> None:
        rows = [LatestStatusRow("node-a", "nccl", 1781748000, "pass")]
        health = {
            "node-a": NcclHealthMetric(
                "node-a", 1781748000, "2026-06-17T20:20:00-07:00", 20,
                "img", "13.0", "2.9.0", 26, 44.5, 628.2,
                {"mlx5_0": 46.1, "mlx5_13": 46.3},
            )
        }
        records = nccl_health_rows_to_csv_records(rows, health)

        self.assertEqual(NCCL_HEALTH_CSV_COLUMNS[0], "node")
        self.assertEqual(records[0]["BUS_BW"], "44.5000")
        self.assertEqual(records[0]["LATENCY"], "628.2000")

    def test_parse_nccl_health_json(self) -> None:
        raw = (
            '[{"node":"node-a","timestamp":1781748000,'
            '"la_timestamp":"2026-06-17T20:20:00-07:00","iterations":20,'
            '"image_name":"img","cuda":"13.0","pytorch":"2.9.0","samples":26,'
            '"bus_bw":44.5,"latency":628.2,"mlx5_0":46.1,"mlx5_13":46.3}]'
        )
        result = _parse_nccl_health_json(raw)
        self.assertIn("node-a", result)
        self.assertEqual(result["node-a"].iterations, 20)
        self.assertAlmostEqual(result["node-a"].bus_bw, 44.5)
        self.assertAlmostEqual(result["node-a"].port_max_gbps["mlx5_13"], 46.3)

    def test_parse_nccl_health_json_empty(self) -> None:
        self.assertEqual(_parse_nccl_health_json("[]"), {})
        self.assertEqual(_parse_nccl_health_json(""), {})
        self.assertEqual(_parse_nccl_health_json("bad json"), {})



if __name__ == "__main__":
    unittest.main()
