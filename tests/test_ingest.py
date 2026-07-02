from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cval.storage.ingest import (
    add_nccl_ib_port_results,
    add_nccl_ib_ports_from_summary,
    add_nccl_result,
    add_storage_result,
    add_validation_result,
)

class IngestTests(unittest.TestCase):
    def test_add_validation_result_writes_run_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"

            timestamp = add_validation_result(
                "slc01-cl02-hgx-0001",
                "storage",
                "pass",
                "12345",
                image_name="pytorch:26.05-py3",
                pytorch_version="2.8.0a0+abc123",
                cuda_version="12.9",
                db_path=db_path,
            )

            self.assertEqual(timestamp, 12345)
            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, test, timestamp, result, image_name, pytorch_version, cuda_version FROM runs"
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "slc01-cl02-hgx-0001",
                    "storage",
                    12345,
                    "pass",
                    "pytorch:26.05-py3",
                    "2.8.0a0+abc123",
                    "12.9",
                ),
            )

    def test_add_validation_result_migrates_existing_runs_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE runs ("
                    "node TEXT NOT NULL, "
                    "test TEXT NOT NULL, "
                    "timestamp INTEGER NOT NULL, "
                    "result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete'))"
                    ")"
                )
                connection.commit()

            add_validation_result(
                "slc01-cl02-hgx-0001",
                "all",
                "pass",
                "12345",
                image_name="pytorch:26.05-py3",
                pytorch_version="2.8.0a0+abc123",
                cuda_version="12.9",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, test, timestamp, result, image_name, pytorch_version, cuda_version FROM runs"
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "slc01-cl02-hgx-0001",
                    "all",
                    12345,
                    "pass",
                    "pytorch:26.05-py3",
                    "2.8.0a0+abc123",
                    "12.9",
                ),
            )

    def test_add_storage_result_parses_fio_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            (results_dir / "randread.json").write_text(
                json.dumps({"jobs": [{"read": {"iops": 10, "bw": 20}, "write": {}}]}),
                encoding="utf-8",
            )
            db_path = root / "storage.db"

            add_storage_result(
                "slc01-cl02-hgx-0001",
                12345,
                results_dir,
                image_name="pytorch:26.05-py3",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, timestamp, image_name, randread_iops, randread_bw "
                    "FROM storage_performance"
                ).fetchone()
            self.assertEqual(
                row,
                ("slc01-cl02-hgx-0001", 12345, "pytorch:26.05-py3", 10.0, 20.0),
            )

    def test_add_nccl_result_writes_metric_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"

            add_nccl_result(
                "slc01-cl02-hgx-0001",
                "12345",
                "44.7",
                "626.1",
                image_name="pytorch:26.05-py3",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, timestamp, image_name, busbw, latency FROM nccl_performance"
                ).fetchone()
            self.assertEqual(
                row,
                ("slc01-cl02-hgx-0001", 12345, "pytorch:26.05-py3", 44.7, 626.1),
            )


class NcclIbPortIngestTests(unittest.TestCase):
    def test_add_nccl_ib_port_results_writes_one_row_per_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            ports = {
                "mlx5_0": {"avg_gbps": 0.0, "max_gbps": 0.0, "last_gbps": 0.0, "samples": 26},
                "mlx5_4": {"avg_gbps": 20.285, "max_gbps": 46.236, "last_gbps": 46.1, "samples": 26},
                "mlx5_5.2": {"avg_gbps": 12.0, "max_gbps": 24.0, "last_gbps": 23.0, "samples": 26},
            }

            add_nccl_ib_port_results(
                "slc01-cl02-hgx-0001",
                "12345",
                ports,
                image_name="pytorch:26.05-py3",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT node, timestamp, device, avg_gbps, max_gbps, samples "
                    "FROM nccl_ib_port_performance ORDER BY device"
                ).fetchall()

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1], ("slc01-cl02-hgx-0001", 12345, "mlx5_4", 20.285, 46.236, 26))
        self.assertEqual(rows[2][2], "mlx5_5.2")

    def test_add_nccl_ib_ports_from_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            summary = Path(tmpdir) / "nccl-summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_BUSBW": 44.5,
                        "GCR_IB_PORT_BW_GBPS": {
                            "mlx5_4": {"avg_gbps": 20.3, "max_gbps": 46.2, "last_gbps": 46.0, "samples": 20},
                        },
                    }
                ),
                encoding="utf-8",
            )

            add_nccl_ib_ports_from_summary(
                "node-a", "12345", summary, image_name="img", db_path=db_path
            )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, device, avg_gbps, samples FROM nccl_ib_port_performance"
                ).fetchone()
        self.assertEqual(row, ("node-a", "mlx5_4", 20.3, 20))

    def test_add_nccl_ib_ports_from_summary_missing_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            summary = Path(tmpdir) / "nccl-summary.json"
            summary.write_text(json.dumps({"GCR_BUSBW": 44.5}), encoding="utf-8")

            # No GCR_IB_PORT_BW_GBPS block: writes zero rows but must not raise.
            add_nccl_ib_ports_from_summary("node-a", "12345", summary, db_path=db_path)

            with closing(sqlite3.connect(db_path)) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM nccl_ib_port_performance"
                ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()