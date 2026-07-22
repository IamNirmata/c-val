from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cval.storage.ingest import (
    NCCL_IB_PORT_COLUMNS,
    add_nccl_health_from_summary,
    add_nccl_result,
    add_storage_result,
    add_validation_result,
    migrate_nccl_health,
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


class NcclHealthIngestTests(unittest.TestCase):
    def test_add_nccl_health_writes_one_wide_row_with_port_maxima(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            summary = Path(tmpdir) / "nccl-summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_LATENCY": 628.2,
                        "GCR_IB_PORT_BW_GBPS": {
                            "mlx5_0": {
                                "avg_gbps": 20.0,
                                "max_gbps": 46.1,
                                "last_gbps": 45.9,
                                "samples": 26,
                            },
                            "mlx5_13": {
                                "avg_gbps": 20.2,
                                "max_gbps": 46.3,
                                "last_gbps": 46.0,
                                "samples": 26,
                            },
                            "mlx5_5.2": {"max_gbps": 99.0, "samples": 26},
                        },
                    }
                ),
                encoding="utf-8",
            )

            add_nccl_health_from_summary(
                "slc01-cl02-hgx-0001",
                "12345",
                summary,
                image_name="pytorch:26.05-py3",
                cuda_version="13.0",
                pytorch_version="2.9.0",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                columns = [row[1] for row in connection.execute("PRAGMA table_info(IB_HEALTH)")]
                row = connection.execute(
                    "SELECT Node, timestamp, la_timestamp, iterations, image_name, cuda, pytorch, "
                    "samples, BUS_BW, LATENCY, mlx5_0, mlx5_13 FROM IB_HEALTH"
                ).fetchone()

        self.assertEqual(columns[-14:], list(NCCL_IB_PORT_COLUMNS))
        self.assertEqual(row[0:2], ("slc01-cl02-hgx-0001", 12345))
        self.assertIn("-08:00", row[2])
        self.assertEqual(row[3:10], (20, "pytorch:26.05-py3", "13.0", "2.9.0", 26, 44.5, 628.2))
        self.assertEqual(row[10:], (46.1, 46.3))

    def test_migrate_legacy_tables_to_one_row_per_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            validation_db = Path(tmpdir) / "validation.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.executescript(
                    """
                    CREATE TABLE nccl_performance (
                        node TEXT, timestamp INTEGER, image_name TEXT, busbw REAL, latency REAL
                    );
                    CREATE TABLE nccl_ib_port_performance (
                        node TEXT, timestamp INTEGER, device TEXT, image_name TEXT,
                        avg_gbps REAL, max_gbps REAL, last_gbps REAL, samples INTEGER
                    );
                    INSERT INTO nccl_performance VALUES ('node-a', 12345, 'img', 44.5, 628.2);
                    INSERT INTO nccl_performance VALUES ('node-a', 12346, 'img', 45.0, 620.0);
                    INSERT INTO nccl_ib_port_performance
                        VALUES ('node-a', 12345, 'mlx5_0', '', 20.0, 46.1, 45.9, 26);
                    INSERT INTO nccl_ib_port_performance
                        VALUES ('node-a', 12345, 'mlx5_13', '', 20.2, 46.3, 46.0, 26);
                    """
                )
                connection.commit()
            with closing(sqlite3.connect(validation_db)) as connection:
                connection.execute(
                    "CREATE TABLE runs (node TEXT, test TEXT, timestamp INTEGER, result TEXT, "
                    "image_name TEXT, pytorch_version TEXT, cuda_version TEXT)"
                )
                connection.execute(
                    "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("node-a", "nccl", 12345, "pass", "img", "2.9.0", "13.0"),
                )
                connection.commit()

            summary = migrate_nccl_health(
                db_path, validation_db_path=validation_db, default_iterations=20
            )

            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT Node, timestamp, iterations, cuda, pytorch, samples, BUS_BW, "
                    "LATENCY, mlx5_0, mlx5_13 FROM IB_HEALTH ORDER BY timestamp"
                ).fetchall()
                legacy_count = connection.execute(
                    "SELECT COUNT(*) FROM nccl_ib_port_performance"
                ).fetchone()[0]

        self.assertEqual(summary, {"migrated_runs": 2, "total_rows": 2, "rows_with_ports": 1})
        self.assertEqual(rows[0], ("node-a", 12345, 20, "13.0", "2.9.0", 26, 44.5, 628.2, 46.1, 46.3))
        self.assertEqual(rows[1][0:3], ("node-a", 12346, 20))
        self.assertEqual(legacy_count, 2)  # additive migration keeps rollback data


if __name__ == "__main__":
    unittest.main()