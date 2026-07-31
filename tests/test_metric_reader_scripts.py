"""Integration tests for the Python metric readers injected into the PVC pod."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cval.storage.ingest import NCCL_IB_PORT_COLUMNS
from cval.storage.metrics import (
    STORAGE_METRIC_FIELDS,
    _NCCL_FETCH_SCRIPT,
    _NCCL_HEALTH_FETCH_SCRIPT,
    _STORAGE_FETCH_SCRIPT,
)


def _run_script(script: str, db_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-", str(db_path)],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def _storage_script() -> str:
    return _STORAGE_FETCH_SCRIPT.replace(
        "__CVAL_STORAGE_COLUMNS__", repr(list(STORAGE_METRIC_FIELDS))
    )


def _wide_nccl_script() -> str:
    return _NCCL_HEALTH_FETCH_SCRIPT.replace(
        "__CVAL_NCCL_PORT_COLUMNS__", repr(list(NCCL_IB_PORT_COLUMNS))
    )


class GeneratedMetricReaderIntegrationTests(unittest.TestCase):
    def test_generated_readers_execute_against_populated_databases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nccl_db = root / "nccl metrics ?#%.db"
            storage_db = root / "storage metrics ?#%.db"
            self._populate_nccl(nccl_db)
            self._populate_storage(storage_db)

            compact = _run_script(_NCCL_FETCH_SCRIPT, nccl_db)
            storage = _run_script(_storage_script(), storage_db)
            wide = _run_script(_wide_nccl_script(), nccl_db)

        self.assertEqual(compact.returncode, 0, compact.stderr)
        self.assertEqual(compact.stderr, "")
        self.assertEqual(
            json.loads(compact.stdout),
            [
                {"node": "node-a", "busbw": 502.5, "latency": 21.25},
                {"node": "node-b", "busbw": 480.0, "latency": 24.0},
            ],
        )

        self.assertEqual(storage.returncode, 0, storage.stderr)
        self.assertEqual(storage.stderr, "")
        storage_rows = json.loads(storage.stdout)
        self.assertEqual([row["node"] for row in storage_rows], ["node-a", "node-b"])
        self.assertEqual(set(storage_rows[0]), {"node", *STORAGE_METRIC_FIELDS})
        self.assertEqual(storage_rows[0][STORAGE_METRIC_FIELDS[0]], 100.0)
        self.assertEqual(storage_rows[0][STORAGE_METRIC_FIELDS[-1]], 111.0)
        self.assertEqual(storage_rows[1][STORAGE_METRIC_FIELDS[0]], 200.0)

        self.assertEqual(wide.returncode, 0, wide.stderr)
        self.assertEqual(wide.stderr, "")
        wide_rows = json.loads(wide.stdout)
        self.assertEqual([row["node"] for row in wide_rows], ["node-a", "node-b"])
        node_a = wide_rows[0]
        self.assertEqual(node_a["timestamp"], 200)
        self.assertEqual(node_a["la_timestamp"], "latest-a")
        self.assertEqual(node_a["iterations"], 7)
        self.assertEqual(node_a["image_name"], "image:new")
        self.assertEqual(node_a["cuda"], "13.0")
        self.assertEqual(node_a["pytorch"], "2.9")
        self.assertEqual(node_a["samples"], 8)
        self.assertEqual(node_a["bus_bw"], 502.5)
        self.assertEqual(node_a["latency"], 21.25)
        self.assertEqual(node_a["mlx5_0"], 300.0)
        self.assertEqual(node_a["mlx5_13"], 313.0)

    def test_generated_readers_interpolate_real_error_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "populated-but-wrong-schema.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("CREATE TABLE unrelated (value TEXT NOT NULL)")
                connection.execute("INSERT INTO unrelated VALUES ('evidence')")
                connection.commit()

            cases = (
                (_NCCL_FETCH_SCRIPT, "nccl metrics error", "IB_HEALTH"),
                (_storage_script(), "storage metrics error", "storage_performance"),
                (_wide_nccl_script(), "IB_HEALTH metrics error", "IB_HEALTH"),
            )
            results = [
                (prefix, table, _run_script(script, db_path))
                for script, prefix, table in cases
            ]

        for prefix, table, completed in results:
            with self.subTest(prefix=prefix):
                self.assertEqual(completed.returncode, 0)
                self.assertEqual(json.loads(completed.stdout), [])
                self.assertIn(prefix, completed.stderr)
                self.assertIn(f"no such table: {table}", completed.stderr)
                self.assertNotIn("{exc}", completed.stderr)

    @staticmethod
    def _populate_nccl(path: Path) -> None:
        port_ddl = ", ".join(f"{column} REAL" for column in NCCL_IB_PORT_COLUMNS)
        port_names = ", ".join(NCCL_IB_PORT_COLUMNS)
        placeholders = ", ".join("?" for _ in NCCL_IB_PORT_COLUMNS)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                f"""
                CREATE TABLE IB_HEALTH (
                    Node TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    la_timestamp TEXT NOT NULL,
                    iterations INTEGER,
                    image_name TEXT NOT NULL,
                    cuda TEXT NOT NULL,
                    pytorch TEXT NOT NULL,
                    samples INTEGER,
                    BUS_BW REAL,
                    LATENCY REAL,
                    {port_ddl},
                    PRIMARY KEY (Node, timestamp)
                )
                """
            )
            rows = (
                ("node-a", 100, "old-a", 5, "image:old", "12.9", "2.8", 6, 490.0, 23.0, 100.0),
                ("node-a", 200, "latest-a", 7, "image:new", "13.0", "2.9", 8, 502.5, 21.25, 300.0),
                ("node-b", 150, "latest-b", 4, "image:b", "12.8", "2.7", 5, 480.0, 24.0, 500.0),
            )
            for row in rows:
                base, first_port = row[:-1], row[-1]
                connection.execute(
                    f"""
                    INSERT INTO IB_HEALTH (
                        Node, timestamp, la_timestamp, iterations, image_name,
                        cuda, pytorch, samples, BUS_BW, LATENCY, {port_names}
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {placeholders})
                    """,
                    (*base, *(first_port + index for index in range(len(NCCL_IB_PORT_COLUMNS)))),
                )
            connection.commit()

    @staticmethod
    def _populate_storage(path: Path) -> None:
        metric_ddl = ", ".join(f"{column} REAL" for column in STORAGE_METRIC_FIELDS)
        metric_names = ", ".join(STORAGE_METRIC_FIELDS)
        placeholders = ", ".join("?" for _ in STORAGE_METRIC_FIELDS)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                f"""
                CREATE TABLE storage_performance (
                    node TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    {metric_ddl},
                    PRIMARY KEY (node, timestamp)
                )
                """
            )
            for node, timestamp, first_value in (
                ("node-a", 100, 10.0),
                ("node-a", 200, 100.0),
                ("node-b", 150, 200.0),
            ):
                connection.execute(
                    f"""
                    INSERT INTO storage_performance (node, timestamp, {metric_names})
                    VALUES (?, ?, {placeholders})
                    """,
                    (node, timestamp, *(first_value + index for index in range(len(STORAGE_METRIC_FIELDS)))),
                )
            connection.commit()


if __name__ == "__main__":
    unittest.main()
