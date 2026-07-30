from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from cval.config import encode_config_snapshot, load_config
from cval.storage.per_test_results import (
    PerTestResultRecord,
    resolve_test_results_db_path,
    write_per_test_result,
    validate_table_manifest,
    framework_metric_ingestion_session,
)
from cval.validation.ingestion import _validate_adapter_receipt, ingest_test_results_file
from cval.validation.plugins import (
    IngestionConflictError,
    IngestionDisabledError,
    IngestionReceipt,
)


class PerTestResultStorageTests(unittest.TestCase):
    def test_default_off_gate_prevents_any_database_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_result = Path(tmpdir) / "result.json"
            config = load_config()

            with self.assertRaises(IngestionDisabledError):
                ingest_test_results_file(
                    missing_result,
                    config=config,
                    result_digest="sha256:" + "0" * 64,
                    config_snapshot_b64=encode_config_snapshot(config),
                )

            self.assertEqual(list(Path(tmpdir).iterdir()), [])

    def test_common_row_is_strictly_idempotent_and_health_stays_null(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage_results.db"
            record = self._record()

            self.assertTrue(write_per_test_result(record, db_path=db_path, now=10))
            self.assertFalse(write_per_test_result(record, db_path=db_path, now=20))
            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT run_id, test_id, status, artifacts_path, "
                    "health_class_name, health_class_numerical, "
                    "health_baseline_id, evaluated_at, created_at, updated_at "
                    "FROM test_results"
                ).fetchone()
                migrations = connection.execute(
                    "SELECT version, name FROM schema_migrations"
                ).fetchall()

        self.assertEqual(
            row,
            (
                "node-a-123",
                "storage",
                "pass",
                "/data/run/artifacts",
                None,
                None,
                None,
                None,
                10,
                10,
            ),
        )
        self.assertEqual(migrations, [(1, "initial-per-test-results")])

    def test_combination_key_must_be_empty_or_canonical_digest_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage_results.db"
            with self.assertRaisesRegex(ValueError, "combination_key"):
                write_per_test_result(
                    replace(self._record(), combination_key="not-a-digest"),
                    db_path=db_path,
                )
            self.assertFalse(db_path.exists())

    def test_changed_same_run_raw_evidence_rolls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage_results.db"
            record = self._record()
            write_per_test_result(record, db_path=db_path)

            changed_payload = json.loads(record.raw_result_json)
            changed_payload["message"] = "changed"
            changed = replace(
                record,
                raw_result_json=json.dumps(
                    changed_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            with self.assertRaises(IngestionConflictError):
                write_per_test_result(changed, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT raw_result_json FROM test_results"
                ).fetchall()

        self.assertEqual(rows, [(record.raw_result_json,)])

    def test_newer_schema_is_rejected_without_changing_journal_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "future.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT, applied_at INTEGER)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (2, 'future', 1)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "migration manifest"):
                write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual(journal_mode, "wal")
        self.assertNotIn("test_results", tables)

    def test_malformed_current_schema_preserves_wal_and_adds_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "malformed.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT, applied_at INTEGER)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES "
                    "(1, 'initial-per-test-results', 1)"
                )
                connection.execute("CREATE TABLE test_results (run_id TEXT)")
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "missing table"):
                write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual(journal_mode, "wal")
        self.assertNotIn("metric_ingestion_receipts", tables)

    def test_malformed_receipt_manifest_preserves_wal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "malformed-receipt.db"
            write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "ALTER TABLE metric_ingestion_receipts DROP COLUMN evidence_digest"
                )
                connection.execute("PRAGMA journal_mode=WAL")
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "evidence_digest"):
                write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

        self.assertEqual(journal_mode, "wal")

    def test_missing_adapter_version_table_preserves_wal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "adapter-version.db"
            write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("DROP TABLE adapter_schema_versions")
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "adapter.schema|Adapter schema"):
                write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

        self.assertEqual(journal_mode, "wal")

    def test_future_adapter_version_does_not_block_common_raw_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "adapter-version.db"
            self.assertTrue(write_per_test_result(self._record(), db_path=db_path))
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "INSERT INTO adapter_schema_versions "
                    "VALUES ('storage', 2, 1)"
                )
                connection.commit()

            self.assertFalse(write_per_test_result(self._record(), db_path=db_path))

    def test_weak_common_column_types_are_rejected_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "weak.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                    "applied_at INTEGER NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES "
                    "(1, 'initial-per-test-results', 1)"
                )
                connection.execute(
                    "CREATE TABLE test_results ("
                    "result_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "run_id TEXT NOT NULL UNIQUE, test_id TEXT, node TEXT, "
                    "started_timestamp TEXT, completed_timestamp TEXT, "
                    "status TEXT, exit_code TEXT, image_name TEXT, "
                    "pytorch_version TEXT, cuda_version TEXT, "
                    "test_config_digest TEXT, combination_key TEXT, "
                    "result_path TEXT, summary_path TEXT, artifacts_path TEXT, "
                    "raw_result_json TEXT, result_digest TEXT, health_class_name TEXT, "
                    "health_class_numerical TEXT, health_baseline_id TEXT, "
                    "evaluated_at TEXT, created_at TEXT, updated_at TEXT)"
                )
                connection.execute(
                    "CREATE TABLE metric_ingestion_receipts ("
                    "run_id TEXT PRIMARY KEY, test_id TEXT, adapter_api_version TEXT, "
                    "evidence_digest TEXT, inserted_count INTEGER, updated_count INTEGER, "
                    "metric_names_json TEXT, created_at INTEGER)"
                )
                connection.execute(
                    "CREATE TABLE adapter_schema_versions ("
                    "test_id TEXT PRIMARY KEY, version INTEGER, applied_at INTEGER)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "definition|DDL|triggers"):
                write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                count = connection.execute("SELECT COUNT(*) FROM test_results").fetchone()[0]
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

        self.assertEqual(count, 0)
        self.assertEqual(journal_mode, "wal")

    def test_required_index_on_decoy_table_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decoy-index.db"
            write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("DROP INDEX idx_test_results_node_completed")
                connection.execute(
                    "CREATE TABLE decoy(node TEXT, completed_timestamp INTEGER)"
                )
                connection.execute(
                    "CREATE INDEX idx_test_results_node_completed "
                    "ON decoy(node, completed_timestamp DESC)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "indexes|Index .* definition"):
                write_per_test_result(self._record(), db_path=db_path)

    def test_extra_implicit_unique_constraint_is_rejected(self) -> None:
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.execute(
                "CREATE TABLE metrics ("
                "node TEXT NOT NULL, timestamp INTEGER NOT NULL, image_name TEXT, "
                "PRIMARY KEY(node, timestamp), UNIQUE(image_name))"
            )

            with self.assertRaisesRegex(RuntimeError, "implicit indexes"):
                validate_table_manifest(
                    connection,
                    "metrics",
                    required_columns={"node", "timestamp", "image_name"},
                    primary_key=("node", "timestamp"),
                    implicit_indexes={
                        ("pk", ("node", "timestamp"), ("BINARY", "BINARY"))
                    },
                )

    def test_sql_comment_cannot_spoof_required_check(self) -> None:
        for table_sql in (
            "CREATE TABLE values_table ("
            "status TEXT CHECK(1) /* CHECK (STATUS IN ('PASS','FAIL')) */)",
            "CREATE TABLE values_table (status TEXT, "
            "CONSTRAINT \"CHECK (STATUS IN ('PASS','FAIL'))\" CHECK(1))",
        ):
            with self.subTest(sql=table_sql), closing(
                sqlite3.connect(":memory:")
            ) as connection:
                connection.execute(table_sql)

                with self.assertRaisesRegex(RuntimeError, "required constraint"):
                    validate_table_manifest(
                        connection,
                        "values_table",
                        required_columns={"status"},
                        required_sql_fragments=(
                            "CHECK (STATUS IN ('PASS','FAIL'))",
                        ),
                        constraint_counts=(1, 0),
                    )

    def test_nocase_implicit_index_is_rejected(self) -> None:
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.execute(
                "CREATE TABLE metrics (run_id TEXT COLLATE NOCASE PRIMARY KEY)"
            )

            with self.assertRaisesRegex(RuntimeError, "implicit indexes"):
                validate_table_manifest(
                    connection,
                    "metrics",
                    required_columns={"run_id"},
                    primary_key=("run_id",),
                    implicit_indexes={("pk", ("run_id",), ("BINARY",))},
                )

    def test_extra_migration_row_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "extra-migration.db"
            write_per_test_result(self._record(), db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (99, 'unexpected', 1)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "migration manifest"):
                write_per_test_result(self._record(), db_path=db_path)

    def test_declared_target_rejects_symlinked_test_directory(self) -> None:
        registered = load_config().tests.registry.require("storage")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "validation-root"
            outside = Path(tmpdir) / "outside"
            outside.mkdir()
            (root / "validation_tests").mkdir(parents=True)
            (root / "validation_tests" / "storage").symlink_to(
                outside,
                target_is_directory=True,
            )

            with self.assertRaisesRegex(ValueError, "symlink"):
                resolve_test_results_db_path(root, registered)

    def test_generic_receipt_requires_valid_counts_and_durable_row(self) -> None:
        registered = load_config().tests.registry.require("storage")
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "results.db"
            write_per_test_result(self._record(), db_path=db_path)
            with self.assertRaisesRegex(ValueError, "non-negative integer"):
                _validate_adapter_receipt(
                    IngestionReceipt(
                        test_id="storage",
                        run_id="node-a-123",
                        inserted_count=-1,
                        updated_count=0,
                        metric_names=("metric",),
                        evidence_digest="sha256:" + "a" * 64,
                        created_at=1,
                    ),
                    registered_test=registered,
                    run_id="node-a-123",
                    db_path=db_path,
                )
            with self.assertRaisesRegex(RuntimeError, "durable metric receipt"):
                _validate_adapter_receipt(
                    IngestionReceipt(
                        test_id="storage",
                        run_id="node-a-123",
                        inserted_count=1,
                        updated_count=0,
                        metric_names=("metric",),
                        evidence_digest="sha256:" + "a" * 64,
                        created_at=1,
                    ),
                    registered_test=registered,
                    run_id="node-a-123",
                    db_path=db_path,
                )

    def test_adapter_cannot_commit_framework_owned_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "results.db"
            write_per_test_result(self._record(), db_path=db_path)

            with self.assertRaises(sqlite3.DatabaseError):
                with framework_metric_ingestion_session(db_path) as connection:
                    connection.execute("CREATE TABLE adapter_probe(value TEXT)")
                    connection.execute("INSERT INTO adapter_probe VALUES ('durable')")
                    connection.commit()
            with closing(sqlite3.connect(db_path)) as connection:
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='adapter_probe'"
                ).fetchone()

        self.assertIsNone(table)

    def test_adapter_cannot_disable_authorizer_or_attach_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "results.db"
            outside = root / "outside.db"
            write_per_test_result(self._record(), db_path=db_path)

            with framework_metric_ingestion_session(db_path) as connection:
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.set_authorizer(None)
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(f"ATTACH DATABASE '{outside}' AS outside")
                cursor = connection.execute("SELECT 1")
                with self.assertRaises(AttributeError):
                    _ = cursor.connection

            self.assertFalse(outside.exists())

    @staticmethod
    def _record() -> PerTestResultRecord:
        raw_payload = {
            "schema_version": "cval.test-result.v1",
            "test_id": "storage",
            "status": "pass",
            "phase": "finished",
            "started_at": "2026-07-28T16:00:00Z",
            "completed_at": "2026-07-28T16:00:01Z",
            "duration_ms": 1000,
            "exit_code": 0,
            "summary": "/data/run/summary.txt",
            "artifacts": "/data/run/artifacts",
            "message": "",
        }
        return PerTestResultRecord(
            run_id="node-a-123",
            test_id="storage",
            node="node-a",
            run_timestamp=100,
            started_timestamp=100,
            completed_timestamp=101,
            status="pass",
            exit_code=0,
            image_name="image",
            pytorch_version="2.8",
            cuda_version="12.9",
            test_config_digest="sha256:" + "a" * 64,
            combination_key="",
            result_path="/data/run/result.json",
            summary_path="/data/run/summary.txt",
            artifacts_path="/data/run/artifacts",
            result_digest="sha256:" + "b" * 64,
            raw_result_json=json.dumps(
                raw_payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )


if __name__ == "__main__":
    unittest.main()
