import importlib.util
import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/cval-sqlite-journal.py"
SPEC = importlib.util.spec_from_file_location("journal_migration", SCRIPT)
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


class JournalMigrationTests(unittest.TestCase):
    @staticmethod
    def create_sources(metadata):
        metadata.mkdir()
        for component in migration.COMPONENTS:
            with closing(sqlite3.connect(metadata / f"dltest_{component}.db")) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.executescript(f'CREATE TABLE "{component}" (run_key TEXT,value REAL); INSERT INTO "{component}" VALUES ("node-1",3.5); CREATE TABLE cval_ingested_runs(run_key TEXT,sample_dir TEXT,updated_at INTEGER); INSERT INTO cval_ingested_runs VALUES("node-1","/sample",1); CREATE TABLE cval_ingest_metadata(id INTEGER,generation_id TEXT,state TEXT); INSERT INTO cval_ingest_metadata VALUES(1,"g1","complete");')

    def test_failed_backup_does_not_convert_any_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata"
            self.create_sources(metadata)
            originals = {path: path.read_bytes() for path in metadata.glob("*.db")}
            with patch.object(migration, "file_digest", return_value="mismatch"):
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    migration.migrate(metadata, root / "backup", 30, staging_directory=root)
            for path, original in originals.items():
                self.assertEqual(path.read_bytes(), original)
                with closing(migration.open_db(path, "ro")) as connection:
                    self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(json.loads((root / "backup/manifest.json").read_text())["databases"], [])
            self.assertEqual(list(root.glob("cval-journal-*")), [])

    def test_published_backup_requires_matching_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory) / "staged.db"
            backup = Path(directory) / "backup.db"
            staged.write_bytes(b"sample evidence")
            with patch.object(migration, "file_digest", return_value="mismatch"):
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    migration.publish_backup(staged, backup, time.monotonic() + 10)
            self.assertEqual(staged.read_bytes(), b"sample evidence")
            with self.assertRaises(FileExistsError):
                migration.publish_backup(staged, backup, time.monotonic() + 10)

    def test_metric_sampling_has_bounded_query_work(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.executescript('CREATE TABLE sample(run_key TEXT,value INTEGER); CREATE TABLE cval_ingested_runs(run_key TEXT,sample_dir TEXT,updated_at INTEGER); INSERT INTO cval_ingested_runs VALUES("missing","/sample",1); CREATE TABLE cval_ingest_metadata(id INTEGER,generation_id TEXT,state TEXT); INSERT INTO cval_ingest_metadata VALUES(1,"g1","complete");')
            connection.executemany("INSERT INTO sample VALUES (?,?)", (("node-1", value) for value in range(10000)))
            connection.commit()
            connection.set_progress_handler(lambda: 1, 1000)
            report = migration.evidence(connection, "sample")
            self.assertEqual(report["sample_count"], 6)
            self.assertEqual(report["receipt_count"], 1)

    def test_backup_source_probe_does_not_change_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.db"
            with closing(sqlite3.connect(source)) as writer:
                writer.execute("CREATE TABLE sample(value)")
                writer.execute("INSERT INTO sample VALUES (7)")
                writer.commit()
            before = source.read_bytes()
            with closing(migration.open_db(source, "ro")) as reader:
                report = migration.probe_backup_source(reader, source)
            self.assertIn(report["status"], (sqlite3.SQLITE_OK, sqlite3.SQLITE_DONE))
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(list(Path(directory).iterdir()), [source])

    def test_backup_deadline_reports_lock_state(self):
        reader = Mock()
        destination = Mock()

        def blocked_backup(_destination, *, pages, progress):
            progress(sqlite3.SQLITE_BUSY, 0, 0)
            progress(sqlite3.SQLITE_BUSY, 0, 0)

        reader.backup.side_effect = blocked_backup
        with patch.object(migration.time, "monotonic", side_effect=[100, 101, 111]), patch.object(migration, "emit") as report:
            with self.assertRaisesRegex(TimeoutError, "status=5, remaining_pages=0, total_pages=0"):
                migration.backup_database(reader, destination, Path("/source.db"), 10)
        self.assertEqual(report.call_args.args[0]["elapsed_seconds"], 11)
        self.assertEqual(report.call_args.args[0]["event"], "backup_progress")

    def test_backup_progress_reports_real_page_completion(self):
        with closing(sqlite3.connect(":memory:")) as reader, closing(sqlite3.connect(":memory:")) as destination:
            reader.execute("CREATE TABLE sample(value)")
            reader.execute("INSERT INTO sample VALUES (7)")
            reader.commit()
            with patch.object(migration, "emit") as report:
                migration.backup_database(reader, destination, Path("/source.db"), 10)
            self.assertEqual(report.call_args.args[0]["status"], sqlite3.SQLITE_DONE)
            self.assertEqual(report.call_args.args[0]["remaining_pages"], 0)
            self.assertGreater(report.call_args.args[0]["total_pages"], 0)
            self.assertEqual(destination.execute("SELECT value FROM sample").fetchall(), [(7,)])

    def test_backup_and_conversion_preserve_raw_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata"
            self.create_sources(metadata)
            report = migration.migrate(metadata, root / "backup", 30)
            self.assertTrue(all(item["converted"] for item in report["databases"]))
            for item in report["databases"]:
                self.assertEqual(item["backup_sha256"], hashlib.sha256(Path(item["backup"]).read_bytes()).hexdigest())
            for component in migration.COMPONENTS:
                for parent in (metadata, root / "backup"):
                    with closing(sqlite3.connect(parent / f"dltest_{component}.db")) as connection:
                        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")
                        self.assertEqual(connection.execute(f'SELECT * FROM "{component}"').fetchall(), [("node-1", 3.5)])
            self.assertEqual(len(json.loads((root / "backup/manifest.json").read_text())["databases"]), 4)


if __name__ == "__main__":
    unittest.main()