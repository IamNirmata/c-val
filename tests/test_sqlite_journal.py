import importlib.util
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/cval-sqlite-journal.py"
SPEC = importlib.util.spec_from_file_location("journal_migration", SCRIPT)
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


class JournalMigrationTests(unittest.TestCase):
    def test_backup_and_conversion_preserve_raw_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata"
            metadata.mkdir()
            for component in migration.COMPONENTS:
                with closing(sqlite3.connect(metadata / f"dltest_{component}.db")) as connection:
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.executescript(f'CREATE TABLE "{component}" (run_key TEXT,value REAL); INSERT INTO "{component}" VALUES ("node-1",3.5); CREATE TABLE cval_ingested_runs(run_key TEXT,sample_dir TEXT,updated_at INTEGER); INSERT INTO cval_ingested_runs VALUES("node-1","/sample",1); CREATE TABLE cval_ingest_metadata(id INTEGER,generation_id TEXT,state TEXT); INSERT INTO cval_ingest_metadata VALUES(1,"g1","complete");')
            report = migration.migrate(metadata, root / "backup", 30)
            self.assertTrue(all(item["converted"] for item in report["databases"]))
            for component in migration.COMPONENTS:
                for parent in (metadata, root / "backup"):
                    with closing(sqlite3.connect(parent / f"dltest_{component}.db")) as connection:
                        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")
                        self.assertEqual(connection.execute(f'SELECT * FROM "{component}"').fetchall(), [("node-1", 3.5)])
            self.assertEqual(len(json.loads((root / "backup/manifest.json").read_text())["databases"]), 4)


if __name__ == "__main__":
    unittest.main()