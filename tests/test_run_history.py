from __future__ import annotations

import copy
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cval.k8s.client import CommandResult
from cval.storage.run_history import (
    get_run_history_rows,
    _RUN_HISTORY_WRITE_AUTHORIZATION,
    ingest_run_history_file,
    ingest_run_history_result as _ingest_run_history_result,
    run_history_rows_from_db,
)
from cval.config import encode_config_snapshot, load_config
from cval.validation.results import validation_result_v2_digest
from cval.validation.results import parse_validation_result_v2
from tests.test_results_v2 import payload


def ingest_run_history_result(*args, **kwargs):
    result = args[0]
    kwargs["result_digest"] = validation_result_v2_digest(result)
    kwargs["_authorization"] = _RUN_HISTORY_WRITE_AUTHORIZATION
    return _ingest_run_history_result(*args, **kwargs)


class RunHistoryTests(unittest.TestCase):
    def test_file_writer_enforces_default_off_gate(self) -> None:
        result = parse_validation_result_v2(payload())
        config = load_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = Path(tmpdir) / "result.json"
            result_path.write_text(
                __import__("json").dumps(payload()), encoding="utf-8"
            )

            with self.assertRaisesRegex(ValueError, "writes are disabled"):
                ingest_run_history_file(
                    result_path,
                    db_path=Path(tmpdir) / "history.db",
                    config=config,
                    result_digest=validation_result_v2_digest(result),
                    config_snapshot_b64=encode_config_snapshot(config),
                )

            self.assertFalse((Path(tmpdir) / "history.db").exists())

    def test_ingest_creates_normalized_schema_and_rows(self) -> None:
        result = parse_validation_result_v2(payload())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "node-run-history.db"

            ingest_run_history_result(
                result,
                db_path=db_path,
                result_path="/data/run/result.json",
                now=1000,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                run = connection.execute(
                    "SELECT run_id, node, overall_status, tests_requested_json, "
                    "created_at, updated_at FROM runs"
                ).fetchone()
                tests = connection.execute(
                    "SELECT test_id, selected, execution_order, status "
                    "FROM run_tests ORDER BY execution_order"
                ).fetchall()
                migrations = connection.execute(
                    "SELECT version, name FROM schema_migrations"
                ).fetchall()
                view = connection.execute(
                    "SELECT tests_ran FROM node_run_history"
                ).fetchone()[0]

        self.assertEqual(
            run,
            (
                "node-a-123",
                "node-a",
                "pass",
                '["storage","nccl","dltest","smoke"]',
                1000,
                1000,
            ),
        )
        self.assertEqual(
            tests,
            [
                ("storage", 1, 10, "pass"),
                ("nccl", 1, 20, "pass"),
                ("dltest", 1, 30, "pass"),
                ("smoke", 1, 40, "pass"),
            ],
        )
        self.assertEqual(migrations, [(1, "initial-run-history")])
        self.assertEqual(view, "storage,nccl,dltest,smoke")

    def test_repeated_ingestion_is_idempotent(self) -> None:
        result = parse_validation_result_v2(payload())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            ingest_run_history_result(result, db_path=db_path, now=100)
            ingest_run_history_result(result, db_path=db_path, now=200)

            with closing(sqlite3.connect(db_path)) as connection:
                run_count = connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                test_count = connection.execute(
                    "SELECT COUNT(*) FROM run_tests"
                ).fetchone()[0]
                timestamps = connection.execute(
                    "SELECT created_at, updated_at FROM runs"
                ).fetchone()

        self.assertEqual(run_count, 1)
        self.assertEqual(test_count, 4)
        self.assertEqual(timestamps, (100, 100))

    def test_rejects_run_identity_or_terminal_mutation(self) -> None:
        original = parse_validation_result_v2(payload())
        identity_payload = copy.deepcopy(payload())
        identity_payload["node"] = "other-node"
        identity = parse_validation_result_v2(identity_payload)
        failed_payload = copy.deepcopy(payload())
        failed_payload["overall"] = "fail"
        failed_payload["tests"]["nccl"]["status"] = "fail"  # type: ignore[index]
        failed_payload["tests"]["nccl"]["exit_code"] = 2  # type: ignore[index]
        failed = parse_validation_result_v2(failed_payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            ingest_run_history_result(original, db_path=db_path)
            with self.assertRaisesRegex(ValueError, "immutable run evidence"):
                ingest_run_history_result(identity, db_path=db_path)
            with self.assertRaisesRegex(ValueError, "immutable run evidence"):
                ingest_run_history_result(failed, db_path=db_path)

            with closing(sqlite3.connect(db_path)) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1)
                self.assertEqual(
                    connection.execute(
                        "SELECT overall_status FROM runs"
                    ).fetchone()[0],
                    "pass",
                )

    def test_rejects_registered_test_set_change(self) -> None:
        smaller_payload = copy.deepcopy(payload())
        del smaller_payload["tests"]["smoke"]  # type: ignore[index]
        smaller = parse_validation_result_v2(smaller_payload)
        larger = parse_validation_result_v2(payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            ingest_run_history_result(smaller, db_path=db_path)
            with self.assertRaisesRegex(ValueError, "immutable (run|test) evidence"):
                ingest_run_history_result(larger, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                test_ids = {
                    row[0]
                    for row in connection.execute("SELECT test_id FROM run_tests")
                }

        self.assertEqual(test_ids, {"storage", "nccl", "dltest"})

    def test_read_only_filters_do_not_create_missing_db(self) -> None:
        first = parse_validation_result_v2(payload())
        second_payload = copy.deepcopy(payload())
        second_payload["run_id"] = "node-b-124"
        second_payload["node"] = "node-b"
        second_payload["timestamp"] = 124
        second_payload["timestamp_la"] = "1969-12-31T16:02:04-08:00"
        second = parse_validation_result_v2(second_payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing = root / "missing.db"
            self.assertEqual(run_history_rows_from_db(missing), [])
            self.assertFalse(missing.exists())

            db_path = root / "history.db"
            ingest_run_history_result(first, db_path=db_path, now=100)
            ingest_run_history_result(second, db_path=db_path, now=101)
            rows = run_history_rows_from_db(
                db_path,
                node="node-a",
                test_id="smoke",
                status="pass",
                limit=10,
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].run_id, "node-a-123")
        self.assertEqual(rows[0].tests_ran, "storage,nccl,dltest,smoke")

    def test_rejects_invalid_history_limit_without_db_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
                run_history_rows_from_db(db_path, limit=0)
            self.assertFalse(db_path.exists())

    def test_remote_reader_uses_stdin_and_read_only_mode(self) -> None:
        calls = []

        class Client:
            def run(self, args, check=True, input_text=None, timeout=None):
                calls.append((args, input_text))
                if args[:3] == ["get", "pod", "-n"]:
                    return CommandResult(
                        args=args,
                        stdout='{"status":{"phase":"Running"}}',
                        stderr="",
                        returncode=0,
                    )
                return CommandResult(
                    args=args,
                    stdout="[]",
                    stderr="",
                    returncode=0,
                )

        rows = get_run_history_rows(
            client=Client(),
            pod="pvc-pod",
            namespace="gcr-admin",
            db_path="/data/history.db",
        )

        self.assertEqual(rows, [])
        exec_args, script = calls[-1]
        self.assertEqual(exec_args[:5], ["exec", "-i", "-n", "gcr-admin", "pvc-pod"])
        self.assertIn("python3", exec_args)
        self.assertIn("mode=ro", script)
        self.assertNotIn(".upper()", script)
        self.assertIn("WHERE type IN ('table','index','view','trigger')", script)
        manifest = json.loads(exec_args[-1])
        self.assertIn("'pass','fail','incomplete'", manifest["table:runs"])
        self.assertTrue(
            any(key.startswith("index:sqlite_autoindex_") for key in manifest)
        )

    def test_local_and_remote_readers_reject_partial_or_extra_schema(self) -> None:
        class ExecutingClient:
            def run(self, args, check=True, input_text=None, timeout=None):
                if args[:3] == ["get", "pod", "-n"]:
                    return CommandResult(
                        args=args,
                        stdout='{"status":{"phase":"Running"}}',
                        stderr="",
                        returncode=0,
                    )
                separator = args.index("--")
                remote_args = args[separator + 1 :]
                completed = subprocess.run(
                    [sys.executable, *remote_args[1:]],
                    input=input_text,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if completed.returncode:
                    raise RuntimeError(completed.stderr)
                return CommandResult(
                    args=args,
                    stdout=completed.stdout,
                    stderr=completed.stderr,
                    returncode=completed.returncode,
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                    "applied_at INTEGER NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES "
                    "(1, 'initial-run-history', 1)"
                )
                connection.execute("CREATE TABLE unexpected(value TEXT)")
                connection.commit()

            with self.assertRaises(RuntimeError):
                run_history_rows_from_db(db_path)
            with self.assertRaisesRegex(RuntimeError, "schema manifest"):
                get_run_history_rows(
                    client=ExecutingClient(),
                    pod="pvc-pod",
                    namespace="gcr-admin",
                    db_path=str(db_path),
                )

            with closing(sqlite3.connect(db_path)) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
        self.assertEqual(tables, {"schema_migrations", "unexpected"})

    def test_writer_rejects_newer_schema_without_run_rows(self) -> None:
        result = parse_validation_result_v2(payload())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT, applied_at INTEGER)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (99, 'future', 1)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "migration manifest"):
                ingest_run_history_result(result, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual(tables, {"schema_migrations"})
        self.assertEqual(journal_mode, "wal")

    def test_writer_rejects_malformed_current_schema_without_changing_wal(self) -> None:
        result = parse_validation_result_v2(payload())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                    "applied_at INTEGER NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES "
                    "(1, 'initial-run-history', 1)"
                )
                connection.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY)")
                connection.commit()

            with self.assertRaisesRegex(
                RuntimeError,
                "schema manifest|missing table|columns|Database views",
            ):
                ingest_run_history_result(result, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

        self.assertEqual(journal_mode, "wal")

    def test_retry_rejects_changed_image_or_config_digest(self) -> None:
        original_payload = payload()
        original = parse_validation_result_v2(original_payload)
        changed_payload = copy.deepcopy(original_payload)
        changed_payload["image_name"] = "different-image"
        changed_payload["global_config_digest"] = "sha256:" + "b" * 64
        changed = parse_validation_result_v2(changed_payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            ingest_run_history_result(original, db_path=db_path)

            with self.assertRaisesRegex(ValueError, "immutable run evidence"):
                ingest_run_history_result(changed, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT image_name, global_config_digest FROM runs"
                ).fetchone()

        self.assertEqual(
            row,
            (original.image_name, original.global_config_digest),
        )

    def test_retry_rejects_changed_generated_errors_and_test_metadata(self) -> None:
        original_payload = payload()
        original = parse_validation_result_v2(original_payload)
        changed_payload = copy.deepcopy(original_payload)
        changed_payload["generated_at"] = "2026-07-28T16:00:02Z"
        changed_payload["errors"] = [
            {
                "code": "late-error",
                "message": "changed",
                "test_id": None,
                "timestamp": "2026-07-28T16:00:02Z",
                "detail_path": "",
            }
        ]
        changed_payload["tests"]["storage"]["display_name"] = "Changed"  # type: ignore[index]
        changed = parse_validation_result_v2(changed_payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            ingest_run_history_result(original, db_path=db_path)

            with self.assertRaisesRegex(ValueError, "immutable run evidence"):
                ingest_run_history_result(changed, db_path=db_path)

    def test_extra_migration_or_changed_view_is_rejected(self) -> None:
        result = parse_validation_result_v2(payload())
        for mutation, expected in (
            (
                lambda connection: connection.execute(
                    "INSERT INTO schema_migrations VALUES (0, 'unexpected', 1)"
                ),
                "migration manifest",
            ),
            (
                lambda connection: (
                    connection.execute("DROP VIEW node_run_history"),
                    connection.execute(
                        "CREATE VIEW node_run_history AS SELECT * FROM runs"
                    ),
                ),
                "schema manifest|View node_run_history",
            ),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "history.db"
                ingest_run_history_result(result, db_path=db_path)
                with closing(sqlite3.connect(db_path)) as connection:
                    mutation(connection)
                    connection.commit()

                with self.assertRaisesRegex(RuntimeError, expected):
                    run_history_rows_from_db(db_path)

    def test_local_reader_and_writer_require_exact_complete_schema_manifest(self) -> None:
        result = parse_validation_result_v2(payload())
        mutations = (
            lambda connection: (
                connection.execute(
                    "CREATE TABLE transient_sequence_source("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT)"
                ),
                connection.execute("DROP TABLE transient_sequence_source"),
            ),
            lambda connection: (
                connection.execute("DROP INDEX idx_runs_node_started"),
                connection.execute(
                    "CREATE INDEX idx_runs_node_started "
                    "ON runs(node ASC, started_timestamp DESC)"
                ),
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "history.db"
                ingest_run_history_result(result, db_path=db_path)
                with closing(sqlite3.connect(db_path)) as connection:
                    mutation(connection)
                    connection.commit()

                with self.assertRaisesRegex(RuntimeError, "schema manifest"):
                    run_history_rows_from_db(db_path)
                with self.assertRaisesRegex(RuntimeError, "schema manifest"):
                    ingest_run_history_result(result, db_path=db_path)

    def test_writer_rejects_sequence_only_database_as_nonempty(self) -> None:
        result = parse_validation_result_v2(payload())
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE temporary_sequence_source("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT)"
                )
                connection.execute("DROP TABLE temporary_sequence_source")
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "schema_migrations"):
                ingest_run_history_result(result, db_path=db_path)

            with closing(sqlite3.connect(db_path)) as connection:
                objects = {
                    (row[0], row[1])
                    for row in connection.execute(
                        "SELECT type, name FROM sqlite_master"
                    )
                }
        self.assertEqual(objects, {("table", "sqlite_sequence")})

    def test_reader_rejects_newer_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "history.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, name TEXT, applied_at INTEGER)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (99, 'future', 1)"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "migration manifest"):
                run_history_rows_from_db(db_path)


if __name__ == "__main__":
    unittest.main()
