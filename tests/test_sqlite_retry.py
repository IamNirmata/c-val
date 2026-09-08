import sqlite3
import tempfile
import unittest
from pathlib import Path

from cval.storage.retry import RetryPolicy, retry_sqlite


class SQLiteRetryTests(unittest.TestCase):
    def test_exhaustion_is_bounded(self):
        waits = []
        calls = []

        def busy():
            calls.append(1)
            error = sqlite3.OperationalError("database is locked")
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise error

        with self.assertRaises(sqlite3.OperationalError):
            retry_sqlite(busy, RetryPolicy(attempts=3, delay_seconds=0), sleeper=waits.append)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(waits), 2)

    def test_retries_whole_transaction_after_lock_released(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.db"
            blocker = sqlite3.connect(path)
            blocker.execute("CREATE TABLE sample (value INTEGER)")
            blocker.execute("BEGIN IMMEDIATE")
            waits = []

            def release(seconds):
                waits.append(seconds)
                blocker.rollback()

            def write():
                connection = sqlite3.connect(path, timeout=0)
                try:
                    with connection:
                        connection.execute("INSERT INTO sample VALUES (1)")
                finally:
                    connection.close()

            retry_sqlite(write, RetryPolicy(delay_seconds=0), sleeper=release)
            self.assertEqual(waits, [0])
            self.assertEqual(blocker.execute("SELECT COUNT(*) FROM sample").fetchone()[0], 1)
            blocker.close()

    def test_non_lock_error_is_not_retried(self):
        waits = []

        def fail():
            raise sqlite3.OperationalError("no such table: missing")

        with self.assertRaises(sqlite3.OperationalError):
            retry_sqlite(fail, RetryPolicy(), sleeper=waits.append)
        self.assertEqual(waits, [])