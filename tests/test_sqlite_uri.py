from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from cval.storage.sqlite_uri import (
    SQLiteFileIdentity,
    connect_sqlite_file,
    sqlite_file_uri,
    sqlite_readonly_script_prelude,
)


class SQLiteUriSafetyTests(unittest.TestCase):
    @staticmethod
    def _create(path: Path, value: str = "intended") -> None:
        with closing(connect_sqlite_file(path, mode="rwc")) as connection:
            connection.execute("CREATE TABLE marker (value TEXT NOT NULL)")
            connection.execute("INSERT INTO marker VALUES (?)", (value,))
            connection.commit()

    def test_reserved_characters_are_encoded_and_open_the_exact_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "compatibility %?# data.db"
            self._create(path)

            uri = sqlite_file_uri(path, parameters={"mode": "ro"}, must_exist=True)
            self.assertIn("%25", uri)
            self.assertIn("%3F", uri)
            self.assertIn("%23", uri)
            with closing(connect_sqlite_file(path, mode="ro")) as connection:
                self.assertEqual(
                    connection.execute("SELECT value FROM marker").fetchone()[0],
                    "intended",
                )
                main = connection.execute("PRAGMA database_list").fetchone()[2]
            self.assertEqual(Path(main), path)

    def test_final_and_ancestor_symlinks_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            real = root / "real.db"
            self._create(real)
            final_link = root / "final.db"
            final_link.symlink_to(real)
            with self.assertRaisesRegex(ValueError, "symlink"):
                connect_sqlite_file(final_link, mode="ro")

            real_dir = root / "real-dir"
            real_dir.mkdir()
            nested = real_dir / "nested.db"
            self._create(nested)
            linked_dir = root / "linked-dir"
            linked_dir.symlink_to(real_dir, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlink"):
                connect_sqlite_file(linked_dir / "nested.db", mode="ro")

    def test_expected_identity_rejects_replaced_inode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "identity.db"
            self._create(path)
            identity = SQLiteFileIdentity.capture(path)
            path.rename(Path(tmpdir) / "original.db")
            self._create(path, "replacement")

            with self.assertRaisesRegex(RuntimeError, "inode changed|path/device/inode"):
                connect_sqlite_file(path, mode="ro", expected_identity=identity)

    def test_injected_readonly_opener_encodes_and_asserts_main_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intended = root / "in-pod %?#.db"
            decoy = root / "decoy.db"
            self._create(intended)
            self._create(decoy, "decoy")
            namespace: dict[str, object] = {}
            exec(sqlite_readonly_script_prelude(), namespace)
            opener = namespace["connect_sqlite_readonly"]

            with closing(opener(str(intended))) as connection:  # type: ignore[operator]
                self.assertEqual(
                    connection.execute("SELECT value FROM marker").fetchone()[0],
                    "intended",
                )

            real_connect = sqlite3.connect
            with patch(
                "sqlite3.connect",
                side_effect=lambda *_args, **_kwargs: real_connect(decoy),
            ), self.assertRaisesRegex(RuntimeError, "main path mismatch"):
                opener(str(intended))  # type: ignore[operator]


if __name__ == "__main__":
    unittest.main()
