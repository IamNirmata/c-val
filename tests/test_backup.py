from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-backup.sh"
HELPER = REPO_ROOT / "scripts/cval-backup.py"
TIMESTAMP = "20260801T120000Z"


def load_helper():
    spec = importlib.util.spec_from_file_location("cval_backup_test_module", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BackupScriptTests(unittest.TestCase):
    def _root(self, parent: Path) -> Path:
        root = parent / "continuous_validation"
        (root / "metadata").mkdir(parents=True)
        (root / "metadata/value.txt").write_bytes(b"value")
        return root

    def _run(
        self,
        root: Path | None,
        *arguments: str,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = ["bash", str(SCRIPT)]
        if root is not None:
            command.extend(("--source", str(root)))
        command.extend(arguments)
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=os.environ
            | {"CVAL_BACKUP_TIMESTAMP": TIMESTAMP, "PYTHON": sys.executable}
            | (env or {}),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def _apply(
        self,
        root: Path,
        *arguments: str,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return self._run(
            root,
            *arguments,
            "--apply",
            "--confirm",
            "backup",
            "--quiesced",
            "--confirm-quiesced",
            "writers-stopped",
            env=env,
        )

    def test_inspection_creates_nothing_reports_capacity_and_excludes_backups(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            os.link(root / "metadata/value.txt", root / "metadata/value-hardlink.txt")
            old = root / "backups/old"
            old.mkdir(parents=True)
            (old / "ignored.txt").write_text("ignored", encoding="utf-8")

            completed = self._run(root, "--safety-margin-percent", "25")

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("inspection (created nothing)", completed.stdout)
            self.assertIn("source file count: 2", completed.stdout)
            self.assertIn("apparent unique bytes (hardlinks deduplicated): 5", completed.stdout)
            self.assertIn("safety margin percent: 25.0", completed.stdout)
            self.assertIn("required bytes including safety margin: 7", completed.stdout)
            self.assertIn("capacity sufficient: yes", completed.stdout)
            self.assertIn("NOT independent disaster recovery", completed.stdout)
            self.assertIn("independent external storage", completed.stdout)
            self.assertIn("--confirm-quiesced writers-stopped", completed.stdout)
            self.assertEqual([path.name for path in (root / "backups").iterdir()], ["old"])

    def test_inspection_insufficient_capacity_has_no_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            before = sorted(path.relative_to(root) for path in root.rglob("*"))

            completed = self._run(
                root,
                env={"_CVAL_BACKUP_TEST_FREE_BYTES": "1"},
            )

            self.assertEqual(completed.returncode, 1, completed.stderr)
            self.assertIn("destination filesystem free bytes: 1", completed.stdout)
            self.assertIn("capacity sufficient: NO", completed.stdout)
            self.assertEqual(sorted(path.relative_to(root) for path in root.rglob("*")), before)
            self.assertFalse((root / "backups").exists())

    def test_apply_refuses_insufficient_capacity_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))

            completed = self._apply(
                root,
                env={"_CVAL_BACKUP_TEST_FREE_BYTES": "1"},
            )

            self.assertEqual(completed.returncode, 2)
            self.assertIn("insufficient destination capacity", completed.stderr)
            self.assertFalse((root / "backups").exists())

    def test_apply_requires_backup_and_quiescence_gates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            missing_backup = self._run(root, "--apply")
            missing_quiescence = self._run(
                root, "--apply", "--confirm", "backup"
            )
            wrong_quiescence = self._run(
                root,
                "--apply",
                "--confirm",
                "backup",
                "--quiesced",
                "--confirm-quiesced",
                "almost-stopped",
            )

            self.assertEqual(missing_backup.returncode, 2)
            self.assertIn("--confirm backup", missing_backup.stderr)
            self.assertEqual(missing_quiescence.returncode, 2)
            self.assertIn("--confirm-quiesced writers-stopped", missing_quiescence.stderr)
            self.assertEqual(wrong_quiescence.returncode, 2)
            self.assertFalse((root / "backups").exists())

    def test_wal_sidecar_is_rejected_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            (root / "metadata/validation.db-wal").write_bytes(b"live")

            completed = self._apply(root)

            self.assertEqual(completed.returncode, 2)
            self.assertIn("live SQLite sidecar present", completed.stderr)
            self.assertFalse((root / "backups").exists())

    def test_concurrent_mutation_rejects_and_cleans_staging(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))

            completed = self._apply(
                root,
                env={"_CVAL_BACKUP_TEST_MUTATE_RELATIVE": "metadata/value.txt"},
            )

            self.assertEqual(completed.returncode, 2)
            self.assertIn("source tree changed during backup", completed.stderr)
            self.assertTrue((root / "metadata/value.txt").read_bytes().endswith(b"mutation"))
            self.assertEqual(list((root / "backups").iterdir()), [])

    def test_hardlinks_are_preserved_and_external_links_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = self._root(parent)
            os.link(root / "metadata/value.txt", root / "metadata/alias.txt")

            completed = self._apply(root)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            backup = Path(json.loads(completed.stdout)["backup"])
            first = (backup / "metadata/value.txt").stat()
            second = (backup / "metadata/alias.txt").stat()
            self.assertEqual((first.st_dev, first.st_ino), (second.st_dev, second.st_ino))
            self.assertEqual(first.st_nlink, 2)

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = self._root(parent)
            os.link(root / "metadata/value.txt", parent / "outside.txt")

            completed = self._run(root)

            self.assertEqual(completed.returncode, 2)
            self.assertIn("hardlink escapes the included source tree", completed.stderr)
            self.assertFalse((root / "backups").exists())

    def test_symlink_fifo_and_device_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            os.symlink("value.txt", root / "metadata/link.txt")
            completed = self._run(root)
            self.assertEqual(completed.returncode, 2)
            self.assertIn("refusing symlink", completed.stderr)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            os.mkfifo(root / "metadata/pipe")
            completed = self._run(root)
            self.assertEqual(completed.returncode, 2)
            self.assertIn("refusing FIFO", completed.stderr)

        helper = load_helper()
        with self.assertRaisesRegex(helper.BackupError, "refusing device"):
            helper._kind(os.stat("/dev/null", follow_symlinks=False), "device")

    def test_successful_apply_and_verify_sqlite_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            note = root / "metadata/value.txt"
            note.chmod(0o640)
            database = root / "metadata/validation.db"
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("CREATE TABLE values_table (value TEXT)")
                connection.execute("INSERT INTO values_table VALUES ('ok')")
                connection.commit()

            applied = self._apply(root)

            self.assertEqual(applied.returncode, 0, applied.stderr)
            payload = json.loads(applied.stdout)
            backup = Path(payload["backup"])
            manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
            files = {item["path"]: item for item in manifest["files"]}
            self.assertEqual(manifest["schema"], "cval.backup")
            self.assertEqual(manifest["schema_version"], 1)
            self.assertTrue(manifest["quiescence"]["declared"])
            self.assertEqual(
                manifest["consistency"]["pre_inventory_sha256"],
                manifest["consistency"]["post_inventory_sha256"],
            )
            self.assertEqual(files["metadata/validation.db"]["method"], "sqlite-backup")
            sqlite_entry = files["metadata/validation.db"]
            source_stat = database.stat()
            backup_database = backup / "metadata/validation.db"
            self.assertEqual(sqlite_entry["source_size"], source_stat.st_size)
            self.assertEqual(sqlite_entry["source_dev"], source_stat.st_dev)
            self.assertEqual(sqlite_entry["source_inode"], source_stat.st_ino)
            self.assertEqual(sqlite_entry["source_mtime_ns"], source_stat.st_mtime_ns)
            self.assertEqual(
                sqlite_entry["source_sha256"],
                __import__("hashlib").sha256(database.read_bytes()).hexdigest(),
            )
            self.assertEqual(sqlite_entry["backup_size"], backup_database.stat().st_size)
            self.assertEqual(sqlite_entry["backup_sha256"], sqlite_entry["sha256"])
            self.assertEqual(files["metadata/value.txt"]["method"], "regular-copy")
            self.assertEqual(stat.S_IMODE((backup / "metadata/value.txt").stat().st_mode), 0o640)
            self.assertFalse(any(path.name.endswith(("-wal", "-shm", "-journal")) for path in backup.rglob("*")))
            with closing(sqlite3.connect(backup / "metadata/validation.db")) as connection:
                self.assertEqual(
                    connection.execute("SELECT value FROM values_table").fetchone(),
                    ("ok",),
                )

            verified = self._run(None, "--verify", str(backup))

            self.assertEqual(verified.returncode, 0, verified.stderr)
            verification = json.loads(verified.stdout)
            self.assertTrue(verification["restore_ready"])
            self.assertEqual(verification["files"], 2)

    def test_verify_rejects_tampering_and_unexpected_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            applied = self._apply(root)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            (backup / "metadata/value.txt").write_bytes(b"tampered")

            tampered = self._run(None, "--verify", str(backup))

            self.assertEqual(tampered.returncode, 2)
            self.assertRegex(tampered.stderr, "size/mode mismatch|checksum mismatch")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            applied = self._apply(root)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            (backup / "unexpected.txt").write_text("unexpected", encoding="utf-8")

            unexpected = self._run(None, "--verify", str(backup))

            self.assertEqual(unexpected.returncode, 2)
            self.assertIn("backup file set mismatch", unexpected.stderr)

    def test_interruption_cleans_only_staging_and_preserves_existing_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            existing = root / "backups/existing"
            existing.mkdir(parents=True)
            marker = existing / "keep.txt"
            marker.write_text("keep", encoding="utf-8")

            completed = self._apply(
                root,
                env={"_CVAL_BACKUP_TEST_INTERRUPT_AFTER_FILES": "1"},
            )

            self.assertEqual(completed.returncode, 2)
            self.assertIn("injected interruption", completed.stderr)
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep")
            self.assertEqual(list((root / "backups").iterdir()), [existing])

    def test_external_destination_is_supported_and_backups_stay_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = self._root(parent)
            old = root / "backups/old"
            old.mkdir(parents=True)
            (old / "ignored.txt").write_text("ignored", encoding="utf-8")
            external = parent / "external"
            external.mkdir()

            preview = self._run(root, "--destination-root", str(external))
            applied = self._apply(root, "--destination-root", str(external))

            self.assertEqual(preview.returncode, 0, preview.stderr)
            self.assertIn(str(external), preview.stdout)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["excluded"], ["backups/"])
            self.assertFalse((backup / "backups").exists())
            self.assertTrue((root / "backups/old/ignored.txt").exists())


if __name__ == "__main__":
    unittest.main()
