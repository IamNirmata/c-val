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
from unittest.mock import patch

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

    def test_external_backup_includes_existing_backups(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            previous = root / "backups/old"
            previous.mkdir(parents=True)
            (previous / "retained.txt").write_bytes(b"previous backup evidence")
            destination = Path(tmpdir) / "cval-backup"
            inspected = self._run(root, "--destination-root", str(destination))
            self.assertEqual(inspected.returncode, 0, inspected.stderr)
            self.assertIn("excluded: none", inspected.stdout)
            self.assertIn("source file count: 2", inspected.stdout)
            self.assertFalse(destination.exists())

            completed = self._apply(root, "--destination-root", str(destination))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            backup = Path(json.loads(completed.stdout)["backup"])
            self.assertEqual((backup / "backups/old/retained.txt").read_bytes(), b"previous backup evidence")
            self.assertEqual(json.loads((backup / "manifest.json").read_text())["excluded"], [])
            verified = self._run(None, "--verify", str(backup))
            self.assertEqual(verified.returncode, 0, verified.stderr)
            self.assertEqual((previous / "retained.txt").read_bytes(), b"previous backup evidence")

    def test_exclusion_skips_dl_evaluation_without_stat_or_traversal(self) -> None:
        module = load_helper()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            (root / "dl_unit_test_evaluation").mkdir()
            (root / "dl_unit_test_evaluation/live.db-journal").write_bytes(b"excluded")
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            original_stat = os.stat

            def guarded_stat(path, *arguments, **keywords):
                if "dl_unit_test_evaluation" in Path(path).parts:
                    raise AssertionError("excluded directory was inspected")
                return original_stat(path, *arguments, **keywords)

            try:
                with patch.object(module.os, "stat", side_effect=guarded_stat):
                    inventory = module.inventory_tree(descriptor, exclude_backups=False, exclude_dl_unit_test_evaluation=True)
                self.assertEqual(set(inventory), {".", "metadata", "metadata/value.txt"})
            finally:
                os.close(descriptor)

    def test_excluded_directory_changes_do_not_invalidate_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            protected = root / "dl_unit_test_evaluation"
            protected.mkdir()
            (protected / "script.py").write_bytes(b"source")
            (protected / "live.db-journal").write_bytes(b"journal")
            destination = Path(tmpdir) / "external"
            preview = self._run(root, "--destination-root", str(destination), "--exclude-dl-unit-test-evaluation")
            self.assertEqual(preview.returncode, 0, preview.stderr)
            self.assertIn("excluded: dl_unit_test_evaluation/", preview.stdout)
            applied = self._apply(root, "--destination-root", str(destination), "--exclude-dl-unit-test-evaluation", env={"_CVAL_BACKUP_TEST_MUTATE_RELATIVE": "dl_unit_test_evaluation/script.py"})
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            manifest = json.loads((backup / "manifest.json").read_text())
            self.assertEqual(manifest["excluded"], ["dl_unit_test_evaluation/"])
            self.assertFalse((backup / "dl_unit_test_evaluation").exists())
            self.assertEqual((backup / "metadata/value.txt").read_bytes(), b"value")
            verified = self._run(None, "--verify", str(backup))
            self.assertEqual(verified.returncode, 0, verified.stderr)
            self.assertEqual(json.loads(verified.stdout)["excluded"], ["dl_unit_test_evaluation/"])

    def test_dl_exclusion_does_not_skip_similarly_named_or_nested_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            for name in ("dl_unit_test_evaluation", "dl_unit_test_evaluation-other", "archives/dl_unit_test_evaluation"):
                directory = root / name
                directory.mkdir(parents=True)
                (directory / "keep.txt").write_text(name)
            result = self._apply(root, "--exclude-dl-unit-test-evaluation")
            self.assertEqual(result.returncode, 0, result.stderr)
            backup = Path(json.loads(result.stdout)["backup"])
            self.assertEqual(json.loads((backup / "manifest.json").read_text())["excluded"], ["backups/", "dl_unit_test_evaluation/"])
            self.assertFalse((backup / "dl_unit_test_evaluation").exists())
            self.assertTrue((backup / "dl_unit_test_evaluation-other/keep.txt").is_file())
            self.assertTrue((backup / "archives/dl_unit_test_evaluation/keep.txt").is_file())

    def test_verify_keeps_supporting_prior_backup_manifest_versions(self) -> None:
        for version in (1, 2):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as tmpdir:
                root = self._root(Path(tmpdir))
                result = self._apply(root)
                self.assertEqual(result.returncode, 0, result.stderr)
                backup = Path(json.loads(result.stdout)["backup"])
                path = backup / "manifest.json"
                manifest = json.loads(path.read_text())
                manifest["schema_version"] = version
                manifest["capacity"].pop("exclude_dl_unit_test_evaluation")
                path.write_text(json.dumps(manifest))
                verified = self._run(None, "--verify", str(backup))
                self.assertEqual(verified.returncode, 0, verified.stderr)

    def test_publish_falls_back_in_owner_only_directory_when_renameat2_is_unsupported(
        self,
    ) -> None:
        module = load_helper()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            root.chmod(0o700)
            (root / "staging").mkdir()
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with patch.object(
                    module,
                    "rename_noreplace_at",
                    side_effect=OSError(22, "unsupported"),
                ):
                    module._publish_staging(descriptor, "staging", "published")
            finally:
                os.close(descriptor)

            self.assertFalse((root / "staging").exists())
            self.assertTrue((root / "published").is_dir())

    def test_publish_fallback_never_replaces_existing_destination(self) -> None:
        module = load_helper()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            root.chmod(0o700)
            (root / "staging").mkdir()
            published = root / "published"
            published.mkdir()
            marker = published / "marker"
            marker.write_text("retain", encoding="utf-8")
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with patch.object(
                    module,
                    "rename_noreplace_at",
                    side_effect=OSError(22, "unsupported"),
                ):
                    with self.assertRaises(FileExistsError):
                        module._publish_staging(
                            descriptor, "staging", "published"
                        )
            finally:
                os.close(descriptor)

            self.assertTrue((root / "staging").is_dir())
            self.assertEqual(marker.read_text(encoding="utf-8"), "retain")

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

    def test_historical_snapshots_are_explicitly_preserved_as_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            historical = root / "backups/.old.staging/baselines"
            historical.mkdir(parents=True)
            database = historical / "classification.db"
            database.write_bytes(b"SQLite format 3\x00retained incomplete snapshot")
            journal = historical / "classification.db-journal"
            journal.write_bytes(b"retained journal")
            before = {path: path.read_bytes() for path in (database, journal)}
            destination = Path(tmpdir) / "cval-backup"
            rejected = self._run(root, "--destination-root", str(destination))
            self.assertEqual(rejected.returncode, 2)
            self.assertIn("sidecar", rejected.stderr)
            applied = self._apply(root, "--destination-root", str(destination), "--preserve-history-bytes")
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            manifest = json.loads((backup / "manifest.json").read_text())
            self.assertTrue(manifest["capacity"]["preserve_history_bytes"])
            for original, content in before.items():
                self.assertEqual((backup / original.relative_to(root)).read_bytes(), content)
                self.assertEqual(original.read_bytes(), content)
            verified = self._run(None, "--verify", str(backup))
            self.assertEqual(verified.returncode, 0, verified.stderr)
            self.assertTrue(json.loads(verified.stdout)["history_preserved_as_is"])

    def test_history_option_does_not_relax_current_database_protection(self) -> None:
        for directory in ("metadata", "evaluation-engine", "dl_unit_test_evaluation"):
            with self.subTest(directory=directory), tempfile.TemporaryDirectory() as tmpdir:
                root = self._root(Path(tmpdir))
                sidecar = root / directory / "data.db-journal"
                sidecar.parent.mkdir(exist_ok=True)
                sidecar.write_bytes(b"current journal")
                result = self._apply(root, "--destination-root", str(Path(tmpdir) / "external"), "--preserve-history-bytes")
                self.assertEqual(result.returncode, 2)
                self.assertIn("sidecar", result.stderr)
                self.assertEqual(sidecar.read_bytes(), b"current journal")

    def test_history_option_rejects_hardlinks_to_current_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            (root / "archives").mkdir()
            os.link(root / "metadata/value.txt", root / "archives/alias.txt")
            result = self._apply(root, "--destination-root", str(Path(tmpdir) / "external"), "--preserve-history-bytes")
            self.assertEqual(result.returncode, 2)
            self.assertIn("crosses current and historical", result.stderr)
            self.assertFalse((Path(tmpdir) / "external").exists())

    def test_verify_rejects_history_exemption_for_current_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            applied = self._apply(root)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            manifest_path = backup / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["capacity"]["preserve_history_bytes"] = True
            manifest["files"][0]["method"] = "history-byte-copy"
            manifest_path.write_text(json.dumps(manifest))
            result = self._run(None, "--verify", str(backup))
            self.assertEqual(result.returncode, 2)
            self.assertIn("not permitted for this path", result.stderr)

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
            self.assertEqual(manifest["schema_version"], 3)
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

    def test_verify_accepts_manifest_above_old_limit_and_keeps_new_bound(self) -> None:
        module = load_helper()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            applied = self._apply(root)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            manifest_path = backup / "manifest.json"
            with manifest_path.open("ab") as handle:
                for _chunk in range(64):
                    handle.write(b" " * (1024 * 1024))
            self.assertGreater(manifest_path.stat().st_size, 64 * 1024 * 1024)
            verified = self._run(None, "--verify", str(backup))
            self.assertEqual(verified.returncode, 0, verified.stderr)
            self.assertTrue(json.loads(verified.stdout)["restore_ready"])
            with patch.object(module, "MAX_MANIFEST_BYTES", 64 * 1024 * 1024):
                with self.assertRaisesRegex(ValueError, "Input exceeds"):
                    module.verify_backup(backup)
            with manifest_path.open("r+b") as handle:
                handle.truncate(module.MAX_MANIFEST_BYTES + 1)
            with self.assertRaisesRegex(ValueError, "Input exceeds"):
                module.verify_backup(backup)

    def test_writer_refuses_manifest_larger_than_reader_limit(self) -> None:
        module = load_helper()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(Path(tmpdir))
            with patch.object(module, "MAX_MANIFEST_BYTES", 1), patch.object(module, "_publish_staging") as publish:
                with self.assertRaisesRegex(module.BackupError, "backup manifest exceeds"):
                    module.run(["--source", str(root), "--apply", "--confirm", "backup", "--quiesced", "--confirm-quiesced", "writers-stopped"])
                publish.assert_not_called()
            self.assertEqual(list((root / "backups").iterdir()), [])
            self.assertEqual((root / "metadata/value.txt").read_bytes(), b"value")

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

    def test_existing_external_destination_includes_nested_backups(self) -> None:
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
            self.assertEqual(manifest["excluded"], [])
            self.assertEqual((backup / "backups/old/ignored.txt").read_text(), "ignored")
            self.assertTrue((root / "backups/old/ignored.txt").exists())


if __name__ == "__main__":
    unittest.main()
