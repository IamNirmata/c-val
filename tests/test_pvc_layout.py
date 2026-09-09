import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "pvc_layout", Path(__file__).resolve().parents[1] / "scripts/cval-pvc-layout.py"
)
LAYOUT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LAYOUT)


class PvcLayoutTests(unittest.TestCase):
    def _archive_fixture(self, root):
        (root / "metadata").mkdir()
        for component in LAYOUT.COMPONENTS:
            with closing(sqlite3.connect(root / "metadata" / f"dltest_{component}.db")) as connection:
                connection.execute("CREATE TABLE cval_ingested_runs(run_key TEXT, sample_dir TEXT)")
                connection.execute("INSERT INTO cval_ingested_runs VALUES (?,?)", ("retained", str(root / "dltest/node-a/run")))
                connection.commit()
        for source in LAYOUT.ARCHIVE_ENTRIES:
            if source.endswith(".tar.gz"):
                (root / source).write_bytes(b"legacy bundle")
            else:
                (root / source).mkdir()
                (root / source / "evidence.txt").write_bytes(b"retained legacy data")
        (root / "logs").mkdir()

    def test_archive_plan_is_deterministic_and_does_not_mutate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._archive_fixture(root)
            before = sorted(path.name for path in root.iterdir())
            plan = LAYOUT.archive_plan(root, "20260909_073800_PDT")
            self.assertEqual(plan, LAYOUT.archive_plan(root, "20260909_073800_PDT"))
            self.assertEqual({entry["source"] for entry in plan["entries"]}, set(LAYOUT.ARCHIVE_ENTRIES))
            self.assertEqual(len(plan["plan_sha256"]), 64)
            self.assertEqual(before, sorted(path.name for path in root.iterdir()))

    def test_archive_plan_rejects_references_symlinks_and_unknown_receipts(self):
        for condition in ("referenced", "symlink", "unavailable"):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self._archive_fixture(root)
                database = root / "metadata/dltest_compute_performance.db"
                if condition == "referenced":
                    with closing(sqlite3.connect(database)) as connection:
                        connection.execute("INSERT INTO cval_ingested_runs VALUES (?,?)", ("old", str(root / "baselines/snapshot")))
                        connection.commit()
                elif condition == "symlink":
                    bundle = root / "dltest.tar.gz"
                    bundle.unlink()
                    bundle.symlink_to(root / "metadata")
                else:
                    database.unlink()
                with self.assertRaises(ValueError):
                    LAYOUT.archive_plan(root, "20260909_073800_PDT")
                self.assertFalse((root / "archive").exists())

    def test_archive_and_rollback_preserve_contents_and_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._archive_fixture(root)
            name = "20260909_073800_PDT"
            plan = LAYOUT.archive_plan(root, name)
            before = {entry["source"]: entry["identity"] for entry in plan["entries"]}
            raw_before = {path.name: path.read_bytes() for path in (root / "metadata").iterdir()}
            result = LAYOUT.apply_archive(root, name, plan["plan_sha256"], confirm="archive-legacy", sources_idle="idle")
            self.assertEqual(result["state"], "complete")
            archived = root / "archive" / name
            for source, destination in LAYOUT.ARCHIVE_ENTRIES.items():
                self.assertFalse((root / source).exists())
                self.assertEqual(LAYOUT.entry_identity(archived / destination), before[source])
            self.assertTrue((root / "logs").is_dir())
            self.assertEqual(raw_before, {path.name: path.read_bytes() for path in (root / "metadata").iterdir()})
            restored = LAYOUT.rollback_archive(root, name, confirm="restore-legacy", sources_idle="idle")
            self.assertEqual(restored["state"], "rolled_back")
            self.assertEqual((root / "baselines/evidence.txt").read_bytes(), b"retained legacy data")
            self.assertEqual((root / "dltest.tar.gz").read_bytes(), b"legacy bundle")
            self.assertEqual(LAYOUT.rollback_archive(root, name, confirm="restore-legacy", sources_idle="idle")["state"], "rolled_back")

    def test_archive_requires_confirmation_and_unchanged_plan(self):
        for condition in ("unconfirmed", "not_idle", "digest", "changed"):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self._archive_fixture(root)
                name = "20260909_073800_PDT"
                plan = LAYOUT.archive_plan(root, name)
                if condition == "changed":
                    (root / "dltest.tar.gz").write_bytes(b"changed evidence")
                with self.assertRaises(ValueError):
                    LAYOUT.apply_archive(root, name, "wrong" if condition == "digest" else plan["plan_sha256"], confirm="wrong" if condition == "unconfirmed" else "archive-legacy", sources_idle="wrong" if condition == "not_idle" else "idle")
                self.assertFalse((root / "archive").exists())

    def test_cli_archive_without_confirmation_is_nonmutating(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._archive_fixture(root)
            result = subprocess.run(
                [sys.executable, str(Path(LAYOUT.__file__)), "--root", str(root),
                 "--apply-archive", "--archive-name", "20260909_073800_PDT"],
                text=True, capture_output=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--confirm archive-legacy", result.stderr)
            self.assertFalse((root / "archive").exists())

    def test_atomic_rename_refuses_existing_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source").write_bytes(b"original")
            (root / "destination").write_bytes(b"preserve destination")
            with LAYOUT.locked_archive(root, create=True) as (root_fd, _archive_fd):
                with self.assertRaises(FileExistsError):
                    LAYOUT.rename_noreplace(root_fd, "source", root_fd, "destination")
            self.assertEqual((root / "source").read_bytes(), b"original")
            self.assertEqual((root / "destination").read_bytes(), b"preserve destination")

    def test_rollback_refuses_destination_collision_without_partial_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._archive_fixture(root)
            name = "20260909_073800_PDT"
            plan = LAYOUT.archive_plan(root, name)
            LAYOUT.apply_archive(root, name, plan["plan_sha256"], confirm="archive-legacy", sources_idle="idle")
            (root / "baselines").mkdir()
            (root / "baselines/new.txt").write_bytes(b"do not overwrite")
            with self.assertRaises(FileExistsError):
                LAYOUT.rollback_archive(root, name, confirm="restore-legacy", sources_idle="idle")
            self.assertFalse((root / "test1").exists())
            self.assertEqual((root / "baselines/new.txt").read_bytes(), b"do not overwrite")

    def test_partial_archive_has_recoverable_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._archive_fixture(root)
            name = "20260909_073800_PDT"
            plan = LAYOUT.archive_plan(root, name)
            rename = LAYOUT.rename_noreplace

            def fail_second(source_fd, source, destination_fd, destination):
                if source == "deeplearning_unit_test":
                    raise OSError("simulated interruption")
                rename(source_fd, source, destination_fd, destination)

            with patch.object(LAYOUT, "rename_noreplace", side_effect=fail_second), self.assertRaises(OSError):
                LAYOUT.apply_archive(root, name, plan["plan_sha256"], confirm="archive-legacy", sources_idle="idle")
            manifest = json.loads((root / "archive" / name / "manifest.json").read_text())
            self.assertEqual(manifest["state"], "failed")
            self.assertEqual(manifest["moved"], ["baselines"])
            LAYOUT.rollback_archive(root, name, confirm="restore-legacy", sources_idle="idle")
            self.assertTrue(all((root / source).exists() for source in LAYOUT.ARCHIVE_ENTRIES))

    def test_scan_is_bounded_and_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "legacy"
            source.mkdir()
            (source / "one.txt").write_bytes(b"one")
            (root / "outside.txt").write_bytes(b"not part of the inventory")
            (source / "alias").symlink_to(root / "outside.txt")
            result = LAYOUT.entry_summary(source, entry_limit=20, seconds=5)
            self.assertEqual(result["scan_state"], "complete")
            self.assertEqual(result["files"], 1)
            self.assertEqual(result["logical_bytes"], 3)
            self.assertEqual(result["symlinks"], 1)
            bounded = LAYOUT.entry_summary(source, entry_limit=1, seconds=5)
            self.assertEqual(bounded["scan_state"], "bounded_partial")
            self.assertEqual(bounded["entries_scanned"], 1)

    def test_receipts_preserve_historical_path_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "metadata").mkdir()
            path = root / "metadata/dltest_compute_performance.db"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE cval_ingested_runs(run_key TEXT, sample_dir TEXT)")
                connection.executemany("INSERT INTO cval_ingested_runs VALUES (?,?)", [
                    ("legacy", str(root / "dltest/node-a/run")),
                    ("current", str(root / "validation_tests/dltest/runs/node-b/run")),
                ])
                connection.commit()
            before = path.read_bytes()
            report = LAYOUT.receipt_references(root, seconds=5)
            self.assertEqual(report["compute_performance"]["reference_counts"], {"validation_tests": 1, "dltest": 1})
            self.assertEqual(report["numerical_correctness"]["state"], "unavailable")
            self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()