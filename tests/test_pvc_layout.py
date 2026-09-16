import importlib.util
import json
import shutil
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
    def _consolidation_fixture(self, parent, *backup_options):
        root = parent / "raw"
        for name in ("dltest/node/old", "logs/dltest/node/current", "validation_tests/dltest/runs/node/new", "archive/old", "archives/new", "dl_unit_test_evaluation", "metadata", "storage/node", "logs/storage/node"):
            path = root / name
            path.mkdir(parents=True, exist_ok=True)
            (path / "record.txt").write_text(name)
        shutil.copy2(root / "storage/node/record.txt", root / "logs/storage/node/record.txt")
        completed = subprocess.run(
            [sys.executable, str(Path(LAYOUT.__file__).with_name("cval-backup.py")),
             "--source", str(root), "--destination-root", str(parent / "cval-backup"),
             "--apply", "--confirm", "backup", "--quiesced", "--confirm-quiesced", "writers-stopped", *backup_options],
            capture_output=True, text=True, check=True, timeout=20,
        )
        return root, Path(json.loads(completed.stdout)["backup"])

    def test_consolidation_and_rollback_preserve_all_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root, backup = self._consolidation_fixture(Path(directory))
            before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
            protected = LAYOUT.tree_fingerprint(root / "dl_unit_test_evaluation")
            plan = LAYOUT.consolidation_plan(root)
            result = LAYOUT.apply_consolidation(root, backup, plan["plan_sha256"], confirm="consolidate", sources_idle="idle")
            self.assertEqual(result["state"], "complete")
            self.assertFalse((root / "dltest").exists())
            self.assertFalse((root / "validation_tests").exists())
            self.assertFalse((root / "archive").exists())
            self.assertEqual((root / "logs/dltest/node/old/record.txt").read_text(), "dltest/node/old")
            self.assertEqual((root / "logs/validation_tests/dltest/runs/node/new/record.txt").read_text(), "validation_tests/dltest/runs/node/new")
            self.assertEqual(LAYOUT.tree_fingerprint(root / "dl_unit_test_evaluation"), protected)
            self.assertEqual((root / "metadata/record.txt").read_bytes(), before["metadata/record.txt"])
            transaction = Path(result["transaction"])
            self.assertEqual((transaction / "duplicates/storage/node/record.txt").read_bytes(), before["storage/node/record.txt"])
            LAYOUT.backup_tool().verify_backup(backup)
            self.assertEqual(LAYOUT.rollback_consolidation(root, transaction, confirm="restore-layout", sources_idle="idle")["state"], "rolled_back")
            self.assertEqual(before, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()})

    def test_consolidation_and_rollback_ignore_explicitly_excluded_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root, backup = self._consolidation_fixture(Path(directory), "--exclude-dl-unit-test-evaluation")
            protected = root / "dl_unit_test_evaluation"
            (protected / "record.txt").write_text("new independent work after backup")
            (protected / "data.db-journal").write_bytes(b"live excluded journal")
            before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
            fingerprint = LAYOUT.tree_fingerprint
            original_stat = LAYOUT.os.stat

            def guarded_fingerprint(path, **options):
                if path == protected or path.is_relative_to(protected):
                    raise AssertionError("excluded directory was fingerprinted")
                return fingerprint(path, **options)

            def guarded_stat(path, *arguments, **options):
                if not isinstance(path, int) and "dl_unit_test_evaluation" in Path(path).parts:
                    raise AssertionError("excluded directory was inspected")
                return original_stat(path, *arguments, **options)

            with patch.object(LAYOUT, "tree_fingerprint", side_effect=guarded_fingerprint), patch.object(LAYOUT.os, "stat", side_effect=guarded_stat):
                plan = LAYOUT.consolidation_plan(root)
                result = LAYOUT.apply_consolidation(root, backup, plan["plan_sha256"], confirm="consolidate", sources_idle="idle")
                self.assertIsNone(result["protected_unchanged"])
                self.assertEqual(result["excluded"], ["dl_unit_test_evaluation/"])
                transaction = Path(result["transaction"])
                manifest = json.loads((transaction / "manifest.json").read_text())
                self.assertIsNone(manifest["protected"])
                self.assertEqual(manifest["excluded"], ["dl_unit_test_evaluation/"])
                LAYOUT.rollback_consolidation(root, transaction, confirm="restore-layout", sources_idle="idle")
            self.assertEqual(before, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()})
            self.assertFalse((backup / "dl_unit_test_evaluation").exists())

    def test_exclusion_does_not_relax_included_source_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root, backup = self._consolidation_fixture(Path(directory), "--exclude-dl-unit-test-evaluation")
            (root / "metadata/record.txt").write_text("included data changed")
            plan = LAYOUT.consolidation_plan(root)
            with self.assertRaisesRegex(ValueError, "source tree changed"):
                LAYOUT.apply_consolidation(root, backup, plan["plan_sha256"], confirm="consolidate", sources_idle="idle")
            self.assertTrue((root / "dltest/node/old/record.txt").is_file())
            self.assertFalse(backup.with_name(backup.name + "-layout").exists())

    def test_consolidation_requires_gates_current_backup_and_collision_free_plan(self):
        for condition in ("confirmation", "quiescence", "digest", "changed_backup_source", "collision"):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory() as directory:
                root, backup = self._consolidation_fixture(Path(directory))
                if condition == "changed_backup_source":
                    (root / "metadata/record.txt").write_text("new evidence after backup")
                if condition == "collision":
                    (root / "logs/storage/node/record.txt").write_text("different data")
                plan = LAYOUT.consolidation_plan(root)
                with self.assertRaises(ValueError):
                    LAYOUT.apply_consolidation(root, backup, "wrong" if condition == "digest" else plan["plan_sha256"], confirm="wrong" if condition == "confirmation" else "consolidate", sources_idle="wrong" if condition == "quiescence" else "idle")
                self.assertTrue((root / "dltest/node/old/record.txt").is_file())
                self.assertFalse(backup.with_name(backup.name + "-layout").exists())

    def test_interrupted_consolidation_can_be_rolled_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root, backup = self._consolidation_fixture(Path(directory))
            before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
            plan = LAYOUT.consolidation_plan(root)
            move = LAYOUT.move_entry
            calls = []

            def fail_second(*arguments):
                calls.append(arguments)
                if len(calls) == 2:
                    raise OSError("injected interruption")
                return move(*arguments)

            with patch.object(LAYOUT, "move_entry", side_effect=fail_second), self.assertRaises(OSError):
                LAYOUT.apply_consolidation(root, backup, plan["plan_sha256"], confirm="consolidate", sources_idle="idle")
            transaction = backup.with_name(backup.name + "-layout")
            self.assertEqual(json.loads((transaction / "manifest.json").read_text())["state"], "failed")
            LAYOUT.rollback_consolidation(root, transaction, confirm="restore-layout", sources_idle="idle")
            self.assertEqual(before, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()})

    def test_consolidation_rollback_rejects_modified_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root, backup = self._consolidation_fixture(Path(directory))
            plan = LAYOUT.consolidation_plan(root)
            result = LAYOUT.apply_consolidation(root, backup, plan["plan_sha256"], confirm="consolidate", sources_idle="idle")
            (root / "logs/dltest/node/old/record.txt").write_text("new data")
            with self.assertRaises(ValueError):
                LAYOUT.rollback_consolidation(root, Path(result["transaction"]), confirm="restore-layout", sources_idle="idle")
            self.assertFalse((root / "dltest").exists())
            self.assertEqual((root / "logs/dltest/node/old/record.txt").read_text(), "new data")

    def test_consolidation_plan_preserves_distinct_records_and_protected_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("dltest/node/old", "logs/dltest/node/current", "validation_tests/dltest/runs/node/new", "archive/old", "archives/new", "dl_unit_test_evaluation"):
                path = root / name
                path.mkdir(parents=True)
                (path / "record.txt").write_text(name)
            before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
            plan = LAYOUT.consolidation_plan(root)
            self.assertEqual(plan, LAYOUT.consolidation_plan(root))
            self.assertEqual(plan["conflicts"], [])
            moves = {entry["source"]: entry["destination"] for entry in plan["operations"]}
            self.assertEqual(moves["dltest/node/old"], "logs/dltest/node/old")
            self.assertEqual(moves["validation_tests"], "logs/validation_tests")
            self.assertEqual(moves["archive/old"], "archives/old")
            self.assertNotIn("dl_unit_test_evaluation", moves)
            self.assertEqual(before, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()})

    def test_consolidation_collision_is_reported_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nccl/node").mkdir(parents=True)
            (root / "logs/nccl/node").mkdir(parents=True)
            (root / "nccl/node/run.log").write_text("legacy evidence")
            (root / "logs/nccl/node/run.log").write_text("canonical evidence")
            plan = LAYOUT.consolidation_plan(root)
            self.assertEqual(len(plan["conflicts"]), 1)
            self.assertEqual((root / "logs/nccl/node/run.log").read_text(), "canonical evidence")

    def test_relocated_evidence_is_opt_in_and_rejects_symlinks(self):
        from cval.storage.paths import safe_existing_evidence_path

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "validation_tests/dltest/run/summary.json"
            relocated = root / "logs/validation_tests/dltest/run/summary.json"
            relocated.parent.mkdir(parents=True)
            relocated.write_text("{}")
            options = {"expected_path": original, "allowed_root": root, "expect_directory": False}
            with self.assertRaises(ValueError):
                safe_existing_evidence_path(original, **options)
            self.assertEqual(safe_existing_evidence_path(original, allow_relocated=True, **options), relocated)
            relocated.unlink()
            relocated.symlink_to(root / "unrelated")
            with self.assertRaises(ValueError):
                safe_existing_evidence_path(original, allow_relocated=True, **options)

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
                self.assertFalse((root / "archives").exists())

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
            archived = root / "archives" / name
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
                self.assertFalse((root / "archives").exists())

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
            self.assertFalse((root / "archives").exists())

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
            manifest = json.loads((root / "archives" / name / "manifest.json").read_text())
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