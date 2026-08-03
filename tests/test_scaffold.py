from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cval.validation.scaffold as scaffold_module
from cval.validation.registry import load_test_registry
from cval.validation.scaffold import scaffold_validation_test


class ScaffoldTests(unittest.TestCase):
    def test_inspection_and_exact_confirmation_create_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            before = self._tree(root)

            inspection = scaffold_validation_test("smoke", 40, repo_root=root)

            self.assertFalse(inspection["applied"])
            self.assertEqual(inspection["mode"], "inspect")
            self.assertIn("enabled = false", inspection["registry_stanza"])
            self.assertEqual(self._tree(root), before)
            with self.assertRaisesRegex(ValueError, "exact --confirm scaffold"):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="yes"
                )
            self.assertEqual(self._tree(root), before)

    def test_apply_atomically_creates_disabled_pass_fail_only_exact_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            global_config = root / "config/cval.toml"
            global_config.parent.mkdir()
            global_config.write_bytes(b"")
            previous = os.umask(0o777)
            try:
                applied = scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            finally:
                os.umask(previous)

            target = root / "validation-tests/smoke"
            descriptor = (target / "test_config.toml").read_text(encoding="utf-8")
            self.assertTrue(applied["applied"])
            self.assertFalse(applied["global_config_mutated"])
            self.assertEqual(global_config.read_bytes(), b"")
            self.assertEqual(
                sorted(
                    path.relative_to(target).as_posix()
                    for path in target.rglob("*")
                    if path.is_file()
                ),
                sorted(applied["files"]),
            )
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            for path in target.rglob("*"):
                expected = (
                    0o700
                    if path.is_dir()
                    else 0o755
                    if path.suffix == ".sh"
                    else 0o600
                )
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected, path)
            for removed in (
                "database_path",
                "results_db_path",
                "plugin",
                "health",
            ):
                self.assertNotIn(removed, descriptor)
                self.assertNotIn(removed, "\n".join(self._generated_text(target)))
            registry = load_test_registry(
                {
                    "smoke": {
                        "enabled": False,
                        "config_path": "validation-tests/smoke/test_config.toml",
                    }
                },
                repo_root=root,
                include_defaults=False,
                require_enabled=False,
            )
            self.assertFalse(registry.require("smoke").enabled)
            self.assertIsNone(registry.require("smoke").definition.plugin)

    def test_duplicate_existing_and_publish_race_preserve_winner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            self._write_descriptor(root, "storage", 10)
            with self.assertRaisesRegex(ValueError, "already declared by 'storage'"):
                scaffold_validation_test("smoke", 10, repo_root=root)
            self.assertFalse((root / "validation-tests/smoke").exists())

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            scaffold_validation_test(
                "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
            )
            descriptor = root / "validation-tests/smoke/test_config.toml"
            before = descriptor.read_bytes()
            with self.assertRaises(FileExistsError):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            self.assertEqual(descriptor.read_bytes(), before)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original = scaffold_module.rename_noreplace_at

            def race(
                source_fd: int,
                source_name: str,
                destination_fd: int,
                destination_name: str,
            ) -> None:
                os.mkdir(destination_name, mode=0o700, dir_fd=destination_fd)
                winner_fd = os.open(
                    destination_name,
                    os.O_RDONLY | os.O_DIRECTORY,
                    dir_fd=destination_fd,
                )
                try:
                    marker_fd = os.open(
                        "winner",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=winner_fd,
                    )
                    os.close(marker_fd)
                finally:
                    os.close(winner_fd)
                original(source_fd, source_name, destination_fd, destination_name)

            with patch.object(
                scaffold_module, "rename_noreplace_at", side_effect=race
            ):
                with self.assertRaises(FileExistsError):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            target = root / "validation-tests/smoke"
            self.assertEqual([path.name for path in target.iterdir()], ["winner"])
            self.assertFalse(
                any(
                    path.name.startswith(".cval-scaffold-")
                    for path in target.parent.iterdir()
                )
            )

    def test_failed_write_rolls_back_complete_staging_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original = scaffold_module._write_scaffold_file
            calls = 0

            def fail_third(*args: object, **kwargs: object) -> None:
                nonlocal calls
                calls += 1
                if calls == 3:
                    raise OSError("injected write failure")
                original(*args, **kwargs)

            with patch.object(
                scaffold_module, "_write_scaffold_file", side_effect=fail_third
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            self.assertEqual(list((root / "validation-tests").iterdir()), [])

    @staticmethod
    def _root(tmpdir: str) -> Path:
        root = Path(tmpdir)
        (root / "validation-tests").mkdir()
        return root

    @staticmethod
    def _tree(root: Path) -> tuple[str, ...]:
        return tuple(
            sorted(path.relative_to(root).as_posix() for path in root.rglob("*"))
        )

    @staticmethod
    def _generated_text(root: Path) -> list[str]:
        return [
            path.read_text(encoding="utf-8")
            for path in root.rglob("*")
            if path.is_file()
        ]

    @staticmethod
    def _write_descriptor(root: Path, test_id: str, order: int) -> None:
        test_dir = root / "validation-tests" / test_id
        test_dir.mkdir()
        for name in ("setup.sh", "run-test.sh"):
            (test_dir / name).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "test_config.toml").write_text(
            f'''schema_version = "cval.test.v1"
[test]
id = "{test_id}"
display_name = "{test_id.title()}"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
summary_filename = "summary.json"
''',
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
