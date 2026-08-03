from __future__ import annotations

import io
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cval.validation.scaffold as scaffold_module
from cval.config import load_config
from cval.validation.builtins import project_builtin_statuses
from cval.validation.registry import load_test_registry
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_to_env,
)
from cval.validation.runner import run_validation_tests
from cval.validation.scaffold import scaffold_validation_test
from tests.test_results_v2 import payload as historical_v2_payload


class GenericRegistryScaffoldResultTests(unittest.TestCase):
    def test_fourth_pass_fail_test_needs_only_descriptor_and_global_stanza(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            self._write_pass_fail_test(root, "smoke", order=40)
            registry = self._registry(root, {"smoke": True})
            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

            descriptor = root / "validation-tests/smoke/test_config.toml"
            self.assertNotIn("plugin", descriptor.read_text(encoding="utf-8"))
            self.assertEqual(registry.require("smoke").definition.plugin, None)
            self.assertEqual(result["schema_version"], "cval.results")
            self.assertEqual(result["tests"]["smoke"]["status"], "pass")

    def test_descriptor_rejects_removed_database_artifact_fields(self) -> None:
        for field in ("database_path", "results_db_path"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                root = self._root(tmpdir)
                self._write_pass_fail_test(root, "smoke", order=40, artifact=field)
                with self.assertRaisesRegex(ValueError, field):
                    self._registry(root, {"smoke": False})

    def test_scaffold_inspection_and_exact_confirmation_create_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            before = self._tree(root)

            inspection = scaffold_validation_test("smoke", 40, repo_root=root)

            self.assertFalse(inspection["applied"])
            self.assertEqual(inspection["mode"], "inspect")
            self.assertEqual(self._tree(root), before)
            self.assertIn("enabled = false", inspection["registry_stanza"])
            with self.assertRaisesRegex(ValueError, "exact --confirm scaffold"):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="yes"
                )
            self.assertEqual(self._tree(root), before)

    def test_scaffold_apply_is_atomic_disabled_pass_fail_only_with_exact_modes(self) -> None:
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
            descriptor_text = (target / "test_config.toml").read_text(encoding="utf-8")
            self.assertTrue(applied["applied"])
            self.assertFalse(applied["global_config_mutated"])
            self.assertEqual(global_config.read_bytes(), b"")
            self.assertEqual(
                sorted(path.relative_to(target).as_posix() for path in target.rglob("*") if path.is_file()),
                sorted(applied["files"]),
            )
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            for path in target.rglob("*"):
                expected = 0o700 if path.is_dir() else 0o755 if path.suffix == ".sh" else 0o600
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected, path)
            for removed in ("database_path", "results_db_path", "[plugin]", "[health]"):
                self.assertNotIn(removed, descriptor_text)
            registry = self._registry(root, {"smoke": False})
            self.assertFalse(registry.require("smoke").enabled)
            self.assertIsNone(registry.require("smoke").definition.plugin)

    def test_scaffold_duplicate_failure_and_publish_race_preserve_existing_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            self._write_pass_fail_test(root, "storage", order=10)
            with self.assertRaisesRegex(ValueError, "already declared by 'storage'"):
                scaffold_validation_test("smoke", 10, repo_root=root)
            self.assertFalse((root / "validation-tests/smoke").exists())

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            scaffold_validation_test(
                "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
            )
            descriptor = root / "validation-tests/smoke/test_config.toml"
            original = descriptor.read_bytes()
            with self.assertRaises(FileExistsError):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            self.assertEqual(descriptor.read_bytes(), original)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original_rename = scaffold_module.rename_noreplace_at

            def race(source_fd: int, source_name: str, destination_fd: int, destination_name: str) -> None:
                os.mkdir(destination_name, mode=0o700, dir_fd=destination_fd)
                winner_fd = os.open(destination_name, os.O_RDONLY | os.O_DIRECTORY, dir_fd=destination_fd)
                try:
                    marker_fd = os.open(
                        "winner", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=winner_fd
                    )
                    os.close(marker_fd)
                finally:
                    os.close(winner_fd)
                original_rename(source_fd, source_name, destination_fd, destination_name)

            with patch.object(scaffold_module, "rename_noreplace_at", side_effect=race):
                with self.assertRaises(FileExistsError):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            target = root / "validation-tests/smoke"
            self.assertEqual([path.name for path in target.iterdir()], ["winner"])
            self.assertFalse(any(path.name.startswith(".cval-scaffold-") for path in target.parent.iterdir()))

    def test_scaffold_write_failure_rolls_back_staging_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original_write = scaffold_module._write_scaffold_file
            calls = 0

            def fail_third(*args: object, **kwargs: object) -> None:
                nonlocal calls
                calls += 1
                if calls == 3:
                    raise OSError("injected write failure")
                original_write(*args, **kwargs)

            with patch.object(scaffold_module, "_write_scaffold_file", side_effect=fail_third):
                with self.assertRaisesRegex(OSError, "injected"):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            self.assertEqual(list((root / "validation-tests").iterdir()), [])

    def test_disabled_test_is_present_but_never_executed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            marker = root / "executed"
            self._write_pass_fail_test(root, "smoke", order=40, marker=marker)
            registry = self._registry(root, {"smoke": False})

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

            self.assertEqual(result["tests"]["smoke"]["phase"], "not_selected")
            self.assertEqual(result["tests"]["smoke"]["status"], "incomplete")
            self.assertFalse(marker.exists())
            self.assertFalse((root / "data/logs/smoke").exists())

    def test_historical_v1_v2_readers_and_canonical_writer(self) -> None:
        historical_v1 = {
            "schema_version": "cval.results.v1",
            "node": "node-a",
            "timestamp": "123",
            "overall": "pass",
            "tests": {
                test_id: {"status": "pass", "enabled": True}
                for test_id in ("storage", "nccl", "dltest")
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            v1_path = root / "v1.json"
            v2_path = root / "v2.json"
            v1_path.write_text(json.dumps(historical_v1), encoding="utf-8")
            v2_path.write_text(json.dumps(historical_v2_payload()), encoding="utf-8")
            self.assertEqual(load_validation_result(v1_path).schema_version, "cval.results.v1")
            self.assertEqual(load_validation_result(v2_path).schema_version, "cval.results.v2")

            self._write_pass_fail_test(root, "smoke", order=40)
            result = run_validation_tests(
                config=load_config(),
                registry=self._registry(root, {"smoke": True}),
                environ=self._environment(root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            current_path = root / "data/logs/job_logs/node-a/node-a-123/result.json"
            current = load_validation_result(current_path)

            self.assertEqual(result["schema_version"], "cval.results")
            self.assertIsInstance(current, ValidationResultV2)
            self.assertEqual(current.schema_version, "cval.results")

    def test_builtin_storage_nccl_dltest_projections_remain_exact(self) -> None:
        result = load_validation_result(self._write_historical_v1())
        projected = validation_result_to_env(result)

        self.assertEqual(
            {key: projected[key] for key in ("GCRRESULT1", "GCRRESULT2", "GCRRESULT3")},
            {"GCRRESULT1": "pass", "GCRRESULT2": "fail", "GCRRESULT3": "incomplete"},
        )
        self.assertEqual(
            {key: projected[key] for key in ("RUN_STORAGE", "RUN_NCCL", "RUN_DLTEST")},
            {"RUN_STORAGE": "true", "RUN_NCCL": "true", "RUN_DLTEST": "false"},
        )
        self.assertEqual(
            project_builtin_statuses(projected),
            {"storage": "pass", "nccl": "fail", "dltest": "incomplete", "all": "fail"},
        )

    @staticmethod
    def _root(tmpdir: str) -> Path:
        root = Path(tmpdir)
        (root / "validation-tests").mkdir()
        return root

    @staticmethod
    def _tree(root: Path) -> tuple[str, ...]:
        return tuple(sorted(path.relative_to(root).as_posix() for path in root.rglob("*")))

    @staticmethod
    def _registry(root: Path, enabled: dict[str, bool]):
        return load_test_registry(
            {
                test_id: {
                    "enabled": state,
                    "config_path": f"validation-tests/{test_id}/test_config.toml",
                }
                for test_id, state in enabled.items()
            },
            repo_root=root,
            include_defaults=False,
            require_enabled=False,
        )

    @staticmethod
    def _environment(validation_root: Path) -> dict[str, str]:
        return os.environ | {
            "CVAL_VALIDATION_ROOT": str(validation_root),
            "CVAL_RUN_ID": "node-a-123",
            "CVAL_NODE": "node-a",
            "CVAL_TIMESTAMP": "123",
            "CVAL_PYTORCH_VERSION": "2.8.0",
            "CVAL_CUDA_VERSION": "12.9",
        }

    @staticmethod
    def _write_pass_fail_test(
        root: Path,
        test_id: str,
        *,
        order: int,
        artifact: str | None = None,
        marker: Path | None = None,
    ) -> None:
        test_dir = root / "validation-tests" / test_id
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        marker_line = "" if marker is None else f"touch {str(marker)!r}\n"
        (test_dir / "run-test.sh").write_text(
            "#!/bin/bash\nset -euo pipefail\n"
            + marker_line
            + "printf '%s\\n' '{\"status\":\"pass\"}' > \"$CVAL_TEST_SUMMARY_FILE\"\n",
            encoding="utf-8",
        )
        artifact_line = "" if artifact is None else f'{artifact} = "removed.db"\n'
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
{artifact_line}summary_filename = "summary.json"
''',
            encoding="utf-8",
        )

    @staticmethod
    def _write_historical_v1() -> Path:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
        path = Path(handle.name)
        with handle:
            json.dump(
                {
                    "schema_version": "cval.results.v1",
                    "node": "node-a",
                    "timestamp": "123",
                    "overall": "fail",
                    "tests": {
                        "storage": {"status": "pass", "enabled": True},
                        "nccl": {"status": "fail", "enabled": True},
                        "dltest": {"status": "incomplete", "enabled": False},
                    },
                },
                handle,
            )
        GenericRegistryScaffoldResultTests.addClassCleanup(path.unlink, missing_ok=True)
        return path


if __name__ == "__main__":
    unittest.main()
