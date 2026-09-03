from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from cval.config import load_config
from cval.validation.builtins import project_builtin_statuses
from cval.validation.registry import load_test_registry
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_to_env,
)
from cval.validation.runner import run_validation_tests
from tests.test_results_v2 import payload as historical_v2_payload


class GenericRegistryResultTests(unittest.TestCase):
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
        GenericRegistryResultTests.addClassCleanup(path.unlink, missing_ok=True)
        return path


if __name__ == "__main__":
    unittest.main()
