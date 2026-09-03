from __future__ import annotations

import json
import shutil
import tempfile
import tomllib
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.config import config_to_dict, load_config
from cval.validation.operational_targets import (
    BASELINE_BUILD,
    BASELINE_CLASSIFY,
    RESULTS_EXPORT,
    OperationalTarget,
)
from cval.validation.plugins import (
    BaselineBuildContext,
    BaselineClassificationContext,
    ExportContext,
)
from cval.validation.registry import load_test_registry, parse_resource_quantity


class ConfigTests(unittest.TestCase):
    def test_loads_repository_default_config(self) -> None:
        config = load_config()

        self.assertEqual(config.job.job_prefix, "cval")
        self.assertEqual(config.cluster.namespace, "gcr-admin")
        self.assertEqual(config.cluster.pvc_access_pod, "gcr-admin-pvc-access")
        self.assertEqual(config.scheduling.batch_size, 3)
        self.assertEqual(config.scheduling.node_cooldown_seconds, 14400)
        self.assertEqual(config.monitoring.pending_start_timeout_seconds, 480)
        self.assertEqual(config.runtime.repo_dir, "/workspace/c-val")
        self.assertEqual(
            config.runtime.dl_results_root_path,
            "/data/continuous_validation/validation_tests/dltest/runs",
        )
        self.assertTrue(config.tests.registry.require("storage").enabled)
        self.assertTrue(config.tests.registry.require("nccl").enabled)
        self.assertTrue(config.tests.registry.require("dltest").enabled)
        nccl_settings = config.tests.registry.require("nccl").definition.settings
        dltest_settings = config.tests.registry.require("dltest").definition.settings
        self.assertEqual(nccl_settings["gpu_count"], 8)
        self.assertIsNone(nccl_settings.get("ibbw_start_device"))
        self.assertIsNone(nccl_settings.get("ibbw_end_device"))
        self.assertEqual(dltest_settings["iterations"], 100)
        self.assertEqual(config.baseline.storage_peer_tolerance_pct, 10.0)
        self.assertEqual(config.baseline.dl_compute_tolerance_pct, 3.0)
        self.assertEqual(config.baseline.dl_numerical_tolerance_pct, 0.1)
        self.assertEqual(config.baseline.dl_overlap_tolerance_pct, 20.0)
        self.assertEqual(
            [test.id for test in config.tests.registry.tests],
            ["storage", "nccl", "dltest"],
        )
        self.assertEqual(
            config.tests.registry.require("nccl").config_path,
            "validation-tests/nccl/test_config.toml",
        )
        self.assertTrue(str(config.job.template_path).endswith("ymls/specific-node-job.yml"))

    def test_global_test_tables_only_register_and_activate(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        data = tomllib.loads(
            (repo_root / "config" / "cval.toml").read_text(encoding="utf-8")
        )

        for test_id in ("storage", "nccl", "dltest"):
            self.assertEqual(set(data["tests"][test_id]), {"enabled", "config_path"})
            self.assertEqual(
                data["tests"][test_id]["config_path"],
                f"validation-tests/{test_id}/test_config.toml",
            )

        self.assertEqual(
            set(data["storage"]),
            {
                "validation_db_path", "storage_db_path", "nccl_db_path",
                "dl_numerical_db_path", "dl_compute_db_path",
                "dl_collective_db_path", "dl_overlap_db_path",
            },
        )

    def test_loads_partial_override_with_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[cluster]
namespace = "staging"

[scheduling]
batch_size = 2

[runtime]
validation_root = "/tmp/cval"

[tests.dltest]
enabled = false
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.cluster.namespace, "staging")
        self.assertEqual(config.scheduling.batch_size, 2)
        self.assertEqual(config.scheduling.node_cooldown_seconds, 14400)
        self.assertEqual(config.monitoring.pending_start_timeout_seconds, 480)
        self.assertEqual(config.runtime.validation_root, "/tmp/cval")
        self.assertEqual(
            config.tests.registry.require("dltest").definition.settings["iterations"],
            100,
        )
        self.assertFalse(config.tests.registry.require("dltest").enabled)
        self.assertTrue(config.tests.registry.require("storage").enabled)
        self.assertEqual(config.job.git_ref, "0" * 40)

    def test_loads_scheduler_cooldown_and_pending_timeout_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[scheduling]
node_cooldown_seconds = 7200

[monitoring]
pending_start_timeout_seconds = 180
""",
                encoding="utf-8",
            )
            config = load_config(config_path)

        self.assertEqual(config.scheduling.node_cooldown_seconds, 7200)
        self.assertEqual(config.monitoring.pending_start_timeout_seconds, 180)

    def test_rejects_invalid_scheduler_cooldown_and_pending_timeout(self) -> None:
        cases = (
            ("[scheduling]\nnode_cooldown_seconds = -1\n", "node_cooldown_seconds"),
            ("[scheduling]\nnode_cooldown_seconds = true\n", "must be an integer"),
            (
                "[monitoring]\npending_start_timeout_seconds = 0\n",
                "pending_start_timeout_seconds",
            ),
            (
                "[monitoring]\npending_start_timeout_seconds = 1.5\n",
                "must be an integer",
            ),
        )
        for text, message in cases:
            with self.subTest(text=text), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "cval.toml"
                path.write_text(text, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    load_config(path)

    def test_load_config_runs_plugin_config_validation_for_all_declared_tests(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        for nccl_enabled in (True, False):
            with self.subTest(nccl_enabled=nccl_enabled), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                shutil.copytree(repository / "validation-tests", root / "validation-tests")
                descriptor = root / "validation-tests/nccl/test_config.toml"
                text = descriptor.read_text(encoding="utf-8")
                self.assertIn("iterations = 20", text)
                descriptor.write_text(
                    text.replace("iterations = 20", "iterations = 0", 1),
                    encoding="utf-8",
                )
                config_path = root / "cval.toml"
                config_path.write_text(
                    f'''[tests.storage]
enabled = true
config_path = "validation-tests/storage/test_config.toml"
[tests.nccl]
enabled = {str(nccl_enabled).lower()}
config_path = "validation-tests/nccl/test_config.toml"
[tests.dltest]
enabled = true
config_path = "validation-tests/dltest/test_config.toml"
''',
                    encoding="utf-8",
                )
                with patch("cval.config.REPO_ROOT", root), self.assertRaisesRegex(
                    RuntimeError, "invalid_iterations.*positive integer"
                ):
                    load_config(config_path)

    def test_nccl_evaluation_requires_exact_commit_and_image_digest(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shutil.copytree(repository / "validation-tests", root / "validation-tests")
            descriptor = root / "validation-tests/nccl/test_config.toml"
            descriptor.write_text(
                descriptor.read_text(encoding="utf-8").replace(
                    "evaluation_enabled = false", "evaluation_enabled = true"
                ),
                encoding="utf-8",
            )

            def write_config(git_ref: str, image: str) -> Path:
                path = root / "cval.toml"
                path.write_text(
                    f'''[job]
git_ref = "{git_ref}"
[job_template]
container_image = "{image}"
[tests.nccl]
enabled = true
config_path = "validation-tests/nccl/test_config.toml"
''',
                    encoding="utf-8",
                )
                return path

            with patch("cval.config.REPO_ROOT", root), self.assertRaisesRegex(
                ValueError, "exact lowercase 40-hex commit"
            ):
                load_config(write_config("main", "image@sha256:" + "b" * 64))
            with patch("cval.config.REPO_ROOT", root), self.assertRaisesRegex(
                ValueError, "pinned with @sha256"
            ):
                load_config(write_config("a" * 40, "image:latest"))
            with patch("cval.config.REPO_ROOT", root):
                loaded = load_config(
                    write_config("a" * 40, "image@sha256:" + "b" * 64)
                )
            self.assertTrue(
                loaded.tests.registry.require("nccl").definition.settings[
                    "evaluation_enabled"
                ]
            )

    def test_rejects_all_tests_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[tests.storage]
enabled = false
[tests.nccl]
enabled = false
[tests.dltest]
enabled = false
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "At least one test"):
                load_config(config_path)

    def test_config_to_dict_is_json_ready(self) -> None:
        data = config_to_dict(load_config())

        self.assertIsInstance(data["job"]["template_path"], str)
        self.assertEqual(
            data["tests"]["storage"]["config_path"],
            "validation-tests/storage/test_config.toml",
        )
        self.assertTrue(data["tests"]["storage"]["settings"]["install_fio"])

    def test_rejects_test_settings_in_global_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[tests.nccl]
enabled = true
iterations = 99
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "move test-specific settings"):
                load_config(config_path)

    def test_registry_loads_arbitrary_disabled_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test_descriptor(root, "smoke", order=50)

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

        self.assertEqual(len(registry.tests), 1)
        self.assertEqual(registry.tests[0].id, "smoke")
        self.assertFalse(registry.tests[0].enabled)

    def test_descriptor_settings_are_deeply_immutable_in_all_plugin_contexts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            test_dir = root / "validation-tests/smoke"
            test_dir.mkdir(parents=True)
            for name in ("setup.sh", "run-test.sh"):
                (test_dir / name).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "test_config.toml").write_text(
                """
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 1
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[settings]
labels = ["first", "second"]
[settings.nested]
mode = "strict"
items = [{name = "one"}, {name = "two"}]
[artifacts]
summary_filename = "summary.json"
""",
                encoding="utf-8",
            )
            registry = load_test_registry(
                {
                    "smoke": {
                        "enabled": True,
                        "config_path": "validation-tests/smoke/test_config.toml",
                    }
                },
                repo_root=root,
                include_defaults=False,
            )
            registered = registry.require("smoke")
            definition = registered.definition
            base = load_config()
            config = replace(base, tests=replace(base.tests, registry=registry))
            target = OperationalTarget(
                name="smoke",
                owner_test_id="smoke",
                baseline_test_type="smoke",
                status_test="smoke",
                operations=frozenset(
                    {BASELINE_BUILD, BASELINE_CLASSIFY, RESULTS_EXPORT}
                ),
            )
            contexts = (
                BaselineBuildContext(target, definition, config, 30, 3),
                BaselineClassificationContext(target, definition, config, 30),
                ExportContext(
                    target,
                    definition,
                    config,
                    (),
                    (),
                    "read-only-pod",
                    "read-only-namespace",
                    (),
                ),
            )

            for context in contexts:
                with self.subTest(context=type(context).__name__):
                    settings = context.definition.settings
                    with self.assertRaises(TypeError):
                        settings["nested"] = {}  # type: ignore[index]
                    with self.assertRaises(TypeError):
                        settings["nested"]["mode"] = "changed"  # type: ignore[index]
                    with self.assertRaises(TypeError):
                        settings["nested"]["items"][0]["name"] = "changed"  # type: ignore[index]
                    with self.assertRaises(AttributeError):
                        settings["labels"].append("third")

            serialized_registry = registry.to_dict()
            serialized_config = config_to_dict(config)
            json.dumps(serialized_registry)
            json.dumps(serialized_config)
            self.assertIsInstance(
                serialized_registry["smoke"]["settings"]["labels"], list
            )
            serialized_registry["smoke"]["settings"]["nested"]["mode"] = "copy"
            self.assertEqual(definition.settings["nested"]["mode"], "strict")

    def test_registry_rejects_config_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with self.assertRaisesRegex(ValueError, "escapes its allowed root"):
                load_test_registry(
                    {
                        "smoke": {
                            "enabled": False,
                            "config_path": "../outside/test_config.toml",
                        }
                    },
                    repo_root=root,
                    include_defaults=False,
                    require_enabled=False,
                )

    def test_registry_rejects_missing_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(FileNotFoundError, "config_path file not found"):
                load_test_registry(
                    {
                        "smoke": {
                            "enabled": False,
                            "config_path": "validation-tests/smoke/test_config.toml",
                        }
                    },
                    repo_root=Path(tmpdir),
                    include_defaults=False,
                    require_enabled=False,
                )

    def test_descriptor_rejects_removed_database_artifact_fields(self) -> None:
        for field in ("database_path", "results_db_path"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                self._write_test_descriptor(root, "smoke", order=40)
                descriptor = root / "validation-tests/smoke/test_config.toml"
                descriptor.write_text(
                    descriptor.read_text(encoding="utf-8").replace(
                        'summary_filename = "summary.json"',
                        f'{field} = "removed.db"\nsummary_filename = "summary.json"',
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, field):
                    load_test_registry(
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

    def test_registry_rejects_mismatched_test_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test_descriptor(root, "actual", order=10, directory_id="expected")
            with self.assertRaisesRegex(ValueError, "does not match"):
                load_test_registry(
                    {
                        "expected": {
                            "enabled": True,
                            "config_path": "validation-tests/expected/test_config.toml",
                        }
                    },
                    repo_root=root,
                    include_defaults=False,
                )

    def test_registry_rejects_duplicate_config_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test_descriptor(root, "first", order=10)
            shared = "validation-tests/first/test_config.toml"
            with self.assertRaisesRegex(ValueError, "use the same config_path"):
                load_test_registry(
                    {
                        "first": {"enabled": True, "config_path": shared},
                        "second": {"enabled": False, "config_path": shared},
                    },
                    repo_root=root,
                    include_defaults=False,
                )

    def test_registry_rejects_duplicate_enabled_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test_descriptor(root, "first", order=10)
            self._write_test_descriptor(root, "second", order=10)
            with self.assertRaisesRegex(ValueError, "unique execution order"):
                load_test_registry(
                    {
                        test_id: {
                            "enabled": True,
                            "config_path": f"validation-tests/{test_id}/test_config.toml",
                        }
                        for test_id in ("first", "second")
                    },
                    repo_root=root,
                    include_defaults=False,
                )

    def test_registry_rejects_missing_new_test_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "enabled is required"):
                load_test_registry(
                    {
                        "smoke": {
                            "config_path": "validation-tests/smoke/test_config.toml"
                        }
                    },
                    repo_root=Path(tmpdir),
                    include_defaults=False,
                    require_enabled=False,
                )

    def test_resource_quantity_parser(self) -> None:
        self.assertEqual(parse_resource_quantity("1500m"), parse_resource_quantity("1.5"))
        self.assertEqual(parse_resource_quantity("1Gi"), 1024**3)
        with self.assertRaisesRegex(ValueError, "Invalid Kubernetes resource quantity"):
            parse_resource_quantity("many")

    def test_config_rejects_shared_gpu_under_provisioning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[job_template]
gpu_count = "4"
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "does not cover enabled test 'nccl'"):
                load_config(config_path)

    def test_config_rejects_monitoring_shorter_than_sequential_deadlines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[monitoring]
timeout_seconds = 3000
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "sequential test timeouts"):
                load_config(config_path)

    def test_rejects_nonfinite_or_invalid_baseline_controls(self) -> None:
        variants = (
            "robust_z_threshold = nan",
            "dl_degraded_metric_fraction = nan",
            "dl_degraded_metric_fraction = 1.1",
            "dl_degraded_severity_pct = inf",
            "min_samples = 0",
            "window_days = 0",
            "build_interval_seconds = 0",
        )
        for value in variants:
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmpdir:
                config_path = Path(tmpdir) / "cval.toml"
                config_path.write_text(f"[baseline]\n{value}\n", encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_config(config_path)

    @staticmethod
    def _write_test_descriptor(
        root: Path,
        test_id: str,
        *,
        order: int,
        directory_id: str | None = None,
    ) -> None:
        directory_id = directory_id or test_id
        test_dir = root / "validation-tests" / directory_id
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "run-test.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "test_config.toml").write_text(
            f"""
schema_version = "cval.test.v1"

[test]
id = "{test_id}"
display_name = "{test_id.title()}"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30

[artifacts]
summary_filename = "summary.json"
""",
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()