from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cval.validation.plugins import PluginLoadError, load_registered_plugin, validate_registry_plugins
from cval.validation.registry import load_test_registry


_VALID_PLUGIN = '''
from cval.health.engine import metric_specs_from_definition
CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "smoke"
    health_policy_version = "smoke.health.v1"
    capabilities = frozenset({"health"})
    def metric_specs(self, definition):
        return metric_specs_from_definition(definition)
    def load_observations(self, context):
        return ()
PLUGIN = Plugin()
'''


class HealthRegistryTests(unittest.TestCase):
    def _write(
        self,
        root: Path,
        *,
        capabilities: str = 'capabilities = ["health"]',
        health: str | None = None,
        settings: str = "",
        plugin_text: str = _VALID_PLUGIN,
    ):
        test_dir = root / "validation-tests/smoke"
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "run-test.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "plugin.py").write_text(plugin_text, encoding="utf-8")
        health = health if health is not None else '''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "declarative"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["image_name"]
auto_activate = false
[[health.metrics]]
name = "metric"
source = "source"
direction = "low_bad"
tolerance_pct = 5.0
'''
        (test_dir / "test_config.toml").write_text(
            f'''
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 1
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
{settings}
[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
health_classes_db_path = "validation_tests/smoke/smoke_health_classes.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
{capabilities}
{health}
''',
            encoding="utf-8",
        )
        return load_test_registry(
            {
                "smoke": {
                    "enabled": True,
                    "config_path": "validation-tests/smoke/test_config.toml",
                }
            },
            repo_root=root,
            include_defaults=False,
        )

    def test_enabled_health_requires_health_capability(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "requires plugin health capability"):
                self._write(
                    Path(tmpdir),
                    capabilities='capabilities = ["ingest"]',
                )

    def test_health_capability_requires_enabled_health_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "requires enabled"):
                self._write(Path(tmpdir), health="")

    def test_combination_factor_must_be_common_or_top_level_scalar_setting(self) -> None:
        invalid_health = '''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "declarative"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["missing"]
auto_activate = false
[[health.metrics]]
name = "metric"
source = "source"
direction = "low_bad"
tolerance_pct = 5.0
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "not common fields"):
                self._write(Path(tmpdir), health=invalid_health)

    def test_duplicate_metric_sources_are_rejected(self) -> None:
        invalid_health = '''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "declarative"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["image_name"]
auto_activate = false
[[health.metrics]]
name = "first"
source = "same"
direction = "low_bad"
tolerance_pct = 5.0
[[health.metrics]]
name = "second"
source = "same"
direction = "high_bad"
tolerance_pct = 5.0
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "Duplicate health metric source"):
                self._write(Path(tmpdir), health=invalid_health)

    def test_custom_health_plugin_requires_custom_hooks(self) -> None:
        custom_health = '''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "custom"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["image_name"]
auto_activate = false
[[health.metrics]]
name = "metric"
source = "source"
direction = "low_bad"
tolerance_pct = 5.0
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = self._write(Path(tmpdir), health=custom_health)
            with self.assertRaisesRegex(PluginLoadError, "Custom health adapter"):
                load_registered_plugin(registry.require("smoke"))

    def test_metric_specs_must_exactly_match_descriptor(self) -> None:
        bad_plugin = '''
from cval.health.models import MetricSpec
CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "smoke"
    health_policy_version = "smoke.health.v1"
    capabilities = frozenset({"health"})
    def metric_specs(self, definition):
        return (MetricSpec("forged", "source", "low_bad", 5.0),)
    def load_observations(self, context):
        return ()
PLUGIN = Plugin()
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = self._write(Path(tmpdir), plugin_text=bad_plugin)
            with self.assertRaisesRegex(PluginLoadError, "do not match"):
                validate_registry_plugins(registry.tests)

    def test_health_policy_version_must_match_descriptor(self) -> None:
        bad_plugin = _VALID_PLUGIN.replace(
            'health_policy_version = "smoke.health.v1"',
            'health_policy_version = "smoke.health.v2"',
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = self._write(Path(tmpdir), plugin_text=bad_plugin)
            with self.assertRaisesRegex(PluginLoadError, "health_policy_version"):
                load_registered_plugin(registry.require("smoke"))

    def test_custom_candidate_builder_is_rejected_by_frozen_contract(self) -> None:
        custom_health = '''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "custom"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["image_name"]
auto_activate = false
[[health.metrics]]
name = "metric"
source = "source"
direction = "low_bad"
tolerance_pct = 5.0
'''
        custom_plugin = _VALID_PLUGIN.replace(
            "    def load_observations(self, context):\n        return ()\n",
            "    def load_observations(self, context):\n"
            "        return ()\n"
            "    def build_candidate(self, context, observations):\n"
            "        raise AssertionError('must not run')\n"
            "    def classify(self, context, baseline, observations, base):\n"
            "        return base\n",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = self._write(
                Path(tmpdir),
                health=custom_health,
                plugin_text=custom_plugin,
            )
            with self.assertRaisesRegex(PluginLoadError, "framework-owned"):
                load_registered_plugin(registry.require("smoke"))

    def test_valid_declarative_health_plugin_passes_registry_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = self._write(Path(tmpdir))
            self.assertEqual(validate_registry_plugins(registry.tests), ("smoke",))

    def test_nonfinite_health_numbers_are_rejected(self) -> None:
        variants = (
            "robust_z_threshold = nan\n",
            "",
            "",
        )
        for index, extra in enumerate(variants):
            tolerance = "nan" if index == 1 else "5.0"
            weight = "inf" if index == 2 else "1.0"
            health = f'''
[health]
enabled = true
policy_version = "smoke.health.v1"
strategy = "declarative"
min_samples = 3
min_new_results = 1
target_class_count = 5
combination_factors = ["image_name"]
auto_activate = false
{extra}[[health.metrics]]
name = "metric"
source = "source"
direction = "low_bad"
tolerance_pct = {tolerance}
weight = {weight}
'''
            with self.subTest(index=index), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaisesRegex(ValueError, "finite"):
                    self._write(Path(tmpdir), health=health)


if __name__ == "__main__":
    unittest.main()
