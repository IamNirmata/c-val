from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cval.config import load_config
from cval.validation.plugins import (
    PluginLoadError,
    load_registered_plugin,
    validate_registry_plugins,
)
from cval.validation.registry import load_test_registry


_VALID_PLUGIN = '''
CVAL_PLUGIN_API = "cval.plugin.v1"
class ExamplePlugin:
    plugin_id = "{test_id}"
    capabilities = frozenset({{"ingest"}})
    def validate_schema(self, connection, allow_missing):
        return False
    def ingest(self, context):
        return context
PLUGIN = ExamplePlugin()
'''


class IngestionProtocolTests(unittest.TestCase):
    def test_loads_all_builtin_ingestion_plugins(self) -> None:
        registry = load_config().tests.registry

        loaded = validate_registry_plugins(registry.tests)

        self.assertEqual(loaded, ("storage", "nccl", "dltest"))
        for test_id in loaded:
            plugin = load_registered_plugin(registry.require(test_id))
            self.assertEqual(plugin.plugin_id, test_id)
            self.assertEqual(
                plugin.capabilities,
                frozenset({"config", "ingest", "health"}),
            )

    def test_rejects_plugin_identity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = self._registry(
                root,
                plugin_text=_VALID_PLUGIN.format(test_id="other"),
            )

            with self.assertRaisesRegex(PluginLoadError, "plugin_id"):
                load_registered_plugin(registry.require("smoke"))

    def test_rejects_declared_capability_without_method(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = self._registry(
                root,
                plugin_text='''
CVAL_PLUGIN_API = "cval.plugin.v1"
class ExamplePlugin:
    plugin_id = "smoke"
    capabilities = frozenset({"ingest"})
PLUGIN = ExamplePlugin()
''',
            )

            with self.assertRaisesRegex(PluginLoadError, "missing method"):
                load_registered_plugin(registry.require("smoke"))

    def test_normal_validation_does_not_import_disabled_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = self._registry(
                root,
                enabled=False,
                plugin_text='raise RuntimeError("must not import")\n',
            )

            self.assertEqual(validate_registry_plugins(registry.enabled), ())
            with self.assertRaisesRegex(PluginLoadError, "must not import"):
                validate_registry_plugins(registry.tests)

    @staticmethod
    def _registry(
        root: Path,
        *,
        plugin_text: str,
        enabled: bool = True,
    ):
        test_dir = root / "validation-tests" / "smoke"
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        (test_dir / "run-test.sh").write_text(
            "#!/bin/bash\nexit 0\n", encoding="utf-8"
        )
        (test_dir / "plugin.py").write_text(plugin_text, encoding="utf-8")
        (test_dir / "test_config.toml").write_text(
            '''
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 10
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = ["ingest"]
''',
            encoding="utf-8",
        )
        return load_test_registry(
            {
                "smoke": {
                    "enabled": enabled,
                    "config_path": "validation-tests/smoke/test_config.toml",
                }
            },
            repo_root=root,
            include_defaults=False,
            require_enabled=enabled,
        )


if __name__ == "__main__":
    unittest.main()
