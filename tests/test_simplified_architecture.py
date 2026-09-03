from __future__ import annotations

import unittest
from pathlib import Path

from cval.config import config_to_dict, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]


class SimplifiedArchitectureTests(unittest.TestCase):
    def test_removed_derived_architecture_is_absent(self) -> None:
        for relative in (
            "cval/health",
            "cval/evaluator",
            "cval/baselines",
            "cval/nccl_eval",
            "cval/storage/classification_status.py",
            "cval/storage/classification_legacy.py",
            "cval/storage/run_history.py",
            "cval/storage/per_test_results.py",
            "cval/validation/ingestion.py",
            "cval/validation/compatibility.py",
            "deploy/cval-evaluator",
            "scripts/cval-baseline-build.sh",
            "scripts/cval-baseline-classify.sh",
            "scripts/cval-baseline-common.sh",
            "scripts/cval-nccl-postgres-preflight.sh",
            "scripts/cval-split-classifications.py",
            "docs/run-history.md",
            "docs/u8-health-engine-design-report.md",
            "docs/u11-evaluator-rollout.md",
            "docs/database-schema-v3.md",
        ):
            with self.subTest(relative=relative):
                self.assertFalse((REPO_ROOT / relative).exists())

    def test_config_has_no_alternate_write_or_state_surfaces(self) -> None:
        payload = config_to_dict(load_config())
        self.assertNotIn("baseline", payload)
        self.assertNotIn("health_evaluator", payload)
        self.assertNotIn("run_history_enabled", payload["storage"])
        self.assertNotIn("run_history_db_path", payload["storage"])
        self.assertNotIn("per_test_ingestion_enabled", payload["storage"])

    def test_builtin_descriptors_have_no_common_or_health_db_paths(self) -> None:
        for test_id in ("storage", "nccl", "dltest"):
            with self.subTest(test_id=test_id):
                text = (
                    REPO_ROOT / f"validation-tests/{test_id}/test_config.toml"
                ).read_text(encoding="utf-8")
                self.assertNotIn("results_db_path", text)
                self.assertNotIn("health_classes_db_path", text)
                self.assertNotIn("[health]", text)
                self.assertNotIn('"ingest"', text)
                self.assertNotIn('"health"', text)

    def test_builtin_plugins_are_config_and_raw_export_only(self) -> None:
        for test_id in ("storage", "nccl", "dltest"):
            with self.subTest(test_id=test_id):
                descriptor = (
                    REPO_ROOT / f"validation-tests/{test_id}/test_config.toml"
                ).read_text(encoding="utf-8")
                plugin = (
                    REPO_ROOT / f"validation-tests/{test_id}/plugin.py"
                ).read_text(encoding="utf-8")
                self.assertIn('capabilities = ["config", "export"]', descriptor)
                self.assertIn(
                    'capabilities = frozenset({"config", "export"})', plugin
                )
                self.assertNotIn("def build_baseline", plugin)
                self.assertNotIn("def classify", plugin)

    def test_postgresql_extra_and_package_data_are_absent(self) -> None:
        metadata = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")

        self.assertNotIn("psycopg", metadata)
        self.assertNotIn("cval.nccl_eval", metadata)


if __name__ == "__main__":
    unittest.main()
