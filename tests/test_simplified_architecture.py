from __future__ import annotations

import unittest
from pathlib import Path

from cval.config import config_to_dict, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]


class SimplifiedArchitectureTests(unittest.TestCase):
    def test_rejected_production_packages_and_docs_are_absent(self) -> None:
        for relative in (
            "cval/health",
            "cval/evaluator",
            "cval/storage/run_history.py",
            "cval/storage/per_test_results.py",
            "cval/validation/ingestion.py",
            "cval/validation/compatibility.py",
            "docs/run-history.md",
            "docs/u8-health-engine-design-report.md",
            "docs/u11-evaluator-rollout.md",
            "docs/database-schema-v3.md",
        ):
            with self.subTest(relative=relative):
                self.assertFalse((REPO_ROOT / relative).exists())

    def test_config_has_no_alternate_write_or_state_surfaces(self) -> None:
        payload = config_to_dict(load_config())
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

    def test_nccl_is_export_only_in_generic_operational_plugins(self) -> None:
        descriptor = (
            REPO_ROOT / "validation-tests/nccl/test_config.toml"
        ).read_text(encoding="utf-8")
        plugin = (REPO_ROOT / "validation-tests/nccl/plugin.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('capabilities = ["config", "export"]', descriptor)
        self.assertIn('capabilities = frozenset({"config", "export"})', plugin)
        self.assertNotIn("def build_baseline", plugin)
        self.assertNotIn("def classify", plugin)
        generic_build = (REPO_ROOT / "cval/baselines/build.py").read_text(
            encoding="utf-8"
        )
        generic_classify = (REPO_ROOT / "cval/baselines/classify.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("build_nccl_baseline", generic_build)
        self.assertNotIn("_node_values_nccl", generic_classify)


if __name__ == "__main__":
    unittest.main()
