from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cval.validation.path_preflight import preflight_run_paths


REGISTRY = '{"storage":{"enabled":true}}'


class RunPathPreflightTests(unittest.TestCase):
    def test_rejects_symlinked_global_log_ancestor_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            outside = Path(tmpdir) / "outside"
            outside.mkdir()
            root.mkdir()
            (root / "logs").symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(ValueError, "symlink"):
                preflight_run_paths(
                    root, "node-a", "node-a-123", registry_json=REGISTRY
                )

            self.assertEqual(list(outside.iterdir()), [])

    def test_accepts_absent_canonical_paths_without_creating_them(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"

            preflight_run_paths(
                root, "node-a", "node-a-123", registry_json=REGISTRY
            )

            self.assertFalse(root.exists())

    def test_rejects_dynamic_test_symlink_from_runtime_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            outside = Path(tmpdir) / "outside"
            outside.mkdir()
            (root / "validation_tests").mkdir(parents=True)
            (root / "validation_tests/smoke").symlink_to(
                outside,
                target_is_directory=True,
            )

            with self.assertRaisesRegex(ValueError, "symlink"):
                preflight_run_paths(
                    root,
                    "node-a",
                    "node-a-123",
                    registry_json='{"smoke":{"enabled":true}}',
                )

            self.assertEqual(list(outside.iterdir()), [])

    def test_rejects_symlinked_global_stdout_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            outside = Path(tmpdir) / "outside.log"
            log_dir = root / "logs/job_logs/node-a/node-a-123"
            log_dir.mkdir(parents=True)
            (log_dir / "stdout.log").symlink_to(outside)

            with self.assertRaisesRegex(ValueError, "symlink"):
                preflight_run_paths(
                    root, "node-a", "node-a-123", registry_json=REGISTRY
                )

            self.assertFalse(outside.exists())

    def test_rejects_missing_runtime_registry_without_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "registry is required"):
                preflight_run_paths(
                    Path(tmpdir) / "data",
                    "node-a",
                    "node-a-123",
                    registry_json="",
                )


if __name__ == "__main__":
    unittest.main()
