from __future__ import annotations

import io
import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

from cval.validation.supervisor import (
    reserve_secure_run_layout,
    supervise_validation_run,
)


REGISTRY = json.dumps(
    {
        "smoke": {
            "enabled": True,
            "config_path": "validation-tests/smoke/test_config.toml",
            "order": 10,
        }
    },
    sort_keys=True,
    separators=(",", ":"),
)


class SecureRunSupervisorTests(unittest.TestCase):
    def test_reservation_uses_deterministic_owner_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            validation_root = Path(tmpdir) / "data"
            validation_root.mkdir()
            layout = reserve_secure_run_layout(
                validation_root,
                "node-a",
                "node-a-123",
                registry_json=REGISTRY,
            )
            try:
                directories = [
                    layout.run_dir_fd,
                    *layout.test_fds["smoke"],
                ]
                for descriptor in directories:
                    value = os.fstat(descriptor)
                    self.assertEqual(value.st_uid, os.geteuid())
                    self.assertEqual(stat.S_IMODE(value.st_mode), 0o700)
                for descriptor in layout.global_file_fds.values():
                    value = os.fstat(descriptor)
                    self.assertEqual(value.st_uid, os.geteuid())
                    self.assertEqual(stat.S_IMODE(value.st_mode), 0o600)
                test_log_dir = validation_root / "logs/smoke/node-a/node-a-123"
                for path in test_log_dir.iterdir():
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            finally:
                layout.close()

    def test_rejects_ancestor_symlink_before_reservation_without_external_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            outside = root / "outside"
            validation_root.mkdir()
            outside.mkdir()
            (validation_root / "logs").symlink_to(outside, target_is_directory=True)

            with self.assertRaises(OSError):
                reserve_secure_run_layout(
                    validation_root,
                    "node-a",
                    "node-a-123",
                    registry_json=REGISTRY,
                )

            self.assertEqual(list(outside.iterdir()), [])

    def test_ancestor_swap_during_creation_never_redirects_external_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            outside = root / "outside"
            validation_root.mkdir()
            outside.mkdir()
            swapped = False

            def swap_after_logs(relative: tuple[str, ...], _descriptor: int) -> None:
                nonlocal swapped
                if relative == ("logs",) and not swapped:
                    (validation_root / "logs").rename(validation_root / "logs-retained")
                    (validation_root / "logs").symlink_to(
                        outside,
                        target_is_directory=True,
                    )
                    swapped = True

            with self.assertRaises(OSError):
                reserve_secure_run_layout(
                    validation_root,
                    "node-a",
                    "node-a-123",
                    registry_json=REGISTRY,
                    directory_observer=swap_after_logs,
                )

            self.assertTrue(swapped)
            self.assertEqual(list(outside.iterdir()), [])
            self.assertTrue(
                (
                    validation_root
                    / "logs-retained/job_logs/node-a/node-a-123/.run-active"
                ).is_file()
            )

    def test_runner_ancestor_swap_writes_only_through_retained_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/smoke"
            validation_root = root / "data"
            outside = root / "outside"
            test_dir.mkdir(parents=True)
            validation_root.mkdir()
            outside.mkdir()
            (test_dir / "setup.sh").write_text(
                "#!/bin/bash\nset -euo pipefail\n",
                encoding="utf-8",
            )
            (test_dir / "run-test.sh").write_text(
                """#!/bin/bash
set -euo pipefail
mv "$SWAP_VALIDATION_ROOT/validation_tests" "$SWAP_VALIDATION_ROOT/validation_tests-retained"
ln -s "$SWAP_OUTSIDE" "$SWAP_VALIDATION_ROOT/validation_tests"
printf 'anchored\n' > "$CVAL_TEST_OUTPUT_DIR/artifact.txt"
printf '{"ok":true}\n' > "$CVAL_TEST_SUMMARY_FILE"
""",
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 10
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            environment = os.environ | {
                "CVAL_REPO_DIR": str(Path(__file__).resolve().parents[1]),
                "CVAL_TEST_REPO_ROOT": str(repo),
                "CVAL_VALIDATION_TESTS_DIR": str(repo / "validation-tests"),
                "CVAL_VALIDATION_ROOT": str(validation_root),
                "CVAL_TEST_REGISTRY_JSON": REGISTRY,
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_NODE": "node-a",
                "GCRNODE": "node-a",
                "CVAL_TIMESTAMP": "123",
                "GCRTIME": "123",
                "CVAL_IMAGE_NAME": "test-image",
                "CVAL_PYTORCH_VERSION": "test",
                "CVAL_CUDA_VERSION": "test",
                "CVAL_GIT_REF": "test-ref",
                "SWAP_VALIDATION_ROOT": str(validation_root),
                "SWAP_OUTSIDE": str(outside),
            }

            captured_stdout = io.BytesIO()
            captured_stderr = io.BytesIO()
            try:
                return_code = supervise_validation_run(
                    environment=environment,
                    runner_command=(sys.executable, "-m", "cval.validation.runner"),
                    db_update_command=None,
                    validation_tests_dir=repo / "validation-tests",
                    stdout=captured_stdout,
                    stderr=captured_stderr,
                )
            except OSError:
                pass
            else:
                self.fail(
                    "ancestor replacement was not detected; "
                    f"return_code={return_code}; stdout={captured_stdout.getvalue()!r}; "
                    f"stderr={captured_stderr.getvalue()!r}"
                )

            self.assertEqual(list(outside.iterdir()), [])
            retained_run = (
                validation_root
                / "validation_tests-retained/smoke/runs/node-a/node-a-123"
            )
            self.assertEqual(
                (retained_run / "artifacts/artifact.txt").read_text(encoding="utf-8"),
                "anchored\n",
            )
            result = json.loads((retained_run / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(
                result["artifacts"],
                str(
                    validation_root
                    / "validation_tests/smoke/runs/node-a/node-a-123/artifacts"
                ),
            )
            self.assertTrue(
                (
                    validation_root
                    / "logs/job_logs/node-a/node-a-123/.run-active"
                ).is_file()
            )


if __name__ == "__main__":
    unittest.main()
