from __future__ import annotations

import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from cval.config import load_config
from cval.validation.registry import load_test_registry
from cval.validation.results import ValidationResultV2, load_validation_result
from cval.validation.runner import run_validation_tests


class ValidationRunnerTests(unittest.TestCase):
    def test_runs_dynamic_tests_in_order_and_preserves_canonical_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "first", order=10, exit_code=0)
            self._write_test(root, "failing", order=20, exit_code=7)
            self._write_test(root, "after", order=30, exit_code=0)
            self._write_test(root, "disabled", order=40, exit_code=0)
            registry = self._registry(
                root,
                {
                    "first": True,
                    "failing": True,
                    "after": True,
                    "disabled": False,
                },
            )
            validation_root = root / "data"
            stdout = io.StringIO()
            stderr = io.StringIO()

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=stdout,
                stderr=stderr,
            )

            result_path = (
                validation_root
                / "logs"
                / "job_logs"
                / "node-a"
                / "node-a-123"
                / "result.json"
            )
            parsed = load_validation_result(result_path)
            events = self._read_jsonl(result_path.parent / "events.jsonl")
            legacy_env = (result_path.parent / "result.env").read_text(
                encoding="utf-8"
            )

            first_run = (
                validation_root
                / "validation_tests"
                / "first"
                / "runs"
                / "node-a"
                / "node-a-123"
            )
            disabled_log = (
                validation_root
                / "logs"
                / "disabled"
                / "node-a"
                / "node-a-123"
            )

            self.assertIsInstance(parsed, ValidationResultV2)
            self.assertEqual(result["overall"], "fail")
            self.assertEqual(result["tests"]["first"]["status"], "pass")
            self.assertEqual(result["tests"]["failing"]["status"], "fail")
            self.assertEqual(result["tests"]["after"]["status"], "pass")
            self.assertEqual(result["tests"]["disabled"]["phase"], "not_selected")
            self.assertNotIn("run-disabled", stdout.getvalue())
            self.assertEqual(
                [event["test"] for event in events if event["event"] == "test_started"],
                ["first", "failing", "after"],
            )
            self.assertEqual(events[0]["event"], "run_started")
            self.assertEqual(events[-1]["event"], "run_finished")
            self.assertTrue((first_run / "result.json").is_file())
            self.assertTrue((first_run / "summary.json").is_file())
            self.assertTrue((first_run / "artifacts" / "artifact.txt").is_file())
            self.assertFalse(disabled_log.exists())
            self.assertIn("GCRRESULT1=incomplete", legacy_env)
            self.assertIn("overall_result=fail", legacy_env)
            self.assertIn("run-first", stdout.getvalue())
            self.assertIn("run-after", stdout.getvalue())
            self.assertIn("err-failing", stderr.getvalue())
            self.assertIn("CVAL_EVENT", stdout.getvalue())
            self.assertIn("run-first", (result_path.parent / "stdout.log").read_text())
            self.assertIn("err-failing", (result_path.parent / "stderr.log").read_text())
            job_log = (result_path.parent / "job.log").read_text(encoding="utf-8")
            self.assertIn("run-first", job_log)
            self.assertIn("err-failing", job_log)
            self.assertEqual(list(result_path.parent.glob(".*.tmp-*")), [])

    def test_setup_failure_does_not_run_workload_or_stop_later_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "broken-setup", order=10, setup_exit=4)
            self._write_test(root, "later", order=20)
            registry = self._registry(
                root,
                {"broken-setup": True, "later": True},
            )
            validation_root = root / "data"

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["tests"]["broken-setup"]["phase"], "setup_failed")
        self.assertEqual(result["tests"]["broken-setup"]["exit_code"], 4)
        self.assertEqual(result["tests"]["later"]["status"], "pass")
        self.assertEqual(result["overall"], "fail")

    def test_all_disabled_registry_completes_without_executing_scripts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "disabled", order=10)
            registry = self._registry(root, {"disabled": False})
            validation_root = root / "data"

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["overall"], "incomplete")
        self.assertIsNotNone(result["completed_at"])
        self.assertEqual(result["tests"]["disabled"]["phase"], "not_selected")

    def test_timeout_terminates_test_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "slow", order=10, timeout_seconds=1)
            (root / "validation-tests" / "slow" / "run-test.sh").write_text(
                "#!/bin/bash\nwhile :; do :; done\n",
                encoding="utf-8",
            )
            self._write_test(root, "later", order=20)
            registry = self._registry(root, {"slow": True, "later": True})

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["tests"]["slow"]["phase"], "timed_out")
        self.assertEqual(result["tests"]["slow"]["status"], "fail")
        self.assertEqual(result["tests"]["later"]["status"], "pass")

    def test_timeout_kills_term_ignoring_background_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "daemon", order=10, timeout_seconds=1)
            (root / "validation-tests" / "daemon" / "run-test.sh").write_text(
                """#!/bin/bash
(trap '' TERM; while :; do :; done) &
exit 0
""",
                encoding="utf-8",
            )
            registry = self._registry(root, {"daemon": True})
            started = time.monotonic()

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            elapsed = time.monotonic() - started

        self.assertEqual(result["tests"]["daemon"]["phase"], "timed_out")
        self.assertLess(elapsed, 4.0)

    def test_runtime_registry_executes_synthetic_fourth_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=999)
            environment = self._environment(root, root / "data")
            environment.update(
                {
                    "CVAL_VALIDATION_TESTS_DIR": str(root / "validation-tests"),
                    "CVAL_TEST_REGISTRY_JSON": json.dumps(
                        {
                            "smoke": {
                                "enabled": True,
                                "config_path": (
                                    "validation-tests/smoke/test_config.toml"
                                ),
                                "order": 999,
                            }
                        }
                    ),
                }
            )

            result = run_validation_tests(
                config=load_config(),
                environ=environment,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(list(result["tests"]), ["smoke"])
        self.assertEqual(result["tests"]["smoke"]["status"], "pass")

    def test_pass_fail_test_needs_only_descriptor_and_global_stanza(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=40)
            registry = self._registry(root, {"smoke": True})

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(list(result["tests"]), ["smoke"])
        self.assertEqual(result["schema_version"], "cval.results")
        self.assertEqual(result["tests"]["smoke"]["status"], "pass")
        self.assertIsNone(registry.require("smoke").definition.plugin)

    def test_rejects_unsafe_run_id_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            environment = self._environment(root, root / "data")
            environment["CVAL_RUN_ID"] = "../escape"

            with self.assertRaisesRegex(ValueError, "Invalid run_id"):
                run_validation_tests(
                    config=load_config(),
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertFalse((root / "escape").exists())

    def test_rejects_reused_run_id_without_mixing_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            environment = self._environment(root, root / "data")
            first = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=environment,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(FileExistsError, "refusing run_id reuse"):
                run_validation_tests(
                    config=load_config(),
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

        self.assertEqual(first["tests"]["smoke"]["status"], "pass")

    def test_accepts_manifest_preacquired_marker_and_external_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            environment = self._environment(root, root / "data")
            environment["CVAL_EXTERNAL_GLOBAL_LOGGING"] = "true"
            environment["CVAL_RUN_MARKER_PREACQUIRED"] = "true"
            log_dir = root / "data/logs/job_logs/node-a/node-a-123"
            log_dir.mkdir(parents=True)
            marker = log_dir / ".run-active"
            marker.write_text("pod=test\n", encoding="utf-8")
            (log_dir / "stdout.log").write_text("pre-run\n", encoding="utf-8")
            (log_dir / "stderr.log").touch()
            (log_dir / "job.log").write_text("pre-run\n", encoding="utf-8")

            result = run_validation_tests(
                config=load_config(),
                registry=registry,
                environ=environment,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

            self.assertTrue(marker.exists())
            self.assertEqual(
                (log_dir / "stdout.log").read_text(encoding="utf-8"),
                "pre-run\n",
            )

        self.assertEqual(result["overall"], "pass")

    def test_rejects_preexisting_per_test_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            stale_dir = (
                root
                / "data/validation_tests/smoke/runs/node-a/node-a-123"
            )
            stale_dir.mkdir(parents=True)
            stale_file = stale_dir / "stale.txt"
            stale_file.write_text("old evidence\n", encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, "Per-test run evidence"):
                run_validation_tests(
                    config=load_config(),
                    registry=registry,
                    environ=self._environment(root, root / "data"),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertEqual(stale_file.read_text(encoding="utf-8"), "old evidence\n")

    def test_rejects_symlinked_global_evidence_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            validation_root = root / "data"
            external = root / "outside"
            external.mkdir()
            (validation_root / "logs/job_logs").mkdir(parents=True)
            (validation_root / "logs/job_logs/node-a").symlink_to(
                external,
                target_is_directory=True,
            )

            with self.assertRaisesRegex(ValueError, "symlink"):
                run_validation_tests(
                    config=load_config(),
                    registry=registry,
                    environ=self._environment(root, validation_root),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertEqual(list(external.iterdir()), [])

    def test_rejects_symlinked_global_log_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            validation_root = root / "data"
            log_dir = validation_root / "logs/job_logs/node-a/node-a-123"
            log_dir.mkdir(parents=True)
            outside = root / "outside.log"
            (log_dir / "stdout.log").symlink_to(outside)

            with self.assertRaisesRegex(ValueError, "symlink"):
                run_validation_tests(
                    config=load_config(),
                    registry=registry,
                    environ=self._environment(root, validation_root),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertFalse(outside.exists())

    def test_sigterm_persists_interrupted_and_kills_workload_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "signal-test", order=10, timeout_seconds=30)
            (root / "validation-tests/signal-test/run-test.sh").write_text(
                """#!/bin/bash
echo $$ > "$CVAL_TEST_OUTPUT_DIR/pid"
while :; do :; done
""",
                encoding="utf-8",
            )
            environment = self._environment(root, root / "data")
            environment.update(
                {
                    "CVAL_VALIDATION_TESTS_DIR": str(root / "validation-tests"),
                    "CVAL_TEST_REGISTRY_JSON": json.dumps(
                        {
                            "signal-test": {
                                "enabled": True,
                                "config_path": (
                                    "validation-tests/signal-test/test_config.toml"
                                ),
                                "order": 10,
                            }
                        }
                    ),
                }
            )
            process = subprocess.Popen(
                [sys.executable, "-m", "cval.validation.runner"],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            result_path = root / "data/logs/job_logs/node-a/node-a-123/result.json"
            pid_path = (
                root
                / "data/validation_tests/signal-test/runs/node-a/node-a-123/artifacts/pid"
            )
            try:
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    if result_path.is_file() and pid_path.is_file():
                        state = json.loads(result_path.read_text(encoding="utf-8"))
                        if state["tests"]["signal-test"]["phase"] == "running":
                            break
                    time.sleep(0.02)
                else:
                    self.fail("runner did not reach running phase")

                workload_pid = int(pid_path.read_text(encoding="utf-8"))
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=6.0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=2.0)

            state = json.loads(result_path.read_text(encoding="utf-8"))
            with self.assertRaises(ProcessLookupError):
                os.kill(workload_pid, 0)

        self.assertEqual(process.returncode, 130)
        self.assertEqual(state["tests"]["signal-test"]["phase"], "interrupted")

    @staticmethod
    def _environment(repo_root: Path, validation_root: Path) -> dict[str, str]:
        return os.environ | {
            "CVAL_REPO_DIR": str(repo_root),
            "CVAL_VALIDATION_ROOT": str(validation_root),
            "CVAL_RUN_ID": "node-a-123",
            "CVAL_NODE": "node-a",
            "CVAL_TIMESTAMP": "123",
            "CVAL_IMAGE_NAME": "test-image",
            "CVAL_PYTORCH_VERSION": "2.8.0",
            "CVAL_CUDA_VERSION": "12.9",
            "CVAL_GIT_REF": "abc123",
        }

    @staticmethod
    def _registry(root: Path, activations: dict[str, bool]):
        return load_test_registry(
            {
                test_id: {
                    "enabled": enabled,
                    "config_path": f"validation-tests/{test_id}/test_config.toml",
                }
                for test_id, enabled in activations.items()
            },
            repo_root=root,
            include_defaults=False,
            require_enabled=False,
        )

    @staticmethod
    def _write_test(
        root: Path,
        test_id: str,
        *,
        order: int,
        exit_code: int = 0,
        setup_exit: int = 0,
        timeout_seconds: int = 30,
    ) -> None:
        test_dir = root / "validation-tests" / test_id
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text(
            f'#!/bin/bash\necho "setup-{test_id}"\nexit {setup_exit}\n',
            encoding="utf-8",
        )
        (test_dir / "run-test.sh").write_text(
            f"""#!/bin/bash
set -euo pipefail
echo "run-{test_id}"
echo "err-{test_id}" >&2
python3 - "$CVAL_RESULT_JSON_FILE" "$CVAL_TEST_ID" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["tests"][sys.argv[2]]["phase"] == "running"
PY
printf '{{"test":"%s"}}\n' "$CVAL_TEST_ID" > "$CVAL_TEST_SUMMARY_FILE"
printf 'artifact-%s\n' "$CVAL_TEST_ID" > "$CVAL_TEST_OUTPUT_DIR/artifact.txt"
exit {exit_code}
""",
            encoding="utf-8",
        )
        (test_dir / "test_config.toml").write_text(
            f"""
schema_version = "cval.test.v1"

[test]
id = "{test_id}"
display_name = "{test_id.title()}"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = {timeout_seconds}

[artifacts]
summary_filename = "summary.json"
""",
            encoding="utf-8",
        )

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, object]]:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


if __name__ == "__main__":
    unittest.main()
