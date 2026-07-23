from __future__ import annotations

import unittest
import subprocess
import json
import os
import tempfile
from pathlib import Path

from cval.validation.results import load_validation_result
from cval.validation.results import validation_result_to_env


REPO_ROOT = Path(__file__).resolve().parents[1]


class ValidationScriptTests(unittest.TestCase):
    def test_validation_scripts_have_valid_bash_syntax(self) -> None:
        scripts = [
            REPO_ROOT / "validation-tests" / "0-env.sh",
            REPO_ROOT / "validation-tests" / "run-test.sh",
            REPO_ROOT / "validation-tests" / "db-update.sh",
            REPO_ROOT / "validation-tests" / "dltest" / "dltest.sh",
            REPO_ROOT / "validation-tests" / "dltest" / "summarize_results.py",
            REPO_ROOT / "validation-tests" / "nccl" / "ibbw.sh",
            REPO_ROOT / "validation-tests" / "storage" / "storage.sh",
        ]

        bash_scripts = [path for path in scripts if path.suffix == ".sh"]
        subprocess.run(["bash", "-n", *map(str, bash_scripts)], check=True)
        subprocess.run(
            ["python", "-m", "py_compile", str(REPO_ROOT / "validation-tests" / "dltest" / "summarize_results.py")],
            check=True,
        )

    def test_run_test_persists_results_for_db_update(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(encoding="utf-8")

        self.assertIn("CVAL_RESULT_ENV_FILE", script)
        self.assertIn("CVAL_RESULT_JSON_FILE", script)
        self.assertIn('"image_name": env("CVAL_IMAGE_NAME", "")', script)
        self.assertIn("NCCL_IBBW_LOG_FILE", script)
        self.assertIn("start_ibbw_monitor", script)
        self.assertIn("append_ibbw_log_to_nccl_log", script)
        self.assertIn("--ibbw-log-file $NCCL_IBBW_LOG_FILE", script)
        self.assertIn("write_result_state", script)
        self.assertIn("set -uo pipefail", script)

    def test_db_update_records_per_test_results(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "db-update.sh").read_text(encoding="utf-8")

        self.assertIn('Loading structured test result state from $CVAL_RESULT_JSON_FILE', script)
        self.assertIn('python3 -m cval.cli result', script)
        self.assertIn('PYTHONPATH="$CVAL_REPO_DIR"', script)
        self.assertIn('source "$CVAL_RESULT_ENV_FILE"', script)
        self.assertIn('--db-path "$CVAL_VALIDATION_DB_PATH"', script)
        self.assertIn('--db-path "$CVAL_STORAGE_DB_PATH"', script)
        self.assertIn('--db-path "$CVAL_NCCL_DB_PATH"', script)
        self.assertIn('add_main_result "storage" "$GCRRESULT1"', script)
        self.assertIn('add_main_result "nccl" "$GCRRESULT2"', script)
        self.assertIn('add_main_result "dltest" "$GCRRESULT3"', script)
        self.assertIn('add_main_result "all" "$overall_result"', script)
        self.assertIn('RUN_STORAGE) RUN_STORAGE="$value"', script)
        self.assertIn('RUN_NCCL) RUN_NCCL="$value"', script)
        self.assertIn('is_enabled "$RUN_STORAGE"', script)
        self.assertIn('is_enabled "$RUN_NCCL"', script)
        self.assertIn('--image-name "$CVAL_IMAGE_NAME"', script)
        self.assertNotIn('"all" \\\n    "pass"', script)

    def test_runtime_scripts_use_configured_environment(self) -> None:
        run_test = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(
            encoding="utf-8"
        )
        dltest = (REPO_ROOT / "validation-tests" / "dltest" / "dltest.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("CVAL_VALIDATION_TESTS_DIR", run_test)
        self.assertIn("CVAL_NCCL_ITERATIONS", run_test)
        self.assertIn("RUN_STORAGE", run_test)
        self.assertIn("RUN_NCCL", run_test)
        self.assertIn("RUN_DLTEST", run_test)
        self.assertIn("CVAL_IBBW_START_DEVICE", run_test)
        self.assertIn("CVAL_IBBW_END_DEVICE", run_test)
        self.assertIn("--nproc_per_node=\"$CVAL_NCCL_GPU_COUNT\"", run_test)
        self.assertIn('"$CVAL_DL_GPU_COUNT"', run_test)
        self.assertIn("CVAL_DL_TEST_PLAN", dltest)
        self.assertIn("CVAL_DL_ITERATIONS", dltest)
        self.assertIn("CVAL_DL_ITERATIONS=${CVAL_DL_ITERATIONS:-100}", dltest)
        self.assertIn("-m dl_unit_test", dltest)
        self.assertIn("DLTEST_WORK_DIR", dltest)
        self.assertIn("summarize_results.py", dltest)

    def test_ibbw_monitor_reports_gbps(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "nccl" / "ibbw.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("GB/s", script)
        self.assertNotIn("MB/s${space}", script)

    def test_ibbw_monitor_auto_detects_all_ports(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "nccl" / "ibbw.sh").read_text(
            encoding="utf-8"
        )

        # Auto-detection enumerates the sysfs tree instead of a fixed range.
        self.assertIn("/sys/class/infiniband", script)
        self.assertIn("port_xmit_data", script)
        self.assertIn("discover_ports", script)
        # Optional numeric range override is still supported.
        self.assertIn("start_device", script)
        self.assertIn("end_device", script)

    def test_run_test_defaults_ibbw_to_auto_detect(self) -> None:
        run_test = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(
            encoding="utf-8"
        )

        # Range env vars remain as optional overrides but default to empty.
        self.assertIn("CVAL_IBBW_START_DEVICE=${CVAL_IBBW_START_DEVICE:-}", run_test)
        self.assertIn("CVAL_IBBW_END_DEVICE=${CVAL_IBBW_END_DEVICE:-}", run_test)
        self.assertIn("auto-detect", run_test)

    def test_db_update_ingests_consolidated_ib_health(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "db-update.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("db-add-nccl-health", script)
        self.assertIn('"$NCCL_SUMMARY_FILE"', script)
        self.assertIn('--iterations "$CVAL_NCCL_ITERATIONS"', script)
        self.assertIn('--cuda-version "$CVAL_CUDA_VERSION"', script)
        self.assertIn('--pytorch-version "$CVAL_PYTORCH_VERSION"', script)

    def test_structured_validation_result_schema(self) -> None:
        payload = {
            "schema_version": "cval.results.v1",
            "node": "slc01-cl02-hgx-0001",
            "timestamp": "12345",
            "image_name": "pytorch:26.05-py3",
            "pytorch_version": "2.8.0a0+abc123",
            "cuda_version": "12.9",
            "overall": "fail",
            "tests": {
                "storage": {"status": "pass", "log": "storage.log", "summary": "storage.txt"},
                "nccl": {"status": "fail", "log": "nccl.log", "summary": "nccl.json"},
                "dltest": {"status": "pass", "log": "dl.log", "summary": "dl.txt"},
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = Path(tmpdir) / "result.json"
            result_path.write_text(json.dumps(payload), encoding="utf-8")

            result = load_validation_result(result_path)

        self.assertEqual(result.overall, "fail")
        self.assertEqual(result.image_name, "pytorch:26.05-py3")
        self.assertEqual(result.pytorch_version, "2.8.0a0+abc123")
        self.assertEqual(result.cuda_version, "12.9")
        self.assertEqual(result.tests["nccl"].status, "fail")
        self.assertEqual(
            validation_result_to_env(result),
            {
                "GCRRESULT1": "pass",
                "GCRRESULT2": "fail",
                "GCRRESULT3": "pass",
                "overall_result": "fail",
                "image_name": "pytorch:26.05-py3",
                "pytorch_version": "2.8.0a0+abc123",
                "cuda_version": "12.9",
                "RUN_STORAGE": "true",
                "RUN_NCCL": "true",
                "RUN_DLTEST": "true",
            },
        )

    def test_structured_result_allows_disabled_phase(self) -> None:
        payload = {
            "schema_version": "cval.results.v1",
            "node": "node-a",
            "timestamp": "12345",
            "overall": "pass",
            "tests": {
                "storage": {"enabled": True, "status": "pass"},
                "nccl": {"enabled": True, "status": "pass"},
                "dltest": {"enabled": False, "status": "incomplete"},
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "result.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = load_validation_result(path)

        self.assertFalse(result.tests["dltest"].enabled)
        self.assertEqual(result.overall, "pass")
        self.assertEqual(validation_result_to_env(result)["RUN_DLTEST"], "false")

    def test_all_disabled_script_path_invokes_no_test_tools(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = os.environ | {
                "RUN_STORAGE": "false",
                "RUN_NCCL": "false",
                "RUN_DLTEST": "false",
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                "CVAL_RESULT_ENV_FILE": str(root / "result.env"),
                "CVAL_RESULT_JSON_FILE": str(root / "result.json"),
                "STORAGE_OUTPUT_DIR": str(root / "storage"),
                "NCCL_OUTPUT_DIR": str(root / "nccl"),
                "DLTEST_OUTPUT_DIR": str(root / "dl"),
                "STORAGE_LOG_FILE": str(root / "storage.log"),
                "STORAGE_SUMMARY_FILE": str(root / "storage.txt"),
                "NCCL_LOG_FILE": str(root / "nccl.log"),
                "NCCL_SUMMARY_FILE": str(root / "nccl.json"),
                "NCCL_IBBW_LOG_FILE": str(root / "ibbw.log"),
                "DLTEST_LOG_FILE": str(root / "dl.log"),
                "DLTEST_SUMMARY_FILE": str(root / "dl.json"),
            }
            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "validation-tests" / "run-test.sh")],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            payload = json.loads((root / "result.json").read_text(encoding="utf-8"))

        self.assertEqual(completed.stdout.count("SKIPPED (disabled by config)"), 3)
        self.assertEqual(payload["overall"], "incomplete")
        self.assertTrue(all(not test["enabled"] for test in payload["tests"].values()))

    def test_dltest_summary_script_summarizes_rank_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runs_dir = root / "test_plans" / "80gb-example" / "runs"
            runs_dir.mkdir(parents=True)
            (runs_dir / "20260616151716_node_gpu_cuda_pt_RANK0.json").write_text(
                json.dumps(
                    {
                        "test_plan": "80gb-example",
                        "runID": "20260616151716_node_gpu_cuda_pt_RANK0",
                        "nn_tasks": [{"task_name": "linear", "status": "completed"}],
                        "f_tasks": [],
                        "coll_tasks": [{"task_name": "allreduce", "status": "completed"}],
                        "overlap_tasks": [],
                    }
                ),
                encoding="utf-8",
            )
            summary_path = root / "summary.json"

            subprocess.run(
                [
                    "python",
                    str(REPO_ROOT / "validation-tests" / "dltest" / "summarize_results.py"),
                    "--runs-dir",
                    str(runs_dir),
                    "--summary-file",
                    str(summary_path),
                    "--status",
                    "pass",
                    "--test-plan",
                    "80gb-example",
                    "--iterations",
                    "2",
                    "--gpu-count",
                    "1",
                ],
                check=True,
            )

            summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["rank_result_count"], 1)
        self.assertEqual(summary["task_counts"]["nn_tasks"], 1)
        self.assertEqual(summary["status_counts"], {"completed": 2})
        self.assertEqual(summary["rank_results"][0]["rank"], 0)


if __name__ == "__main__":
    unittest.main()