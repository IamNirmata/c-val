from __future__ import annotations

import unittest
import subprocess
import json
import os
import signal
import sqlite3
import stat
import tempfile
import tomllib
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from cval.config import load_config
from cval.validation.registry import load_test_registry, validation_test_config_digest
from cval.validation.nccl_summary import finalize_summary, summarize_ibbw
from cval.validation.results import load_validation_result, validation_result_digest
from cval.validation.results import validation_result_to_env
from cval.validation.runtime import build_runtime_environment, effective_config_digest


REPO_ROOT = Path(__file__).resolve().parents[1]


class ValidationScriptTests(unittest.TestCase):
    def test_nccl_summary_finalizer_parses_hca_samples_and_requires_them(self) -> None:
        self.assertEqual(
            summarize_ibbw(
                "00:00:01 | mlx5_0: 1024 MB/s    mlx5_4: 46.250 GB/s\n"
                "00:00:02 | mlx5_0: 2.000 GB/s    mlx5_4: 47.250 GB/s\n"
            ),
            {
                "mlx5_0": {
                    "avg_gbps": 1.5,
                    "max_gbps": 2.0,
                    "last_gbps": 2.0,
                    "samples": 2,
                },
                "mlx5_4": {
                    "avg_gbps": 46.75,
                    "max_gbps": 47.25,
                    "last_gbps": 47.25,
                    "samples": 2,
                },
            },
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metrics = root / "metrics.json"
            ibbw = root / "ibbw.log"
            metrics.write_text('{"GCR_BUSBW":20.0}\n', encoding="utf-8")
            ibbw.write_text("no HCA samples\n", encoding="utf-8")
            metrics.chmod(0o600)
            ibbw.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "no sampled"):
                finalize_summary(
                    metrics,
                    ibbw,
                    root / "summary.json",
                    ibbw_log_reference="/canonical/ibbw.log",
                    require_hca_samples=True,
                )

    def test_validation_scripts_have_valid_bash_syntax(self) -> None:
        bash_scripts = sorted((REPO_ROOT / "validation-tests").rglob("*.sh"))
        subprocess.run(["bash", "-n", *map(str, bash_scripts)], check=True)
        subprocess.run(
            [
                "python",
                "-m",
                "py_compile",
                str(
                    REPO_ROOT
                    / "validation-tests"
                    / "dltest"
                    / "summarize_results.py"
                ),
            ],
            check=True,
        )

    def test_validation_test_directories_have_standard_footprint(self) -> None:
        for test_id in ("storage", "nccl", "dltest"):
            test_dir = REPO_ROOT / "validation-tests" / test_id
            for relative_path in (
                "README.md",
                "test_config.toml",
                "setup.sh",
                "run-test.sh",
                "tests/README.md",
            ):
                self.assertTrue((test_dir / relative_path).is_file(), relative_path)
            config = tomllib.loads(
                (test_dir / "test_config.toml").read_text(encoding="utf-8")
            )
            self.assertEqual(config["test"]["entrypoint"], "run-test.sh")
            self.assertEqual(config["test"]["setup"], "setup.sh")

    def test_env_script_exports_canonical_v2_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = os.environ | {
                "CVAL_VALIDATION_ROOT": tmpdir,
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
            }
            command = """
source "$1"
printf '%s\n' \
  "$CVAL_RESULT_JSON_FILE" \
  "$STORAGE_OUTPUT_DIR" \
  "$NCCL_SUMMARY_FILE" \
  "$DLTEST_SUMMARY_FILE"
"""
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    command,
                    "bash",
                    str(REPO_ROOT / "validation-tests" / "0-env.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

        paths = completed.stdout.splitlines()
        self.assertTrue(paths[0].endswith("logs/job_logs/node-a/node-a-123/result.json"))
        self.assertTrue(
            paths[1].endswith(
                "validation_tests/storage/runs/node-a/node-a-123/artifacts"
            )
        )
        self.assertTrue(
            paths[2].endswith("validation_tests/nccl/runs/node-a/node-a-123/summary.json")
        )
        self.assertTrue(
            paths[3].endswith("validation_tests/dltest/runs/node-a/node-a-123/summary.json")
        )

    def test_env_script_creates_no_compatibility_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            external = root / "outside"
            external.mkdir()
            (root / "validation_tests").mkdir()
            (root / "validation_tests/storage").symlink_to(
                external,
                target_is_directory=True,
            )
            env = os.environ | {
                "CVAL_VALIDATION_ROOT": str(root),
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
            }

            subprocess.run(
                [
                    "bash",
                    "-c",
                    'source "$1"',
                    "bash",
                    str(REPO_ROOT / "validation-tests/0-env.sh"),
                ],
                env=env,
                check=True,
            )

            self.assertEqual(list(external.iterdir()), [])

    def test_run_test_persists_results_for_db_update(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(encoding="utf-8")
        runner = (REPO_ROOT / "cval" / "validation" / "runner.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("exec python3 -m cval.validation.runner", script)
        self.assertIn("set -euo pipefail", script)
        self.assertIn("CVAL_RESULT_ENV_FILE", runner)
        self.assertIn("CVAL_RESULT_JSON_FILE", runner)
        self.assertIn('"schema_version": "cval.results"', runner)
        self.assertIn("_atomic_write_json", runner)
        self.assertIn("test_setup_started", runner)
        self.assertIn("test_started", runner)
        self.assertIn("test_finished", runner)

    def test_db_update_records_current_raw_results_only(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "db-update.sh").read_text(encoding="utf-8")

        self.assertIn('Loading structured test result state from $CVAL_RESULT_JSON_FILE', script)
        self.assertIn('from cval.validation.results import load_validation_result', script)
        self.assertIn('from cval.storage.write_provenance import authorize_result_write', script)
        self.assertIn('validation_result_to_env_lines', script)
        self.assertIn('PYTHONPATH="$CVAL_REPO_DIR"', script)
        self.assertNotIn('source "$CVAL_RESULT_ENV_FILE"', script)
        self.assertIn('result_projection=$(', script)
        self.assertIn('Result run_id mismatch', script)
        self.assertIn('--db-path "$CVAL_VALIDATION_DB_PATH"', script)
        self.assertIn('--db-path "$CVAL_STORAGE_DB_PATH"', script)
        self.assertIn('--db-path "$CVAL_NCCL_DB_PATH"', script)
        self.assertIn('db-add-dltest-run', script)
        self.assertIn('dltest_ingest_dir=${CVAL_CANONICAL_DLTEST_RUN_DIR:-$DLTEST_RUN_DIR}', script)
        self.assertIn('"$GCRTIME" "$dltest_ingest_dir"', script)
        self.assertIn('"$CVAL_DL_METRIC_LOCK_HELPER"', script)
        self.assertIn('metadata/.dl-metric-ingest.lock', script)
        self.assertIn('db-add-run-results', script)
        self.assertIn('--storage-result "$GCRRESULT1"', script)
        self.assertIn('--overall-result "$overall_result"', script)
        self.assertIn('RUN_STORAGE) RUN_STORAGE="$value"', script)
        self.assertIn('RUN_NCCL) RUN_NCCL="$value"', script)
        self.assertIn('is_enabled "$RUN_STORAGE"', script)
        self.assertIn('is_enabled "$RUN_NCCL"', script)
        self.assertIn('--image-name "$CVAL_IMAGE_NAME"', script)
        self.assertIn('emit_cval_event "ingestion_started"', script)
        self.assertIn('emit_cval_event "ingestion_finished" "pass"', script)
        self.assertNotIn('nccl-eval', script)
        self.assertNotIn('CVAL_NCCL_EVALUATION', script)
        self.assertNotIn('CVAL_NCCL_OUTBOX', script)
        self.assertIn('--ibbw-log "$nccl_ibbw_ingest_log"', script)
        self.assertNotIn('DATABASE_URL', script)
        self.assertNotIn("CVAL_PER_TEST_INGESTION_ENABLED", script)
        self.assertNotIn("CVAL_RUN_HISTORY_ENABLED", script)
        self.assertNotIn("db-ingest-test-results", script)
        self.assertNotIn("db-upsert-run-history", script)
        self.assertIn('--result-json "$CVAL_RESULT_JSON_FILE"', script)
        self.assertNotIn('"all" \\\n    "pass"', script)

    def test_db_update_ingests_passing_dl_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = root / "metadata"
            dl_results = root / "validation_tests/dltest/runs"
            config_path = root / "cval.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[storage]",
                        f'validation_db_path = "{metadata / "validation.db"}"',
                        f'storage_db_path = "{metadata / "test-storage.db"}"',
                        f'nccl_db_path = "{metadata / "test-nccl.db"}"',
                        f'dl_numerical_db_path = "{metadata / "dltest_numerical_correctness.db"}"',
                        f'dl_compute_db_path = "{metadata / "dltest_compute_performance.db"}"',
                        f'dl_collective_db_path = "{metadata / "dltest_collective_performance.db"}"',
                        f'dl_overlap_db_path = "{metadata / "dltest_overlap_performance.db"}"',
                        "",
                        "[runtime]",
                        f'repo_dir = "{REPO_ROOT}"',
                        f'validation_root = "{root}"',
                        f'validation_tests_dir = "{REPO_ROOT / "validation-tests"}"',
                        f'dl_results_root_path = "{dl_results}"',
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            config = load_config(config_path)
            run_dir = dl_results / "node-a/node-a-123"
            ranks_dir = run_dir / "artifacts/workdir/test_plans/80gb-example/runs"
            ranks_dir.mkdir(parents=True)
            (ranks_dir / "example_RANK0.json").write_text(
                json.dumps(
                    {
                        "runID": "example_RANK0",
                        "test_plan": "80gb-example",
                        "nn_tasks": [
                            {
                                "task_name": "linear",
                                "status": "completed",
                                "norm_output": 1.0,
                                "fp_cpu_time": 2.0,
                                "fp_gpu_time": 3.0,
                                "bp_cpu_time": 4.0,
                                "bp_gpu_time": 5.0,
                            }
                        ],
                        "f_tasks": [],
                        "coll_tasks": [
                            {
                                "task_name": "allreduce",
                                "status": "completed",
                                "norm_output": 6.0,
                                "cpu_time": 7.0,
                                "gpu_time": 8.0,
                            }
                        ],
                        "overlap_tasks": [
                            {
                                "task_name": "overlap",
                                "status": "completed",
                                "coll_name": "allreduce",
                                "layer_name": "linear",
                                "coll_mean": 9.0,
                                "coll_stdev": 0.9,
                                "layer_mean": 10.0,
                                "layer_stdev": 1.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "gpu_count": 1,
                        "rank_result_count": 1,
                        "iterations": 100,
                    }
                ),
                encoding="utf-8",
            )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "node-a",
                        "timestamp": "123",
                        "overall": "pass",
                        "tests": {
                            "storage": {"enabled": False, "status": "incomplete"},
                            "nccl": {"enabled": False, "status": "incomplete"},
                            "dltest": {"enabled": True, "status": "pass"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ | build_runtime_environment(config) | {
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_CONFIG_PATH": str(config_path),
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_RESULT_JSON_FILE": str(result_path),
                "CVAL_JOB_LOG_DIR": str(result_path.parent),
                "DLTEST_RUN_DIR": "/proc/self/fd/999/run",
                "CVAL_CANONICAL_DLTEST_RUN_DIR": str(run_dir),
            }
            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "validation-tests/db-update.sh")],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

            counts = {}
            for component in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ):
                db_path = metadata / f"dltest_{component}.db"
                with closing(sqlite3.connect(db_path)) as connection:
                    counts[component] = connection.execute(
                        f'SELECT COUNT(*) FROM "{component}" '
                        "WHERE node = ? AND cval_timestamp = ?",
                        ("node-a", 123),
                    ).fetchone()[0]
            with closing(sqlite3.connect(metadata / "validation.db")) as connection:
                dl_status = connection.execute(
                    "SELECT result FROM runs WHERE node = ? AND test = ? AND timestamp = ?",
                    ("node-a", "dltest", 123),
                ).fetchone()

        self.assertEqual(
            counts,
            {
                "numerical_correctness": 2,
                "compute_performance": 4,
                "collective_performance": 2,
                "overlap_performance": 4,
            },
        )
        self.assertEqual(dl_status, ("pass",))
        self.assertIn("DL metric DB update completed.", completed.stdout)

    def test_db_update_rejects_passing_dl_run_without_rank_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = root / "metadata"
            dl_results = root / "validation_tests/dltest/runs"
            config_path = root / "cval.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[storage]",
                        f'validation_db_path = "{metadata / "validation.db"}"',
                        f'storage_db_path = "{metadata / "test-storage.db"}"',
                        f'nccl_db_path = "{metadata / "test-nccl.db"}"',
                        f'dl_numerical_db_path = "{metadata / "dltest_numerical_correctness.db"}"',
                        f'dl_compute_db_path = "{metadata / "dltest_compute_performance.db"}"',
                        f'dl_collective_db_path = "{metadata / "dltest_collective_performance.db"}"',
                        f'dl_overlap_db_path = "{metadata / "dltest_overlap_performance.db"}"',
                        "",
                        "[runtime]",
                        f'repo_dir = "{REPO_ROOT}"',
                        f'validation_root = "{root}"',
                        f'validation_tests_dir = "{REPO_ROOT / "validation-tests"}"',
                        f'dl_results_root_path = "{dl_results}"',
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            config = load_config(config_path)
            run_dir = dl_results / "node-a/node-a-123"
            run_dir.mkdir(parents=True)
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "node-a",
                        "timestamp": "123",
                        "overall": "pass",
                        "tests": {
                            "storage": {"enabled": False, "status": "incomplete"},
                            "nccl": {"enabled": False, "status": "incomplete"},
                            "dltest": {"enabled": True, "status": "pass"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ | build_runtime_environment(config) | {
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_CONFIG_PATH": str(config_path),
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_RESULT_JSON_FILE": str(result_path),
                "CVAL_JOB_LOG_DIR": str(result_path.parent),
                "DLTEST_RUN_DIR": "/proc/self/fd/999/run",
                "CVAL_CANONICAL_DLTEST_RUN_DIR": str(run_dir),
            }

            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "validation-tests/db-update.sh")],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            databases = list(metadata.glob("*.db")) if metadata.exists() else []

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("no complete rank evidence", completed.stderr)
        self.assertEqual(databases, [])

    def test_runtime_scripts_use_configured_environment(self) -> None:
        run_test = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(
            encoding="utf-8"
        )
        nccl = (
            REPO_ROOT / "validation-tests" / "nccl" / "run-test.sh"
        ).read_text(encoding="utf-8")
        dltest = (
            REPO_ROOT / "validation-tests" / "dltest" / "run-test.sh"
        ).read_text(
            encoding="utf-8"
        )

        self.assertIn("cval.validation.runner", run_test)
        self.assertIn("CVAL_NCCL_ITERATIONS", nccl)
        self.assertIn("CVAL_IBBW_START_DEVICE", nccl)
        self.assertIn("CVAL_IBBW_END_DEVICE", nccl)
        self.assertIn('torchrun --nproc_per_node="$CVAL_NCCL_GPU_COUNT"', nccl)
        self.assertNotIn("nccl_eval", nccl)
        self.assertNotIn("runtime-evidence", nccl)
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
        run_test = (
            REPO_ROOT / "validation-tests" / "nccl" / "run-test.sh"
        ).read_text(encoding="utf-8")

        # Range env vars remain as optional overrides but default to empty.
        self.assertIn("CVAL_IBBW_START_DEVICE=${CVAL_IBBW_START_DEVICE:-}", run_test)
        self.assertIn("CVAL_IBBW_END_DEVICE=${CVAL_IBBW_END_DEVICE:-}", run_test)
        self.assertIn("auto-detect", run_test)

    def test_compatibility_entrypoints_delegate_to_canonical_runners(self) -> None:
        wrappers = {
            "storage/storage.sh": "storage/run-test.sh",
            "nccl/run-nccl-allreduce.sh": "nccl/run-test.sh",
            "dltest/dltest.sh": "dltest/run-test.sh",
        }
        for wrapper, canonical in wrappers.items():
            text = (REPO_ROOT / "validation-tests" / wrapper).read_text(
                encoding="utf-8"
            )
            self.assertIn('exec bash "$SCRIPT_DIR/run-test.sh" "$@"', text)
            self.assertTrue((REPO_ROOT / "validation-tests" / canonical).is_file())

    def test_top_level_runner_delegates_enabled_storage_phase(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            test_root = root / "validation-tests"
            storage_dir = test_root / "storage"
            storage_dir.mkdir(parents=True)
            (storage_dir / "setup.sh").write_text(
                '#!/bin/bash\necho "fake storage setup"\n', encoding="utf-8"
            )
            (storage_dir / "run-test.sh").write_text(
                '#!/bin/bash\necho "fake storage run"\n', encoding="utf-8"
            )
            (storage_dir / "test_config.toml").write_text(
                """
schema_version = "cval.test.v1"

[test]
id = "storage"
display_name = "Storage"
order = 10
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30

[artifacts]
summary_filename = "summary.txt"
""",
                encoding="utf-8",
            )
            env = os.environ | {
                "RUN_STORAGE": "true",
                "RUN_NCCL": "false",
                "RUN_DLTEST": "false",
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_VALIDATION_TESTS_DIR": str(test_root),
                "CVAL_VALIDATION_ROOT": str(root / "data"),
                "CVAL_TEST_REGISTRY_JSON": json.dumps(
                    {
                        "storage": {
                            "enabled": True,
                            "config_path": "validation-tests/storage/test_config.toml",
                            "order": 10,
                        }
                    }
                ),
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
            result_path = (
                root
                / "data"
                / "logs"
                / "job_logs"
                / "node-a"
                / "node-a-123"
                / "result.json"
            )
            payload = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertIn("fake storage setup", completed.stdout)
        self.assertIn("fake storage run", completed.stdout)
        self.assertIn("Storage test is complete.", completed.stdout)
        self.assertEqual(payload["tests"]["storage"]["status"], "pass")
        self.assertEqual(payload["overall"], "pass")
        self.assertFalse((root / "result.json").exists())

    def test_nccl_runner_can_run_with_monitor_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_torchrun = bin_dir / "torchrun"
            fake_torchrun.write_text(
                """#!/bin/bash
set -e
# Model a nested launcher that does not preserve supervisor-owned descriptors.
for fd_path in /proc/$$/fd/*; do
    fd=${fd_path##*/}
    if [[ "$fd" =~ ^[0-9]+$ && "$fd" -gt 2 ]]; then
        eval "exec ${fd}>&-" 2>/dev/null || true
    fi
done
result_file=""
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--result-file" ]]; then
        result_file="$2"
        shift 2
    else
        shift
    fi
done
cat > "$result_file" <<'JSON'
{"GCR_ITERATIONS":20,"GCR_DATA_SIZE_GB":8,"GCR_LATENCY":1.0,"GCR_ALGBW":10.0,"GCR_BUSBW":20.0,"GCR_IB_PORT_BW_GBPS":{}}
JSON
echo "fake torchrun"
""",
                encoding="utf-8",
            )
            fake_torchrun.chmod(0o755)
            output_dir = root / "nccl"
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                "CVAL_IBBW_ENABLED": "false",
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "NCCL_OUTPUT_DIR": str(output_dir),
                "NCCL_LOG_FILE": str(output_dir / "nccl.log"),
                "NCCL_SUMMARY_FILE": str(output_dir / "summary.json"),
                "NCCL_IBBW_LOG_FILE": str(output_dir / "ibbw.log"),
            }
            completed = subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests" / "nccl" / "run-test.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            published_summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )
            published_mode = stat.S_IMODE((output_dir / "summary.json").stat().st_mode)

        self.assertIn("IBBW monitor disabled by config", completed.stdout)
        self.assertIn("fake torchrun", completed.stdout)
        self.assertEqual(
            published_summary,
            {
                "GCR_ITERATIONS": 20,
                "GCR_DATA_SIZE_GB": 8,
                "GCR_LATENCY": 1.0,
                "GCR_ALGBW": 10.0,
                "GCR_BUSBW": 20.0,
                "GCR_IB_PORT_BW_GBPS": {},
                "GCR_IBBW_LOG_FILE": str(output_dir / "ibbw.log"),
            },
        )
        self.assertEqual(published_mode, 0o600)

    def test_nccl_runner_reaps_enabled_ibbw_monitor_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_torchrun = bin_dir / "torchrun"
            fake_torchrun.write_text(
                """#!/bin/bash
set -e
sleep 2
result_file=""
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--result-file" ]]; then
        result_file="$2"
        shift 2
    else
        shift
    fi
done
printf '%s\n' '{"GCR_ITERATIONS":20,"GCR_DATA_SIZE_GB":8,"GCR_LATENCY":1.0,"GCR_ALGBW":10.0,"GCR_BUSBW":20.0,"GCR_IB_PORT_BW_GBPS":{}}' > "$result_file"
""",
                encoding="utf-8",
            )
            fake_torchrun.chmod(0o755)
            counter = root / "infiniband/mlx5_0/ports/1/counters/port_xmit_data"
            counter.parent.mkdir(parents=True)
            counter.write_text("100\n", encoding="utf-8")
            output_dir = root / "output"
            process = subprocess.Popen(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests/nccl/run-test.sh"),
                ],
                env=os.environ
                | {
                    "PATH": f"{bin_dir}:{os.environ['PATH']}",
                    "CVAL_REPO_DIR": str(REPO_ROOT),
                    "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                    "CVAL_IBBW_ENABLED": "true",
                    "IB_SYS_ROOT": str(root / "infiniband"),
                    "GCRNODE": "node-a",
                    "GCRTIME": "123",
                    "NCCL_OUTPUT_DIR": str(output_dir),
                    "NCCL_LOG_FILE": str(output_dir / "nccl.log"),
                    "NCCL_SUMMARY_FILE": str(output_dir / "summary.json"),
                    "NCCL_IBBW_LOG_FILE": str(output_dir / "ibbw.log"),
                },
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            stdout, stderr = process.communicate(timeout=15)
            published_summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                descendants_remain = False
            else:
                descendants_remain = True
                os.killpg(process.pid, signal.SIGKILL)

        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn("NCCL summary published", stdout)
        self.assertFalse(descendants_remain)
        self.assertGreaterEqual(
            published_summary["GCR_IB_PORT_BW_GBPS"]["mlx5_0"]["samples"],
            1,
        )

    def test_nccl_runner_rejects_missing_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_torchrun = bin_dir / "torchrun"
            fake_torchrun.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            fake_torchrun.chmod(0o755)
            output_dir = root / "nccl"
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                "CVAL_IBBW_ENABLED": "false",
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "NCCL_OUTPUT_DIR": str(output_dir),
                "NCCL_LOG_FILE": str(output_dir / "nccl.log"),
                "NCCL_SUMMARY_FILE": str(output_dir / "summary.json"),
                "NCCL_IBBW_LOG_FILE": str(output_dir / "ibbw.log"),
            }
            completed = subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests" / "nccl" / "run-test.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("summary finalization FAILED", completed.stderr)

    def test_storage_runner_preserves_six_json_artifacts_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_fio = bin_dir / "fio"
            fake_fio.write_text(
                """#!/bin/bash
set -e
printf '%s\n' "$*" >> "$FIO_ARGS_LOG"
for argument in "$@"; do
    case "$argument" in
        --output=*) output=${argument#--output=} ;;
    esac
done
printf '%s\n' '{"jobs":[{"read":{"iops":100,"bw":1048576},"write":{"iops":0,"bw":0}}]}' > "$output"
""",
                encoding="utf-8",
            )
            fake_fio.chmod(0o755)
            fake_jq = bin_dir / "jq"
            fake_jq.write_text(
                '#!/bin/bash\necho "100 1048576"\n', encoding="utf-8"
            )
            fake_jq.chmod(0o755)
            output_dir = root / "storage"
            summary_file = output_dir / "storage-summary.txt"
            fio_args_log = root / "fio-args.log"
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "FIO_ARGS_LOG": str(fio_args_log),
                "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "STORAGE_OUTPUT_DIR": str(output_dir),
                "STORAGE_SUMMARY_FILE": str(summary_file),
            }
            subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests" / "storage" / "run-test.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

            result_files = sorted(output_dir.glob("*.json"))
            summary = summary_file.read_text(encoding="utf-8")
            fio_args = fio_args_log.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(result_files), 6)
        self.assertIn("randread.json", summary)
        self.assertIn("randwrite.json", summary)
        self.assertIn("1.00", summary)
        self.assertEqual(len(fio_args), 6)
        self.assertTrue(
            all(f"--directory={output_dir / 'fio-data'}" in line for line in fio_args)
        )
        self.assertFalse((output_dir / "fio-data").exists())

    def test_dl_runner_preserves_rank_json_summary_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "dl-source"
            (source / "src" / "dl_unit_test").mkdir(parents=True)
            plan_dir = source / "test_plans" / "80gb-example"
            plan_dir.mkdir(parents=True)
            (plan_dir / "test_plan.json").write_text("{}\n", encoding="utf-8")

            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_torchrun = bin_dir / "torchrun"
            fake_torchrun.write_text(
                """#!/bin/bash
set -e
runs_dir="$PWD/test_plans/80gb-example/runs"
mkdir -p "$runs_dir"
cat > "$runs_dir/20260728_node_gpu_cuda_pt_RANK0.json" <<'JSON'
{
    "test_plan": "80gb-example",
    "runID": "20260728_node_gpu_cuda_pt_RANK0",
    "nn_tasks": [{"task_name": "linear", "status": "completed"}],
    "f_tasks": [],
    "coll_tasks": [],
    "overlap_tasks": []
}
JSON
""",
                encoding="utf-8",
            )
            fake_torchrun.chmod(0o755)
            output_dir = root / "dl-output"
            summary_file = output_dir / "summary.json"
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "CVAL_VALIDATION_TESTS_DIR": str(REPO_ROOT / "validation-tests"),
                "CVAL_DL_UNIT_TEST_DIR": str(source),
                "CVAL_DL_GPU_COUNT": "1",
                "CVAL_DL_ITERATIONS": "2",
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "DLTEST_OUTPUT_DIR": str(output_dir),
                "DLTEST_LOG_FILE": str(output_dir / "dl.log"),
                "DLTEST_SUMMARY_FILE": str(summary_file),
            }
            subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests" / "dltest" / "run-test.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            summary = json.loads(summary_file.read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["rank_result_count"], 1)
        self.assertEqual(summary["iterations"], 2)
        self.assertEqual(summary["gpu_count"], 1)

    def test_dl_setup_rejects_unsafe_test_plan_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = Path(tmpdir) / "bin"
            bin_dir.mkdir()
            fake_torchrun = bin_dir / "torchrun"
            fake_torchrun.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            fake_torchrun.chmod(0o755)
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "CVAL_DL_TEST_PLAN": "../escape",
            }

            completed = subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "validation-tests/dltest/setup.sh"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("unsafe test plan", completed.stderr)

    def test_db_update_ingests_consolidated_ib_health(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "db-update.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "${CVAL_CANONICAL_STORAGE_OUTPUT_DIR:-$STORAGE_OUTPUT_DIR}",
            script,
        )
        self.assertIn('"$storage_ingest_dir"', script)
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
                "result_node": "slc01-cl02-hgx-0001",
                "result_timestamp": "12345",
                "result_run_id": "slc01-cl02-hgx-0001-12345",
                "result_schema_version": "cval.results.v1",
                "result_global_config_digest": "",
                "result_digest": validation_result_digest(result),
                "result_storage_artifacts": "",
                "result_nccl_summary": "",
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
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_VALIDATION_ROOT": str(root / "data"),
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
            result_path = (
                root
                / "data"
                / "logs"
                / "job_logs"
                / "node-a"
                / "node-a-123"
                / "result.json"
            )
            payload = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertEqual(completed.stdout.count("SKIPPED (disabled by config)"), 3)
        self.assertEqual(payload["overall"], "incomplete")
        self.assertTrue(all(not test["enabled"] for test in payload["tests"].values()))

    def test_current_result_flows_through_raw_db_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            smoke_dir = root / "repo" / "validation-tests" / "smoke"
            smoke_dir.mkdir(parents=True)
            (smoke_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n")
            (smoke_dir / "run-test.sh").write_text(
                '#!/bin/bash\necho "dynamic smoke pass"\nexit 0\n'
            )
            (smoke_dir / "test_config.toml").write_text(
                """
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 999
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
"""
            )
            registry = load_test_registry(
                {
                    "smoke": {
                        "enabled": True,
                        "config_path": "validation-tests/smoke/test_config.toml",
                    }
                },
                repo_root=root / "repo",
                include_defaults=False,
            )
            base = load_config()
            config = replace(
                base,
                storage=replace(
                    base.storage,
                    validation_db_path=str(root / "metadata/validation.db"),
                    storage_db_path=str(root / "metadata/test-storage.db"),
                    nccl_db_path=str(root / "metadata/test-nccl.db"),
                ),
                runtime=replace(
                    base.runtime,
                    repo_dir=str(REPO_ROOT),
                    validation_root=str(root),
                    validation_tests_dir=str(root / "repo/validation-tests"),
                ),
                tests=replace(base.tests, registry=registry),
            )
            env = os.environ | build_runtime_environment(config) | {
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_TEST_REPO_ROOT": str(root / "repo"),
                "CVAL_VALIDATION_TESTS_DIR": str(root / "repo/validation-tests"),
                "CVAL_VALIDATION_ROOT": str(root),
            }
            command = """
set -euo pipefail
source "$1/0-env.sh"
bash "$1/run-test.sh"
bash "$1/db-update.sh"
"""
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    command,
                    "bash",
                    str(REPO_ROOT / "validation-tests"),
                ],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            events_path = root / "logs/job_logs/node-a/node-a-123/events.jsonl"
            with closing(
                sqlite3.connect(root / "metadata/validation.db")
            ) as connection:
                rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY rowid"
                ).fetchall()
            result = json.loads(result_path.read_text(encoding="utf-8"))
            events = events_path.read_text(encoding="utf-8")

        self.assertEqual(result["schema_version"], "cval.results")
        self.assertEqual(result["tests"]["smoke"]["status"], "pass")
        self.assertEqual(
            rows,
            [
                ("storage", "incomplete"),
                ("nccl", "incomplete"),
                ("dltest", "incomplete"),
                ("all", "pass"),
            ],
        )
        self.assertIn('"event":"ingestion_started"', events)
        self.assertIn('"event":"ingestion_finished"', events)
        self.assertIn("Main DB update completed.", completed.stdout)

    def test_db_update_rejects_invalid_result_before_any_db_write(self) -> None:
        for result_text, expected_error in (
            ("{not-json}\n", "Structured result validation failed"),
            (
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "other-node",
                        "timestamp": "123",
                        "overall": "pass",
                        "tests": {
                            "storage": {"status": "pass"},
                            "nccl": {"status": "pass"},
                            "dltest": {"status": "pass"},
                        },
                    }
                ),
                "Result node mismatch",
            ),
        ):
            with self.subTest(expected_error=expected_error):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    result_path = root / "result.json"
                    result_path.write_text(result_text, encoding="utf-8")
                    env = os.environ | {
                        "GCRNODE": "node-a",
                        "GCRTIME": "123",
                        "CVAL_RUN_ID": "node-a-123",
                        "CVAL_REPO_DIR": str(REPO_ROOT),
                        "CVAL_VALIDATION_ROOT": str(root),
                        "CVAL_RESULT_JSON_FILE": str(result_path),
                        "CVAL_VALIDATION_DB_PATH": str(root / "validation.db"),
                    }
                    completed = subprocess.run(
                        [
                            "bash",
                            str(REPO_ROOT / "validation-tests" / "db-update.sh"),
                        ],
                        env=env,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )

                    self.assertNotEqual(completed.returncode, 0)
                    self.assertIn(expected_error, completed.stderr)
                    self.assertFalse((root / "validation.db").exists())

    def test_db_update_missing_passing_nccl_summary_writes_no_status_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = replace(
                base,
                storage=replace(
                    base.storage,
                    validation_db_path=str(root / "validation.db"),
                    storage_db_path=str(root / "storage.db"),
                    nccl_db_path=str(root / "nccl.db"),
                    dl_numerical_db_path=str(root / "dl-numerical.db"),
                    dl_compute_db_path=str(root / "dl-compute.db"),
                    dl_collective_db_path=str(root / "dl-collective.db"),
                    dl_overlap_db_path=str(root / "dl-overlap.db"),
                ),
                runtime=replace(base.runtime, validation_root=str(root)),
            )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "node-a",
                        "timestamp": "123",
                        "overall": "pass",
                        "tests": {
                            "storage": {"enabled": False, "status": "incomplete"},
                            "nccl": {"enabled": True, "status": "pass"},
                            "dltest": {"enabled": False, "status": "incomplete"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ | build_runtime_environment(config) | {
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_RESULT_JSON_FILE": str(result_path),
                "CVAL_JOB_LOG_DIR": str(result_path.parent),
                "NCCL_SUMMARY_FILE": str(
                    root
                    / "validation_tests/nccl/runs/node-a/node-a-123/summary.json"
                ),
            }
            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "validation-tests/db-update.sh")],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("missing required summary", completed.stderr)
        self.assertFalse((root / "validation.db").exists())

    def test_v2_missing_snapshot_writes_no_database(self) -> None:
        from tests.test_results_v2 import payload

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(json.dumps(payload()), encoding="utf-8")
            env = os.environ | {
                "GCRNODE": "node-a",
                "GCRTIME": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_RESULT_JSON_FILE": str(result_path),
                "CVAL_JOB_LOG_DIR": str(result_path.parent),
                "STORAGE_OUTPUT_DIR": "/runs/storage/artifacts",
                "NCCL_SUMMARY_FILE": "/runs/nccl/summary.json",
                "CVAL_VALIDATION_DB_PATH": str(root / "metadata/validation.db"),
                "CVAL_STORAGE_DB_PATH": str(root / "metadata/test-storage.db"),
                "CVAL_NCCL_DB_PATH": str(root / "metadata/test-nccl.db"),
            }

            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "validation-tests/db-update.sh")],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("CVAL_CONFIG_SNAPSHOT_B64 is required", completed.stderr)
            self.assertEqual(list(root.glob("**/*.db")), [])

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
        self.assertTrue(summary["rank_coverage_valid"])

    def test_dltest_summary_rejects_missing_rank(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runs_dir = root / "runs"
            runs_dir.mkdir()
            (runs_dir / "example_RANK0.json").write_text(
                json.dumps(
                    {
                        "test_plan": "80gb-example",
                        "runID": "example_RANK0",
                        "nn_tasks": [
                            {"task_name": "linear", "status": "completed"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            summary_path = root / "summary.json"
            completed = subprocess.run(
                [
                    "python",
                    str(
                        REPO_ROOT
                        / "validation-tests"
                        / "dltest"
                        / "summarize_results.py"
                    ),
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
                    "2",
                ],
                check=False,
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(summary["status"], "fail")
        self.assertFalse(summary["rank_coverage_valid"])
        self.assertEqual(summary["expected_ranks"], [0, 1])
        self.assertEqual(summary["observed_ranks"], [0])

    def test_dltest_summary_rejects_non_object_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runs_dir = root / "runs"
            runs_dir.mkdir()
            (runs_dir / "example_RANK0.json").write_text(
                json.dumps(
                    {
                        "test_plan": "80gb-example",
                        "runID": "example_RANK0",
                        "nn_tasks": [None],
                    }
                ),
                encoding="utf-8",
            )
            summary_path = root / "summary.json"
            completed = subprocess.run(
                [
                    "python",
                    str(
                        REPO_ROOT
                        / "validation-tests/dltest/summarize_results.py"
                    ),
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
                check=False,
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(summary["status"], "fail")
        self.assertFalse(summary["tasks_complete"])


if __name__ == "__main__":
    unittest.main()